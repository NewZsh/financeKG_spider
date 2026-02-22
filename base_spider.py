import os
import json
import threading
import time
import sqlite3
import queue

try:
    import fcntl  # 文件锁支持 (Unix only)
except ImportError:
    fcntl = None
from datetime import datetime

cur_dir = os.path.dirname(os.path.abspath(__file__))
from loguru import logger

# configure loguru to write to a file with desired format
log_file = os.path.join(cur_dir, "logs", "spider.log")
os.makedirs(os.path.dirname(log_file), exist_ok=True)
logger.remove()
logger.add(
    log_file,
    rotation="10 MB",
    retention="10 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {file}:{line} - {message}",
)


# thead safety unique queue
# for the usage of the spider to
# - put a new collected id
# - get a id to crawl its info
class ThreadSafeUniqueQueue:
    def __init__(self):
        self._queue = queue.Queue()
        self._set = set()
        self._lock = threading.Lock()

    def put(self, item):
        with self._lock:
            if item not in self._set:
                self._set.add(item)
                self._queue.put(item)

    def get(self):
        item = self._queue.get()
        with self._lock:
            self._set.discard(item)
        return item

    def task_done(self):
        self._queue.task_done()

    def empty(self):
        return self._queue.empty()

    def qsize(self):
        return self._queue.qsize()

    def clear(self):
        with self._lock:
            self._set.clear()
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    break


class base_spider:
    def __init__(self, id_collect_queue = None):
        self.ua_list = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
        ]

        self.cfg_file = "spider.cfg"
        self.cfg_file = os.path.join(cur_dir, self.cfg_file)
        self.timer = None
        self.refresh_interval = 60  # 60秒 = 1分钟
        
        # 小批量更新配置
        self.batch_size = 100  # 每次处理的记录数
        self.update_buffer = []  # 更新缓冲区
        self.buffer_lock = threading.Lock()  # 缓冲区锁
        self.file_lock = threading.Lock()  # 文件锁

        # bind a loguru logger to this instance
        self.logger = logger.bind(spider=self.__class__.__name__)

        self.__load_cfg()
        self.__start_cfg_refresh_timer()

        # 用sql管理爬虫进展
        # sql维护两个表，一个是record（已经爬过的记录），一个是todo（待爬取的记录）
        # record表结果：src, id, entity_type, visit_time(YYYY-MM-DD), last_visit_time, visit_times
        # todo表结果：src, id, entity_type, found_time
        self.db_file = "data/spider_progress.db"
        self.db_file =  os.path.join(cur_dir, self.db_file)
        self.__init_db()

        # 用队列进行跨线程管理
        self.id_collect_queue = id_collect_queue

    def __init_db(self):
        '''
        初始化数据库和表
        '''
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        # 创建record表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS record (
                src TEXT,
                id TEXT,
                entity_type TEXT,
                visit_time TEXT,
                last_visit_time TEXT,
                visit_times INTEGER,
                PRIMARY KEY (src, id)
            )
        ''')
        
        # 创建todo表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS todo (
                src TEXT,
                id TEXT,
                entity_type TEXT,
                found_time TEXT,
                PRIMARY KEY (src, id)
            )
        ''')
        
        conn.commit()
        conn.close()    

    ## ** PART 1 : 配置管理相关函数 ** ##
    def __load_cfg(self):
        '''
        Load configuration from cfg file
        '''
        try:
            self.cfg = json.loads(open(self.cfg_file, 'r').read())
            self.logger.info(f"配置加载成功，时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            self.logger.exception(f"配置加载失败: {e}")

    # cfg 允许热更，更新参数直接作用于爬虫后台而无需重启
    def __refresh_cfg(self):
        '''
        重新加载配置，仅在实际变更时打印日志
        '''
        try:
            old_cfg = self.cfg.copy() if hasattr(self, 'cfg') else None
            with open(self.cfg_file, 'r') as f:
                new_cfg = json.loads(f.read())
            if old_cfg != new_cfg:
                self.cfg = new_cfg
                self.logger.info(f"配置变更，时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            # 否则不打印日志
        except Exception as e:
            self.logger.exception(f"配置刷新失败: {e}")
    
    def __write_cfg(self):
        '''
        Write current configuration to cfg file
        '''
        with open(self.cfg_file, 'w') as f:
            f.write(json.dumps(self.cfg, indent=4))
    
    def __start_cfg_refresh_timer(self):
        '''
        启动配置刷新定时器
        '''
        if self.timer:
            self.timer.cancel()
        
        self.timer = threading.Timer(self.refresh_interval, self.__cfg_refresh_cycle)
        self.timer.daemon = True  # 设置为守护线程，主程序退出时自动结束
        self.timer.start()
    
    def __cfg_refresh_cycle(self):
        '''
        配置刷新循环
        '''
        try:
            self.__refresh_cfg()
        except Exception as e:
            self.logger.exception(f"配置刷新失败: {e}")
        
        # 重新启动定时器
        self.__start_cfg_refresh_timer()
    
    def stop_cfg_refresh(self):
        '''
        停止配置自动刷新
        '''
        if self.timer:
            self.timer.cancel()
            self.timer = None
            self.logger.info("配置自动刷新已停止")
    
    def set_refresh_interval(self, interval):
        '''
        设置刷新间隔（秒）
        '''
        self.refresh_interval = interval
        self.__start_cfg_refresh_timer()
        self.logger.info(f"配置刷新间隔已设置为: {interval}秒")

    ## ** PART 2 : 代理管理相关函数 ** ##
    def __get_proxy(self, proxy = None):
        '''
        TODO: get proxy from proxy pool
        '''
        if not proxy:
            return None
        
        return {
            "http": f"http://{proxy}",
            "https": f"http://{proxy}",
        }

    ## ** PART 3 : 数据库相关写入函数 ** ##
    def write_db(self, src, id, entity_type):
        '''
        写入数据库
        
        如果第一次访问，last_visit_time 设置为 visit_time，visit_times 设置为 1
        如果之前访问过，last_visit_time 更新为上一次的 visit_time，visit_time 更新为当前时间，visit_times + 1
        '''
        time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # 检查记录是否存在，存在的话，time_str --> visit_time, visit_time --> last_visit_time, visit_times --> visit_times + 1
        cursor.execute('''
            SELECT COUNT(*) FROM record WHERE src=? AND id=?
        ''', (src, id))
        exists = cursor.fetchone()[0] > 0
        if exists:
            # 获取上一次访问时间
            cursor.execute('''
                SELECT visit_time FROM record WHERE src=? AND id=?
            ''', (src, id))
            visit_time = cursor.fetchone()[0]
            cursor.execute('''
                UPDATE record SET visit_time=?, last_visit_time=?, visit_times=visit_times+1 WHERE src=? AND id=?
            ''', (time_str, visit_time, src, id))
        else:
            visit_time = time_str
            last_visit_time = time_str
            cursor.execute('''
                INSERT INTO record (src, id, entity_type, visit_time, last_visit_time, visit_times)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (src, id, entity_type, visit_time, last_visit_time, 1))
        conn.commit()
        conn.close()

    def add_to_todo(self, src, id, entity_type):
        '''
        添加待爬取记录到todo表
        '''
        time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()

        # 插入记录，如果已存在则忽略
        cursor.execute('''
            INSERT OR IGNORE INTO todo (src, id, entity_type, found_time)
            VALUES (?, ?, ?, ?)
        ''', (src, id, entity_type, time_str))
        
        conn.commit()
        conn.close()

    ## ** PART 4 : 相关统计函数 ** ##
    def __get_stats(self):
        '''
        获取统计信息
        '''
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        
        # 获取record表中的记录数
        cursor.execute("SELECT COUNT(*) FROM record")
        record_count = cursor.fetchone()[0]

        # 分src / entity_type统计
        cursor.execute("SELECT src, entity_type, COUNT(*) FROM record GROUP BY src, entity_type")
        record_stats = {}
        for src, entity_type, count in cursor.fetchall():
            if src not in record_stats:
                record_stats[src] = {}
            record_stats[src][entity_type] = count
        
        # 获取todo表中的记录数
        cursor.execute("SELECT COUNT(*) FROM todo")
        todo_count = cursor.fetchone()[0]

        # 分src / entity_type统计
        cursor.execute("SELECT src, entity_type, COUNT(*) FROM todo GROUP BY src, entity_type")
        todo_stats = {}
        for src, entity_type, count in cursor.fetchall():
            if src not in todo_stats:
                todo_stats[src] = {}
            todo_stats[src][entity_type] = count

        # 时效性统计（7天，7-30天，30-90天，90天以上）
        time_frames = {"7_days": 7, "7_30_days": 30, "30_90_days": 90, "90_plus_days": 3650}
        now = datetime.now()
        record_timeframe_stats = {}
        for key, days in time_frames.items():
            cursor.execute(f'''
                SELECT COUNT(*) FROM record 
                WHERE julianday(?) - julianday(visit_time) <= ?
            ''', (now.strftime('%Y-%m-%d'), days))
            count = cursor.fetchone()[0]
            record_timeframe_stats[key] = count
        
        conn.close()
        
        return {
            "record_count": record_count,
            "todo_count": todo_count,
            "record_stats": record_stats,
            "todo_stats": todo_stats,
            "record_timeframe_stats": record_timeframe_stats
        }

