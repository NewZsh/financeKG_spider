import os
import json
import threading
import time
try:
    import fcntl  # 文件锁支持 (Unix only)
except ImportError:
    fcntl = None
from datetime import datetime

cur_dir = os.path.dirname(os.path.abspath(__file__))

class base_spider:
    def __init__(self):
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

        self.__load_cfg()
        self.__start_cfg_refresh_timer()

    def __load_cfg(self):
        '''
        Load configuration from cfg file
        '''
        try:
            self.cfg = json.loads(open(self.cfg_file, 'r').read())
            print(f"配置加载成功，时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"配置加载失败: {e}")

    # cfg 允许热更，更新参数直接作用于爬虫后台而无需重启
    def __refresh_cfg(self):
        '''
        重新加载配置
        '''
        self.__load_cfg()
    
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
        print(f"配置自动刷新定时器已启动，间隔: {self.refresh_interval}秒")
    
    def __cfg_refresh_cycle(self):
        '''
        配置刷新循环
        '''
        try:
            self.__refresh_cfg()
        except Exception as e:
            print(f"配置刷新失败: {e}")
        
        # 重新启动定时器
        self.__start_cfg_refresh_timer()
    
    def stop_cfg_refresh(self):
        '''
        停止配置自动刷新
        '''
        if self.timer:
            self.timer.cancel()
            self.timer = None
            print("配置自动刷新已停止")
    
    def set_refresh_interval(self, interval):
        '''
        设置刷新间隔（秒）
        '''
        self.refresh_interval = interval
        self.__start_cfg_refresh_timer()
        print(f"配置刷新间隔已设置为: {interval}秒")

    # 小批量更新相关方法
    def add_to_update_buffer(self, record_data):
        """
        添加记录到更新缓冲区
        Args:
            record_data: 记录数据字典，包含id, name, type等信息
        """
        with self.buffer_lock:
            self.update_buffer.append(record_data)
            
            # 如果缓冲区达到批量大小，立即执行更新
            if len(self.update_buffer) >= self.batch_size:
                self._flush_buffer()
    
    def _flush_buffer(self):
        """将缓冲区数据写入文件"""
        if not self.update_buffer:
            return
            
        with self.file_lock:
            try:
                # 读取现有文件内容
                existing_data = {}
                if os.path.exists(self.id_file_snapshot):
                    with open(self.id_file_snapshot, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip() and not line.startswith('#'):
                                parts = line.strip().split('\t')
                                if len(parts) >= 1:
                                    existing_data[parts[0]] = line.strip()
                
                # 更新或添加新记录
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                for record in self.update_buffer:
                    record_id = str(record.get('id', ''))
                    if record_id:
                        # 构建记录行
                        name = record.get('name', '')
                        record_type = record.get('type', '')
                        
                        # 如果记录已存在，更新last_spider_time；否则创建新记录
                        if record_id in existing_data:
                            existing_line = existing_data[record_id]
                            parts = existing_line.split('\t')
                            if len(parts) >= 5:
                                # 更新最后爬取时间
                                new_line = f"{record_id}\t{name}\t{record_type}\t{parts[3]}\t{current_time}"
                            else:
                                new_line = f"{record_id}\t{name}\t{record_type}\t{current_time}\t{current_time}"
                        else:
                            new_line = f"{record_id}\t{name}\t{record_type}\t{current_time}\t{current_time}"
                        
                        existing_data[record_id] = new_line
                
                # 写回文件
                with open(self.id_file_snapshot, 'w', encoding='utf-8') as f:
                    f.write("# 公司ID历史记录文件\n")
                    f.write("# 格式: id \\t name \\t type \\t spider_time \\t last_spider_time\n\n")
                    for record_line in existing_data.values():
                        f.write(record_line + '\n')
                
                print(f"批量更新完成，处理了 {len(self.update_buffer)} 条记录")
                
                # 清空缓冲区
                self.update_buffer.clear()
                
            except Exception as e:
                print(f"文件更新失败: {e}")
    
    def set_batch_size(self, size):
        """设置批量大小"""
        self.batch_size = size
        print(f"批量大小设置为: {size}")
    
    def force_flush(self):
        """强制刷新缓冲区"""
        with self.buffer_lock:
            if self.update_buffer:
                self._flush_buffer()
    
    def get_stats(self):
        """获取统计信息（不干扰爬虫操作）"""
        with self.file_lock:
            try:
                if not os.path.exists(self.id_file_snapshot):
                    return {"total_records": 0, "last_updated": None}
                
                with open(self.id_file_snapshot, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                # 过滤注释行和空行
                data_lines = [line for line in lines if line.strip() and not line.startswith('#')]
                
                stats = {
                    "total_records": len(data_lines),
                    "buffer_size": len(self.update_buffer),
                    "last_updated": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                return stats
                
            except Exception as e:
                print(f"获取统计信息失败: {e}")
                return {"error": str(e)}

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
