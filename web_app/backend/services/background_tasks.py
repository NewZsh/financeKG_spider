import threading
import time
import os
import sys

# 设置根目录路径，方便导入其他模块
# web_app/backend/services/background_tasks.py -> web_app/backend/services -> web_app/backend -> web_app -> financeKG_spider
cur_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.abspath(os.path.join(cur_dir, '..', '..', '..', '..'))
if root_dir not in sys.path:
    sys.path.append(root_dir)

from base_spider import ThreadSafeUniqueQueue
from tyc.spider import TYCSpider

# 全局队列
tyc_id_collect_queue = ThreadSafeUniqueQueue()


def _tyc_kw_watcher(poll_interval=10):
    time.sleep(5)  # 延迟启动，避免阻塞 FastAPI 主循环
    watcher_spider = TYCSpider(tyc_id_collect_queue)
    kw_dir_rel = watcher_spider.s_cfg.get("keywords_direc", "./data/tyc_keywords/")
    kw_dir = os.path.abspath(os.path.join(root_dir, kw_dir_rel))
    os.makedirs(kw_dir, exist_ok=True)

    finished_file = watcher_spider.s_cfg.get("keywords_finised_fn", os.path.join(kw_dir, "keywords_finished.txt"))
    if not os.path.exists(finished_file):
        open(finished_file, 'a', encoding='utf-8').close()

    def _load_finished():
        try:
            with open(finished_file, 'r', encoding='utf-8') as f:
                return set([l.strip() for l in f if l.strip()])
        except Exception:
            return set()

    while True:
        finished = _load_finished()
        for fn in os.listdir(kw_dir):
            if fn == os.path.basename(finished_file):
                continue
            if fn.startswith('.'): # 避开重命名为.processed开头，也避开.finished开头
                continue
            path = os.path.join(kw_dir, fn)
            if not os.path.isfile(path):
                continue

            try:
                with open(path, 'r', encoding='utf-8') as f:
                    kws = [ln.strip() for ln in f if ln.strip()]
            except Exception:
                watcher_spider.logger.exception(f"读取关键词文件失败: {path}")
                continue

            for kw in kws:
                if kw in finished:
                    continue

                try:
                    # 假设 search_companies 是用来处理关键字查询的
                    result = watcher_spider.search_companies(kw, max_page=2, save_to_file=True)

                    with open(finished_file, 'a', encoding='utf-8') as ff:
                        ff.write(kw + '\n')
                    finished.add(kw)
                except Exception:
                    watcher_spider.logger.exception(f"Watcher 搜索失败: {kw}")

            # 尝试将已处理的文件重命名，避免再次扫描同一上传文件
            try:
                processed_fn = ".processed_" + fn
                processed_path = os.path.join(kw_dir, processed_fn)
                if not os.path.exists(processed_path):
                    os.rename(path, processed_path)
            except Exception:
                watcher_spider.logger.debug(f"无法重命名文件: {path}")
            
        time.sleep(poll_interval)


def _tyc_id_watcher(poll_interval=60):
    time.sleep(10) # 延迟启动
    watcher_spider = TYCSpider(tyc_id_collect_queue)

    # 从 db 中的 todo 表获取待爬取的 id，进行爬取
    watcher_spider.load_db()

    while True:
        if watcher_spider.id_collect_queue.empty():
            time.sleep(poll_interval)
            continue

        try:
            company_gid = watcher_spider.id_collect_queue.get()
            id_found = watcher_spider.get_all_investment(company_gid, save_to_file=True)
            if id_found:
                watcher_spider.logger.info(f"本次爬取完成，发现 {len(id_found)} 家被投资公司")
            result = watcher_spider.get_all_shareholder(company_gid, save_to_file=True)
            if result:
                gid_found, hid_found = result
                watcher_spider.logger.info(f"本次爬取完成，发现 {len(gid_found)} 家企业股东，{len(hid_found)} 位自然人股东")
            watcher_spider.write_db(src="tyc", id=company_gid, entity_type="1")
        except Exception as e:
            watcher_spider.logger.info(f"本次爬取失败，错误原因: {e}")
            watcher_spider.close_session()


def start_background_tasks():
    kw_thread = threading.Thread(target=_tyc_kw_watcher, args=(10,), daemon=True)
    kw_thread.start()
    print("✅ 关键词监控已启动（后台线程）")

    id_thread = threading.Thread(target=_tyc_id_watcher, args=(10,), daemon=True)
    id_thread.start()
    print("✅ 新id监控已启动（后台线程）")
