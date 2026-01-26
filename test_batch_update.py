#!/usr/bin/env python3
"""
测试小批量更新机制和统计功能
"""

import time
import threading
from qxb.spider import QXBSpider

def test_batch_update():
    """测试小批量更新功能"""
    print("=== 小批量更新机制测试 ===")
    
    # 创建爬虫实例
    spider = QXBSpider()
    
    # 测试统计功能（不干扰爬虫）
    print("\n1. 初始统计信息:")
    stats = spider.stats()
    for category, data in stats.items():
        print(f"{category}:")
        for key, value in data.items():
            print(f"  {key}: {value}")
    
    # 模拟批量爬取
    print("\n2. 开始模拟批量爬取...")
    company_ids = [f"{i:06d}" for i in range(1, 251)]  # 250家公司
    
    # 设置较小的批量大小进行测试
    spider.set_batch_size(30)
    
    # 开始批量爬取
    result_stats = spider.crawl_companies_batch(company_ids, batch_size=30)
    
    print("\n3. 批量爬取完成后的统计信息:")
    for category, data in result_stats.items():
        print(f"{category}:")
        for key, value in data.items():
            print(f"  {key}: {value}")

def test_concurrent_access():
    """测试并发访问（爬虫和统计同时进行）"""
    print("\n=== 并发访问测试 ===")
    
    spider = QXBSpider()
    
    def crawler_thread():
        """爬虫线程"""
        for i in range(50):
            company_id = f"{i+300:06d}"
            spider.get_company_info_byID(company_id)
            time.sleep(0.05)  # 模拟爬取间隔
    
    def stats_thread():
        """统计线程"""
        for i in range(10):
            stats = spider.stats()
            print(f"[统计线程] 第{i+1}次统计 - 缓冲区大小: {stats['缓冲区状态']['当前缓冲区大小']}")
            time.sleep(0.3)
    
    # 启动线程
    crawler = threading.Thread(target=crawler_thread)
    stats_monitor = threading.Thread(target=stats_thread)
    
    crawler.start()
    stats_monitor.start()
    
    # 等待线程完成
    crawler.join()
    stats_monitor.join()
    
    # 强制刷新缓冲区
    spider.force_flush()
    
    print("并发测试完成！")

def test_config_hot_reload():
    """测试配置热更与小批量更新的结合"""
    print("\n=== 配置热更与小批量更新结合测试 ===")
    
    spider = QXBSpider()
    
    # 初始统计
    print("初始批量大小:", spider.batch_size)
    
    # 修改配置（模拟热更）
    print("修改批量大小为50...")
    spider.set_batch_size(50)
    
    # 测试爬取
    for i in range(60):
        company_id = f"{i+400:06d}"
        spider.get_company_info_byID(company_id)
    
    # 强制刷新
    spider.force_flush()
    
    # 最终统计
    stats = spider.stats()
    print("最终统计:")
    print(f"总记录数: {stats['文件统计']['total_records']}")
    print(f"缓冲区大小: {stats['缓冲区状态']['当前缓冲区大小']}")
    
    # 停止配置刷新
    spider.stop_cfg_refresh()
    print("配置热更已停止")

if __name__ == "__main__":
    print("小批量更新机制测试套件")
    print("=" * 50)
    
    # 运行测试
    test_batch_update()
    test_concurrent_access()
    test_config_hot_reload()
    
    print("\n所有测试完成！")