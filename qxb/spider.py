# -*- coding: utf-8 -*-

# financeKG_spider/qxb/spider.py
# Date: 2024-06-10
# Description: Spider for qixinbao website

import aiohttp
import asyncio
import os
import time
from datetime import datetime
import threading
import requests
from bs4 import BeautifulSoup

cur_dir = os.path.dirname(os.path.abspath(__file__))

# 添加父目录到路径，以便导入
import sys
sys.path.append(os.path.dirname(cur_dir))

from base_spider import base_spider

class QXBSpider(base_spider):
    def __init__(self):
        super().__init__()

        self.s_cfg = self.cfg[self.__class__.__name__]

        # id \t name \t type \t spider_time \t last_spider_time
        self.id_file_snapshot = self.s_cfg["id_file_snapshot"]
        if not os.path.exists(self.id_file_snapshot):
            os.makedirs(os.path.dirname(self.id_file_snapshot), exist_ok=True)
            open(self.id_file_snapshot, 'w').close() 
        
        # 统计相关
        self.stats_data = {
            "total_crawled": 0,
            "success_count": 0,
            "error_count": 0,
            "last_crawl_time": None,
            "current_batch_size": 0
        }
        self.stats_lock = threading.Lock()
    
    def stats(self):
        """
        获取爬虫统计信息（不干扰爬虫操作）
        """
        with self.stats_lock:
            file_stats = self.get_stats()
            
            stats_info = {
                "文件统计": file_stats,
                "爬虫运行统计": self.stats_data.copy(),
                "缓冲区状态": {
                    "当前缓冲区大小": len(self.update_buffer),
                    "批量大小设置": self.batch_size
                },
                "配置信息": {
                    "配置刷新间隔": f"{self.refresh_interval}秒",
                    "最后配置加载": time.strftime('%Y-%m-%d %H:%M:%S')
                }
            }
            
            return stats_info
    
    def update_crawl_stats(self, success=True):
        """更新爬虫统计"""
        with self.stats_lock:
            self.stats_data["total_crawled"] += 1
            if success:
                self.stats_data["success_count"] += 1
            else:
                self.stats_data["error_count"] += 1
            self.stats_data["last_crawl_time"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.stats_data["current_batch_size"] = len(self.update_buffer)

    def get_headers(self):
        return {
            "User-Agent": self.ua_list[0]
        }

    def get_company_info_byID(self, company_id):
        '''
        Get company information by company ID
        '''
        try:
            # 构建请求URL
            url = f"{self.s_cfg['base_url']}company/{company_id}"
            print(f"正在爬取公司ID: {company_id}, URL: {url}")
            
            # 发送HTTP请求
            headers = self.get_headers()
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # 解析HTML
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 提取公司基本信息
            company_info = {
                "id": company_id,
                "name": "",
                "type": "",
                "status": "",
                "legal_person": "",
                "registered_capital": "",
                "establishment_date": "",
                "address": "",
                "business_scope": "",
                "phone": "",
                "email": "",
                "website": "",
                "credit_code": ""
            }
            
            # 从标题提取公司名称
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text()
                if '-' in title_text:
                    company_info["name"] = title_text.split('-')[0].strip()
            
            # 提取法定代表人
            legal_person_span = soup.find('span', string='法定代表人：')
            if legal_person_span:
                parent_div = legal_person_span.find_parent('div')
                if parent_div:
                    link = parent_div.find('a')
                    if link:
                        company_info["legal_person"] = link.get_text().strip()
            
            # 提取注册资本
            capital_span = soup.find('span', string='注册资本：')
            if capital_span:
                parent_div = capital_span.find_parent('div')
                if parent_div:
                    capital_text = parent_div.find('span')
                    if capital_text:
                        company_info["registered_capital"] = capital_text.get_text().strip()
            
            # 提取成立日期
            date_span = soup.find('span', string='成立日期：')
            if date_span:
                parent_div = date_span.find_parent('div')
                if parent_div:
                    date_text = parent_div.find('span')
                    if date_text:
                        company_info["establishment_date"] = date_text.get_text().strip()
            
            # 提取统一社会信用代码
            credit_span = soup.find('span', string='统一社会信用代码：')
            if credit_span:
                parent_div = credit_span.find_parent('div')
                if parent_div:
                    credit_text = parent_div.find('span', class_='credit-number')
                    if credit_text:
                        company_info["credit_code"] = credit_text.get_text().strip()
            
            # 提取地址
            address_span = soup.find('span', string='地址：')
            if address_span:
                parent_span = address_span.find_parent('span')
                if parent_span:
                    address_text = parent_span.find('span', class_='clickable-text')
                    if address_text:
                        company_info["address"] = address_text.get_text().strip()
            
            # 提取经营范围
            scope_td = soup.find('td', string='经营范围')
            if scope_td:
                parent_tr = scope_td.find_parent('tr')
                if parent_tr:
                    scope_span = parent_tr.find('span')
                    if scope_span:
                        company_info["business_scope"] = scope_span.get_text().strip()
            
            # 提取电话
            phone_span = soup.find('span', string='电话：')
            if phone_span:
                parent_div = phone_span.find_parent('div')
                if parent_div:
                    phone_text = parent_div.find('span')
                    if phone_text:
                        company_info["phone"] = phone_text.get_text().strip()
            
            # 提取经营状态
            status_td = soup.find('td', string='经营状态')
            if status_td:
                parent_tr = status_td.find_parent('tr')
                if parent_tr:
                    status_span = parent_tr.find('span')
                    if status_span:
                        company_info["status"] = status_span.get_text().strip()
            
            # 设置默认值
            if not company_info["type"]:
                company_info["type"] = "股份有限公司"  # 默认值
            if not company_info["status"]:
                company_info["status"] = "存续"  # 默认值
            
            print(f"成功提取公司信息: {company_info['name']}")
            
            # 使用小批量更新机制记录爬取结果
            self.add_to_update_buffer(company_info)
            
            # 更新统计
            self.update_crawl_stats(success=True)
            
            return company_info
            
        except requests.exceptions.RequestException as e:
            print(f"网络请求失败: {e}")
            self.update_crawl_stats(success=False)
            return None
        except Exception as e:
            print(f"解析公司信息失败: {e}")
            self.update_crawl_stats(success=False)
            return None
    
    def get_company_IDlist(self, keyword):
        '''
        Get a list of company IDs by keyword search
        '''
        # 模拟搜索操作
        print(f"正在搜索关键词: {keyword}")
        
        # 模拟搜索结果
        search_results = [
            {"id": "100001", "name": f"{keyword}公司A"},
            {"id": "100002", "name": f"{keyword}公司B"},
            {"id": "100003", "name": f"{keyword}公司C"}
        ]
        
        return search_results
    
    def crawl_companies_batch(self, company_ids, batch_size=50):
        """
        批量爬取公司信息
        Args:
            company_ids: 公司ID列表
            batch_size: 每批处理的数量
        """
        print(f"开始批量爬取 {len(company_ids)} 家公司信息，批量大小: {batch_size}")
        
        # 设置批量大小
        self.set_batch_size(batch_size)
        
        total_count = len(company_ids)
        success_count = 0
        
        for i, company_id in enumerate(company_ids):
            try:
                # 爬取单个公司信息
                company_info = self.get_company_info_byID(company_id)
                success_count += 1
                
                # 显示进度
                if (i + 1) % 10 == 0:
                    progress = (i + 1) / total_count * 100
                    print(f"进度: {i + 1}/{total_count} ({progress:.1f}%)")
                
                # 小批量延迟，避免过快请求
                time.sleep(0.1)
                
            except Exception as e:
                print(f"爬取公司 {company_id} 失败: {e}")
                self.update_crawl_stats(success=False)
        
        # 强制刷新缓冲区，确保所有数据写入文件
        self.force_flush()
        
        print(f"批量爬取完成，成功: {success_count}/{total_count}")
        
        # 返回统计信息
        return self.stats()


if __name__ == "__main__":
    spider = QXBSpider()
    
    company_id = "3a424331-f1e8-491f-b07e-6b3ab59dd75b"

    info = spider.get_company_info_byID(company_id)