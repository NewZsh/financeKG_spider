# -*- coding: utf-8 -*-

# financeKG_spider/qxb/spider.py
# Date: 2024-06-10
# Description: Spider for qixinbao website

import aiohttp
import asyncio
import os
import time
import random
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
        
        # 初始化Session，保持会话和复用连接
        self.session = requests.Session()
        self.session.headers.update(self.get_headers())
    
    def get_headers(self):
        """
        获取完整的请求头（模拟Mac Chrome真实请求头）
        核心思路：模拟真人浏览器的请求特征，降低被检测的概率
        """
        return {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.qixin.com/",  # 来源页，模拟从首页跳转
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Connection": "keep-alive"
        }

    def get_company_info_byID(self, company_id):
        '''
        Get company information by company ID
        '''
        try:
            # 构建请求URL
            url = f"{self.s_cfg['base_url']}company/{company_id}"
            print(f"正在爬取公司ID: {company_id}, URL: {url}")
            
            # 随机延迟（1-3秒），模拟真人操作间隔
            delay = random.uniform(1, 3)
            print(f"等待 {delay:.2f} 秒...")
            time.sleep(delay)
            
            # 使用Session发送请求（复用连接和Cookie，允许重定向）
            response = self.session.get(
                url,
                timeout=10,
                allow_redirects=True
            )
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
    
    def close_session(self):
        """
        关闭Session，释放连接资源
        在爬取完成后调用，确保资源正确释放
        """
        if self.session:
            self.session.close()
            print("Session已关闭，资源已释放")
    
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
                
                # 批次间随机延迟（每10条增加更长延迟，避免连续快速请求）
                if (i + 1) % 10 == 0:
                    batch_delay = random.uniform(5, 8)
                    print(f"批次间隔延迟 {batch_delay:.2f} 秒...")
                    time.sleep(batch_delay)
                
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