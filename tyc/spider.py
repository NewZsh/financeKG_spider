# -*- coding: utf-8 -*-

# financeKG_spider/tyc/spider.py
# Date: 2026-01-28
# Description: Spider for tianyancha (天眼查) website - Investment Information Crawler

import requests
import time
import json
import os
import sys
from datetime import datetime

cur_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(cur_dir))

from base_spider import base_spider


class TYCSpider(base_spider):
    def __init__(self):
        super().__init__()
        self.s_cfg = self.cfg[self.__class__.__name__]
        
        # 初始化 Session，保持会话和复用连接
        self.session = requests.Session()
        self.session.headers.update(self.get_headers())
    
    def get_headers(self):
        """
        获取请求头
        从 spider.cfg 中读取，允许用户手动更新关键鉴权信息
        """
        headers = self.s_cfg.get("headers", {})
        return headers
    
    def get_api_url(self):
        """获取 API 基础 URL"""
        return self.s_cfg.get("api_base_url", "https://capi.tianyancha.com/cloud-company-background/company/investListV2")
    
    def get_request_sleep_seconds(self):
        """获取请求间隔（秒）"""
        return self.s_cfg.get("request_sleep_seconds", 3)
    
    def build_request_body(self, company_gid, page_num=1, page_size=100):
        """
        构建请求体
        Args:
            company_gid: 公司 ID (gid)
            page_num: 页码（从1开始）
            page_size: 每页数量
        
        Returns:
            dict: 请求体
        """
        return {
            "gid": str(company_gid),
            "pageSize": page_size,
            "pageNum": page_num,
            "benefitSharesType": 1,
            "percentLevel": "-100",
            "registation": "-100",
            "province": "-100",
            "category": "-100",
            "fullSearchText": ""
        }
    
    def get_investment_list(self, company_gid, page_num=1, page_size=100):
        """
        获取单页投资数据
        Args:
            company_gid: 公司 ID
            page_num: 页码
        
        Returns:
            dict: API 返回的响应数据
        """
        try:
            url = self.get_api_url()
            
            # 构建请求体
            request_body = self.build_request_body(company_gid, page_num, page_size)
            
            print(f"正在爬取公司 {company_gid} 的第 {page_num} 页投资信息...")
            
            # 发送 POST 请求
            response = self.session.post(
                url,
                json=request_body,
                timeout=15,
                allow_redirects=True
            )
            response.raise_for_status()
            
            # 解析 JSON 响应
            data = response.json()
            
            # 检查响应状态
            if data.get("state") == "ok":
                print(f"成功获取第 {page_num} 页数据")
                return data
            else:
                error_msg = data.get("message", "Unknown error")
                print(f"API 返回错误: {error_msg}")
                return None
            
        except requests.exceptions.RequestException as e:
            print(f"网络请求失败: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON 解析失败: {e}")
            return None
        except Exception as e:
            print(f"获取投资信息失败: {e}")
            return None
    
    def get_all_investments(self, company_gid):
        """
        获取公司的所有对外投资信息（自动处理分页）
        Args:
            company_gid: 公司 ID
        
        Returns:
            list: 所有投资记录
        """
        all_investments = []
        page_num = 1
        page_size = 100
        sleep_seconds = self.get_request_sleep_seconds()
        
        while True:
            # 发送请求前等待
            if page_num > 1:
                print(f"等待 {sleep_seconds} 秒后发送下一个请求...")
                time.sleep(sleep_seconds)
            
            # 获取当前页数据
            response_data = self.get_investment_list(company_gid, page_num, page_size)
            
            if response_data is None:
                print(f"获取第 {page_num} 页失败，停止爬取")
                break
            
            # 提取数据
            page_data = response_data.get("data", {})
            investments = page_data.get("result", [])
            total = page_data.get("total", 0)
            
            # 添加到列表
            all_investments.extend(investments)
            
            print(f"第 {page_num} 页获取 {len(investments)} 条记录，总共 {total} 条")
            
            # 检查是否需要继续分页
            if len(all_investments) >= total:
                print(f"所有 {total} 条记录已获取完毕")
                break
            
            # 计算下一页
            next_page = page_num + 1
            if next_page * page_size > total:
                # 最后一页
                page_num = next_page
            else:
                page_num = next_page
            
            # 安全检查：防止无限循环
            if page_num > 1000:
                print("页码超过限制，停止爬取")
                break
        
        print(f"完成爬取，共获得 {len(all_investments)} 条投资记录")
        return all_investments
    
    def parse_investment_data(self, investments):
        """
        解析投资数据，提取关键信息
        Args:
            investments: 投资记录列表
        
        Returns:
            list: 解析后的投资信息
        """
        parsed_data = []
        
        for investment in investments:
            parsed_item = {
                "id": investment.get("id"),  # 被投资公司 ID
                "name": investment.get("name"),  # 被投资公司名称
                "regStatus": investment.get("regStatus"),  # 注册状态
                "province": investment.get("province"),  # 所在省份
                "city": investment.get("city"),  # 所在城市
                "category": investment.get("category"),  # 行业分类
                "amount": investment.get("amount"),  # 投资金额
                "percent": investment.get("percent"),  # 占股比例
                "legalPersonName": investment.get("legalPersonName"),  # 法定代表人
                "establishTime": investment.get("estiblishTime"),  # 成立时间戳
                "regCapital": investment.get("companyBaseInfo", {}).get("regCapital") if investment.get("companyBaseInfo") else None,  # 注册资本
                "industry": investment.get("companyIndustry", {}).get("nameLevel1") if investment.get("companyIndustry") else None,  # 行业
            }
            
            parsed_data.append(parsed_item)
        
        return parsed_data
    
    def crawl_company_investments(self, company_gid, save_to_file=False):
        """
        爬取公司的对外投资信息
        Args:
            company_gid: 公司 ID
            save_to_file: 是否保存到文件
        
        Returns:
            list: 投资记录列表
        """
        print(f"\n========== 开始爬取公司 {company_gid} 的对外投资信息 ==========")
        
        # 获取所有投资信息（自动分页）
        investments = self.get_all_investments(company_gid)
        
        # 解析数据
        parsed_investments = self.parse_investment_data(investments)
                
        # 可选：保存到文件
        if save_to_file:
            output_file = os.path.join(cur_dir, f"investments_{company_gid}.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(parsed_investments, f, ensure_ascii=False, indent=2)
            print(f"投资数据已保存到: {output_file}")
        
        return parsed_investments
    
    def close_session(self):
        """关闭 Session，释放连接资源"""
        if self.session:
            self.session.close()
            print("Session 已关闭，资源已释放")


if __name__ == "__main__":
    # 示例：爬取视源电子 (gid: 1391758803) 的对外投资信息
    spider = TYCSpider()
    
    company_gid = "1391758803"  # 395739442 视源电子 1391758803 视睿电子
    
    try:
        # 爬取投资信息
        investments = spider.crawl_company_investments(company_gid, save_to_file=True)
        
        print("\n========== 投资信息示例 ==========")
        for i, inv in enumerate(investments):
            print(f"\n{i}. {inv.get('name')}")
            print(f"   地区: {inv.get('province')} {inv.get('city')}")
            print(f"   行业: {inv.get('category')}")
            print(f"   占股比例: {inv.get('percent')}")
            print(f"   投资金额: {inv.get('amount')}")
    
    finally:
        spider.close_session()
