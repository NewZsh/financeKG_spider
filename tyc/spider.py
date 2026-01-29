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

        # 数据目录
        self.data_direc = self.s_cfg["data_direc"]
        self.data_direc = os.path.join(cur_dir, self.data_direc)
        os.makedirs(self.data_direc, exist_ok=True)

        # 请求头有效性标志
        # 多次请求失败后，置为False，暂停请求，等待人工干预
        self.headers_is_valid = True
        self.headers_trial_limit = 3  # 允许的连续失败次数

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
    
    def __get_investment_page(self, company_gid, page_num=1, page_size=100):
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
            
            self.logger.info(f"正在爬取公司 {company_gid} 的第 {page_num} 页投资信息...")
            
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
                self.logger.info(f"成功获取第 {page_num} 页数据")
                return data
            else:
                error_msg = data.get("message", "Unknown error")
                self.logger.error(f"API 返回错误: {error_msg}")
                return None
            
        except requests.exceptions.RequestException as e:
            self.logger.exception(f"网络请求失败: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.exception(f"JSON 解析失败: {e}")
            return None
        except Exception as e:
            self.logger.exception(f"获取投资信息失败: {e}")
            return None

    def __parse_investment_data(self, investments):
        """
        解析投资数据，提取关键信息
        Args:
            investments: 投资记录列表
        
        Returns:
            list: 解析后的投资信息
        """
        company_base_info_dict = {}
        for inv in investments:
            id = inv.get("id")
            if not id:
                continue
            
            company_base_info = inv.get("companyBaseInfo", {})
            company_industry = inv.get("companyIndustry", {})
            if "industryInfo" not in company_base_info:
                company_base_info["industryInfo"] = company_industry
            company_base_info_dict[id] = company_base_info

            inv.pop("companyBaseInfo", None)
        
        return investments, company_base_info_dict

    def get_all_investment(self, company_gid, save_to_file=False):
        """
        获取公司的所有对外投资信息（自动处理分页）
        Args:
            company_gid: 公司 ID
        
        Returns:
            list: 所有投资记录
        """
        if not self.headers_is_valid:
            self.logger.warning("请求头无效，暂停请求")
            raise Exception("请求头无效，暂停请求")
        
        output_file = os.path.join(self.data_direc, f"investments_{company_gid}.json")
        if os.path.exists(output_file):
            self.logger.info(f"数据文件 {output_file} 已存在，跳过爬取")
            return

        self.logger.info(f"\n========== 开始爬取公司 {company_gid} 的对外投资信息 ==========")
        
        page_num = 1
        page_size = 100
        parsed_cnt = 0
        sleep_seconds = self.get_request_sleep_seconds()
        
        failure_count = 0
        id_found = set()
        while True:
            # 发送请求前等待
            if page_num > 1:
                self.logger.info(f"等待 {sleep_seconds} 秒后发送下一个请求...")
                time.sleep(sleep_seconds)
            
            # 获取当前页数据
            response_data = self.__get_investment_page(company_gid, page_num, page_size)
            
            if response_data is None:
                self.logger.error(f"获取第 {page_num} 页失败，停止爬取")
                failure_count += 1
                if failure_count >= self.headers_trial_limit:
                    self.logger.error("连续多次请求失败，请求头设置为无效，等待人工干预")
                    self.headers_is_valid = False

                break
            
            # 提取数据
            page_data = response_data.get("data", {})
            investments = page_data.get("result", [])
            total = page_data.get("total", 0)
            parsed_cnt += len(investments)
                        
            # 解析数据
            investments, company_base_info_dict = self.__parse_investment_data(investments)
                    
            # 可选：保存到文件（仅在本页有记录时写入，避免创建空文件）
            if save_to_file:
                if investments:
                    output_file = os.path.join(self.data_direc, f"investments_{company_gid}.json")
                    with open(output_file, 'a', encoding='utf-8') as f:
                        for inv in investments:
                            f.write(json.dumps(inv, ensure_ascii=False) + "\n")
                    for id, base_info in company_base_info_dict.items():
                        id_found.add(id)
                        output_file = os.path.join(self.data_direc, f"base_info_{id}.json")
                        if not os.path.exists(output_file):
                            with open(output_file, 'w', encoding='utf-8') as f:
                                f.write(json.dumps(base_info, ensure_ascii=False) + "\n")
                else:
                    self.logger.info(f"第 {page_num} 页无投资记录，跳过文件写入")

            self.logger.info(f"第 {page_num} 页获取 {len(investments)} 条记录，总共 {total} 条")
            
            # 检查是否需要继续分页
            if parsed_cnt >= total:
                self.logger.info(f"所有 {total} 条记录已获取完毕")
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
                self.logger.warning("页码超过限制，停止爬取")
                break
        
        return list(id_found)

    def close_session(self):
        """关闭 Session，释放连接资源"""
        if self.session:
            self.session.close()
            self.logger.info("Session 已关闭，资源已释放")


if __name__ == "__main__":
    # 示例：爬取视源电子 (gid: 1391758803) 的对外投资信息
    spider = TYCSpider()
      
    id_found = ["1391758803"] # 395739442 视源电子 1391758803 视睿电子

    while True:
        for company_gid in id_found:
            try:
                id_found = spider.get_all_investment(company_gid, save_to_file=True)
                spider.logger.info(f"本次爬取完成，发现 {len(id_found)} 家被投资公司")
            except Exception as e:
                id_found = []
                spider.close_session()
                break
        
        if len(id_found) == 0:
            break
