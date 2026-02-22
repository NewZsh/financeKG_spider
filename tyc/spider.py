# -*- coding: utf-8 -*-

# financeKG_spider/tyc/spider.py
# Date: 2026-01-30
# Description: Spider for tianyancha (天眼查) website
#              - Company Search Crawler
#              - Investment Information Crawler
#              - Shareholder Information Crawler
# 当前版本 v1.0，未支持老旧信息的重新爬取，当前默认是只爬取被第一次发现的id

import sqlite3

import requests
import time
import json
import os
import sys
from datetime import datetime

# back to main directory
cur_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(cur_dir)

from base_spider import base_spider

class TYCSpider(base_spider):
    """天眼查爬虫 - 统一处理公司搜索和投资信息爬取"""
    
    def __init__(self, id_collect_queue):
        super().__init__(id_collect_queue)
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

    def is_id_in_db(self, id):
        conn = sqlite3.connect( self.db_file)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 1 FROM todo WHERE src='tyc' AND entity_type='1' AND id=?
            UNION
            SELECT 1 FROM record WHERE id=?
            LIMIT 1
        """, (id, id))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists

    def filter_new_ids(self, id_list):
        if not id_list:
            return []
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        # 构造批量查询
        placeholders = ','.join(['?'] * len(id_list))
        sql = f"""
            SELECT id FROM todo WHERE src='tyc' AND entity_type='1' AND id IN ({placeholders})
            UNION
            SELECT id FROM record WHERE id IN ({placeholders})
        """
        cursor.execute(sql, id_list + id_list)
        existing_ids = set(row[0] for row in cursor.fetchall())
        conn.close()
        # 返回未在数据库中的ID
        return [i for i in id_list if i not in existing_ids]

    def get_headers(self):
        """
        获取请求头
        从 spider.cfg 中读取，允许用户手动更新关键鉴权信息
        """
        headers = self.s_cfg.get("headers", {}).copy()
        # 添加基础headers
        if "User-Agent" not in headers:
            headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
        return headers
    
    def get_api_url(self):
        """获取 API 基础 URL"""
        return self.s_cfg.get("api_base_url", "https://capi.tianyancha.com/cloud-company-background/company/investListV2")
    
    def get_request_sleep_seconds(self):
        """获取请求间隔（秒）"""
        return self.s_cfg.get("request_sleep_seconds", 3)
    
    def build_request_body(self, company_gid, page_num=1, page_size=100):
        """
        构建投资信息请求体
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
    
    def build_search_request_body(self, keyword, page_num=1, page_size=20):
        """
        构建搜索请求体
        Args:
            keyword: 搜索关键字
            page_num: 页码（从1开始）
            page_size: 每页数量
        
        Returns:
            dict: 请求体
        """
        # 根据用户提供的API请求格式构建，小数点后8位，补0
        session_no = f"{time.time()}0"
        
        filter_json = {
            "economicTypeMethod": {
                "key": "economicTypeMethod",
                "items": [{"value": "1"}]  # 1=内资企业
            },
            "institutionTypeMethod": {
                "key": "institutionTypeMethod",
                "items": [{"value": "1"}]  # 1=企业
            },
            "word": {
                "key": "word",
                "items": [{"value": keyword}]
            }
        }
        
        return {
            "filterJson": json.dumps(filter_json, ensure_ascii=False),
            "searchType": 1,
            "sessionNo": session_no,
            "allowModifyQuery": 1,
            "reportInfo": {
                "page_id": "SearchResult",
                "page_name": "主搜搜索结果页",
                "tab_id": "company",
                "tab_name": "公司",
                "search_session_id": session_no,
                "distinct_id": "5311380"
            },
            "pageNum": page_num,
            "pageSize": page_size
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
    
    def __get_search_page(self, keyword, page_num=1, page_size=20):
        """
        获取单页搜索结果
        Args:
            keyword: 搜索关键字
            page_num: 页码
            page_size: 每页数量
        
        Returns:
            dict: API 返回的响应数据
        """
        try:
            url = "https://capi.tianyancha.com/cloud-tempest/web/searchCompanyV4"
            
            # 构建请求体
            request_body = self.build_search_request_body(keyword, page_num, page_size)
            
            self.logger.info(f"正在爬取关键字 '{keyword}' 的第 {page_num} 页搜索结果...")
            
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
            self.logger.exception(f"获取搜索结果失败: {e}")
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

    def __parse_company_data(self, companies):
        """
        解析公司数据，提取关键信息
        Args:
            companies: 公司列表
        
        Returns:
            list: 公司ID列表
        """
        company_ids = []
        for company in companies:
            company_id = company.get("id")
            if not company_id:
                self.logger.warning(f"公司数据缺少 id 字段: {company}")
                continue
            
            company_ids.append(company_id)
        
        return company_ids
    
    def get_all_shareholder(self, company_gid, save_to_file=False):
        """
        获取公司的所有股东信息（自动处理分页）
        Args:
            company_gid: 公司 ID
            save_to_file: 是否保存到文件
        
        Returns:
            list: 所有股东记录的公司ID列表
        """
        if not self.headers_is_valid:
            self.logger.warning("请求头无效，暂停请求")
            raise Exception("请求头无效，暂停请求")

        output_file_main = os.path.join(self.data_direc, f"shareholders_{company_gid}.json")
        if os.path.exists(output_file_main):
            self.logger.info(f"数据文件 {output_file_main} 已存在，跳过爬取")
            return

        self.logger.info(f"========== 开始爬取公司 {company_gid} 的股东信息 ==========")

        page_num = 1
        page_size = 50
        parsed_cnt = 0
        sleep_seconds = self.get_request_sleep_seconds()

        failure_count = 0
        gid_found = set()
        hid_found = set()

        url = "https://capi.tianyancha.com/cloud-company-background/companyV2/dim/holder/latest/announcement"

        while True:
            if page_num > 1:
                self.logger.info(f"等待 {sleep_seconds} 秒后发送下一个请求...")
                time.sleep(sleep_seconds)

            payload = {
                "gid": str(company_gid),
                "pageSize": page_size,
                "pageNum": page_num,
                "historyType": None,
                "benefitSharesType": 1,
                "_unUseParam": 0
            }

            try:
                response = self.session.post(url, json=payload, timeout=15, allow_redirects=True)
                response.raise_for_status()
                data = response.json()
            except requests.exceptions.RequestException as e:
                self.logger.exception(f"网络请求失败: {e}")
                failure_count += 1
                if failure_count >= self.headers_trial_limit:
                    self.logger.error("连续多次请求失败，请求头设置为无效，等待人工干预")
                    self.headers_is_valid = False
                break
            except json.JSONDecodeError as e:
                self.logger.exception(f"JSON 解析失败: {e}")
                break

            # 检查返回状态
            if data.get("state") != "ok":
                self.logger.error(f"API 返回错误: {data.get('message', 'Unknown error')}")
                failure_count += 1
                if failure_count >= self.headers_trial_limit:
                    self.logger.error("连续多次请求失败，请求头设置为无效，等待人工干预")
                    self.headers_is_valid = False
                break

            failure_count = 0

            page_data = data.get("data") or {}
            shareholders = page_data.get("result")
            total = page_data.get("total")

            if not shareholders:
                self.logger.info(f"第 {page_num} 页无股东记录，停止爬取")
                break

            # 可选：保存到文件（仅在本页有记录时写入，避免创建空文件）
            if save_to_file:
                output_file = os.path.join(self.data_direc, f"shareholders_{company_gid}.json")
                with open(output_file, 'a', encoding='utf-8') as f:
                    for shareholder in shareholders:
                        f.write(json.dumps(shareholder, ensure_ascii=False) + "\n")

            self.logger.info(f"第 {page_num} 页获取 {len(shareholders)} 条记录，总共 {total} 条")

            for shareholder in shareholders:
                # 找到股东ID，注意区分企业股东和自然人股东
                hid = shareholder.get("shareHolderHid")
                gid = shareholder.get("shareHolderGid")

                if hid == gid: # 企业股东
                    gid_found.add(gid)
                else: 
                    # 天眼查的自然人股东的id规则是  {hid}-c{gid} 防止人名相同时候无法区分
                    # 对于实际是同一个人控股两个公司 {hid}-c{gid1} 等价 {hid}-c{gid2}
                    hid = f"{hid}-c{gid}"
                    hid_found.add(hid)

            # 检查是否需要继续分页
            parsed_cnt += len(shareholders)
            if parsed_cnt >= total:
                self.logger.info(f"所有 {total} 条记录已获取完毕")
                break

            page_num += 1
            if page_num > 1000:
                self.logger.warning("页码超过限制，停止爬取")
                break

        # 新发现的 id 继续加入队列
        new_ids = self.filter_new_ids(list(gid_found))
        for cid in new_ids:
            if self.id_collect_queue is not None:
                self.id_collect_queue.put(cid)
            self.add_to_todo(src="tyc", id=cid, entity_type="1")

        return list(gid_found), list(hid_found)
    
    def get_all_investment(self, company_gid, save_to_file=False):
        """
        获取公司的所有对外投资信息（自动处理分页）
        Args:
            company_gid: 公司 ID
            save_to_file: 是否保存到文件
        
        Returns:
            list: 所有投资记录的公司ID列表
        """
        if not self.headers_is_valid:
            self.logger.warning("请求头无效，暂停请求")
            raise Exception("请求头无效，暂停请求")
        
        output_file = os.path.join(self.data_direc, f"investments_{company_gid}.json")
        if os.path.exists(output_file):
            self.logger.info(f"数据文件 {output_file} 已存在，跳过爬取")
            return

        self.logger.info(f"========== 开始爬取公司 {company_gid} 的对外投资信息 ==========")
        
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

            if not investments:
                self.logger.info(f"第 {page_num} 页无投资记录，停止爬取")
                break
                    
            # 可选：保存到文件（仅在本页有记录时写入，避免创建空文件）
            if save_to_file:
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
        
        # 新发现的 id 继续加入队列
        new_ids = self.filter_new_ids(list(id_found))
        for cid in new_ids:
            if self.id_collect_queue is not None:
                self.id_collect_queue.put(cid)
            self.add_to_todo(src="tyc", id=cid, entity_type="1")

        return list(id_found)

    def search_companies(self, keyword, max_page=None, save_to_file=True):
        """
        搜索公司信息（自动处理分页）
        Args:
            keyword: 搜索关键字
            max_page: 最多爬取的页数（None=爬取所有页）
            save_to_file: 是否保存到文件
        
        Returns:
            dict: 搜索结果统计信息
    
        注意：搜索的时候，即使是已经被其他公司的投资或股东所牵连的id，由于本身无credit code等唯一码，在本段代码中，会被新爬的所覆盖。
        这种设计本身也是因为用户如果主动要求搜索，那么用户是很关心这家公司的，所以基础信息也要齐全，而如果一家公司是被其他公司通过关系
        牵连到的，那么用户可能只是想知道这个公司，至于这个公司的其他信息，用户可能并不关心，所以就不需要去爬取了。这种设计也就导致，在
        db文件中，牵连出来的id，也可以记录为已经爬取，后续用户主动搜索的话并不会因为已经爬取而跳过爬取了。总之，搜索的优先级是高于被牵连的。
        """
        if not self.headers_is_valid:
            self.logger.warning("请求头无效，暂停请求")
            raise Exception("请求头无效，暂停请求")
    
        # 如果没有会员，只能看前两页
        if not max_page:
            max_page = 2
        elif max_page > 2:
            max_page = 2
        
        self.logger.info(f"========== 开始搜索关键字: {keyword} ==========")
        
        page_num = 1
        page_size = 20
        failure_count = 0
        company_count = 0
        company_ids = []
        total_pages = 1
        sleep_seconds = self.get_request_sleep_seconds()
        
        while True:
            # 发送请求前等待
            if page_num > 1:
                self.logger.info(f"等待 {sleep_seconds} 秒后发送下一个请求...")
                time.sleep(sleep_seconds)
            
            # 检查是否超过最大页数限制
            if max_page and page_num > max_page:
                self.logger.info(f"已达到最大页数限制 {max_page}，停止爬取")
                break
            
            # 获取当前页数据
            response_data = self.__get_search_page(keyword, page_num, page_size)
            
            if response_data is None:
                self.logger.error(f"获取第 {page_num} 页失败，停止爬取")
                failure_count += 1
                if failure_count >= self.headers_trial_limit:
                    self.logger.error("连续多次请求失败，请求头设置为无效，等待人工干预")
                    self.headers_is_valid = False
                break
            
            # 重置失败计数器
            failure_count = 0
            
            # 提取数据
            page_data = response_data.get("data", {})
            companies = page_data.get("companyList", [])
            total_pages = page_data.get("companyTotalPage", 1)
            
            if not companies:
                self.logger.info(f"第 {page_num} 页无公司数据，停止爬取")
                break
            
            # 解析数据并保存
            page_company_ids = self.__parse_company_data(companies)
            company_count += len(page_company_ids)
            company_ids.extend(page_company_ids)
            
            # 保存到文件
            if save_to_file:
                for company in companies:
                    company_id = company.get("id")
                    if not company_id:
                        continue
                    
                    output_file = os.path.join(self.data_direc, f"base_info_{company_id}.json")
                    
                    # 如果文件已存在，检查是不是存在creditCode
                    if os.path.exists(output_file):
                        with open(output_file, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                        if existing_data.get("creditCode"):
                            self.logger.debug(f"文件 {output_file} 已存在，跳过")
                            continue
                    
                    # 搜索带<em>标签的字段，去掉标签后保存
                    if "name" in company:
                        company["name"] = company["name"].replace("<em>", "").replace("</em>", "")

                    try:
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump(company, f, ensure_ascii=False, indent=2)
                        self.logger.debug(f"已保存: base_info_{company_id}.json")
                        self.write_db(src="tyc", id=company_id, entity_type="1")
                    except Exception as e:
                        self.logger.error(f"保存文件失败: {output_file}, 错误: {e}")
            
            self.logger.info(f"第 {page_num} 页获取 {len(page_company_ids)} 条记录")
            
            # 检查是否需要继续分页
            if page_num >= total_pages:
                self.logger.info(f"所有 {total_pages} 页已获取完毕")
                break
            
            # 下一页
            page_num += 1
            
            # 安全检查：防止无限循环
            if page_num > 1000:
                self.logger.warning("页码超过限制，停止爬取")
                break
        
        result = {
            "keyword": keyword,
            "total_companies": company_count,
            "total_pages": total_pages,
            "company_ids": company_ids,
            "timestamp": datetime.now().isoformat()
        }
        
        self.logger.info(f"搜索完成: 共找到 {company_count} 家公司，跨越 {total_pages} 页")

        # 新发现的 id 继续加入队列
        new_ids = self.filter_new_ids(company_ids)
        for cid in new_ids:
            if self.id_collect_queue is not None:
                self.id_collect_queue.put(cid)
            self.add_to_todo(src="tyc", id=cid, entity_type="1")
        
        return result

    def close_session(self):
        """关闭 Session，释放连接资源"""
        if self.session:
            self.session.close()
            self.logger.info("Session 已关闭，资源已释放")
    
    # 每次重新启动主函数的时候，都会调用 load_db 来加载已经发现的 id 加入到队列中，继续爬取
    def load_db(self):
        # debug等模式下不设置队列，直接返回
        if self.id_collect_queue is None:
            self.logger.warning("ID 收集队列未设置，无法加载数据库")
            return
        
        # 从数据库加载已爬取的 id，加入队列
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM todo WHERE src='tyc' AND entity_type='1'")
        rows = cursor.fetchall()
        for row in rows:
            id = row[0]
            self.id_collect_queue.put(id)
        self.logger.info(f"从数据库加载了 {len(rows)} 个 ID 加入队列")
        conn.close()


# ==================== 使用示例 ====================

def test_investment_crawl(company_gid=None):
    """测试爬取天眼查对外投资信息
    """
    # 示例：爬取视源电子 (gid: 1391758803) 的对外投资信息
    spider = TYCSpider()
      
    if company_gid:
        id_found = [company_gid]
    else:
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


def test_company_search(keywords=None, max_page=None, save_to_file=True, output_file=None):
    """
    测试爬取天眼查公司搜索结果
    
    Args:
        keywords: 搜索关键字列表，默认为 ["CVTE"]
        max_page: 最多爬取的页数
        save_to_file: 是否保存公司数据到文件
        output_file: 搜索结果统计文件
    
    Returns:
        list: 所有搜索结果
    """
    if keywords is None:
        keywords = ["CVTE"]
    
    spider = TYCSpider()
    all_results = []
    
    try:
        for keyword in keywords:
            try:
                result = spider.search_companies(keyword, max_page=max_page, save_to_file=save_to_file)
                all_results.append(result)
                print(f"✓ {keyword}: 共找到 {result['total_companies']} 家公司")
            except Exception as e:
                print(f"✗ {keyword}: 搜索失败 - {e}")
                spider.logger.exception(f"搜索 {keyword} 失败")
    
    finally:
        spider.close_session()
    
    # 输出结果
    print(f"========== 爬取完成 ==========")
    print(f"总共处理 {len(all_results)} 个关键字")
    
    for result in all_results:
        print(f"  {result['keyword']}: {result['total_companies']} 家公司, {result['total_pages']} 页")
    
    # 保存结果到文件
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, ensure_ascii=False, indent=2)
            print(f"\n结果已保存到: {output_file}")
        except Exception as e:
            print(f"保存结果文件失败: {e}")
    
    print("\n爬取完成！")
    print(f"公司数据已保存到: {spider.data_direc}")
    
    return all_results


def test_shareholder_crawl(company_gid=None):
    """测试爬取天眼查股东信息
    """
    # 示例：爬取视源电子 (gid: 1391758803) 的股东信息
    spider = TYCSpider()
      
    if company_gid:
        gid_found = [company_gid]
    else:
        gid_found = ["1391758803"] # 395739442 视源电子 1391758803 视睿电子

    while True:
        for company_gid in gid_found:
            try:
                gid_found, hid_found = spider.get_all_shareholder(company_gid, save_to_file=True)
                spider.logger.info(f"本次爬取完成，发现 {len(gid_found)} 家企业股东，{len(hid_found)} 位自然人股东")
            except Exception as e:
                gid_found = []
                hid_found = []
                spider.close_session()
                break
        
        if len(gid_found) == 0 and len(hid_found) == 0:
            break


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="天眼查爬虫 - 搜索和爬取公司信息",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 搜索单个关键字
  python -m tyc.spider -k "CVTE"
  
  # 搜索多个关键字
  python -m tyc.spider -k "CVTE" -k "百度" -k "阿里"
  
  # 限制爬取页数
  python -m tyc.spider -k "CVTE" -m 5
  
  # 从文件读取关键字（每行一个）
  python -m tyc.spider -f keywords.txt
  
  # 只输出结果，不保存文件
  python -m tyc.spider -k "CVTE" --no-save
  
  # 保存搜索结果统计
  python -m tyc.spider -k "CVTE" -o results.json
  
  # 爬取对外投资（在不搜索的时候启用）
  python -m tyc.spider -t investment -ID 1391758803
  
  # 爬取股东信息（在不搜索的时候启用）
  python -m tyc.spider -t shareholder -ID 1391758803
        """
    )
    
    # 搜索参数
    parser.add_argument(
        "-k", "--keyword",
        action="append",
        dest="keywords",
        help="搜索关键字（可以多次使用）"
    )
    
    parser.add_argument(
        "-f", "--file",
        help="关键字文件路径（每行一个关键字）"
    )
    
    parser.add_argument(
        "-m", "--max-page",
        type=int,
        default=None,
        help="最多爬取的页数（默认爬取所有页）"
    )
    
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="不保存JSON文件"
    )
    
    parser.add_argument(
        "-o", "--output",
        help="输出结果文件（可选）"
    )
    
    # 测试参数
    parser.add_argument(
        "-t", "--test", 
        choices=["investment", "shareholder"], 
        help="运行测试类型"
    )

    parser.add_argument(
        "-ID", "--company-gid",
        type=str,
        default=None,
        help="公司 GID，用于投资爬取测试"
    )
    
    args = parser.parse_args()
    
    # 获取关键字列表
    keywords = []
    
    if args.keywords:
        keywords.extend(args.keywords)
    
    if args.file:
        if not os.path.exists(args.file):
            print(f"错误: 文件 {args.file} 不存在")
            sys.exit(1)
        
        try:
            with open(args.file, 'r', encoding='utf-8') as f:
                file_keywords = [line.strip() for line in f if line.strip()]
                keywords.extend(file_keywords)
        except Exception as e:
            print(f"读取文件失败: {e}")
            sys.exit(1)
    
    if not keywords:
        # 如果没有指定关键字，
        # 如果指定了测试类型为 investment，则运行投资爬取测试
        if args.test == "investment":
            test_investment_crawl(company_gid=args.company_gid)
        elif args.test == "shareholder":
            test_shareholder_crawl(company_gid=args.company_gid)
        else:
            parser.print_help()
        sys.exit(0)
    
    # 去重
    keywords = list(set(keywords))
    
    print(f"\n开始爬取 {len(keywords)} 个关键字的公司信息...")
    print(f"关键字列表: {keywords}\n")

    test_company_search(
        keywords=keywords,
        max_page=args.max_page,
        save_to_file=not args.no_save,
        output_file=args.output
    )
