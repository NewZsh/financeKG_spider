import argparse
from datetime import datetime

from py2neo import Graph, Node, Relationship
import os
import json
import tqdm
import queue
import threading

'''
Neo4j 使用的 tips

1. 规避重复导入
Neo4j 是否会重复导入，取决于你导入数据的方式和 Cypher 语句的写法：

如果你用 Cypher 的 CREATE 语句直接插入节点或关系，每次执行都会新建，不会自动去重，因此会重复导入。
如果你用 MERGE 语句，Neo4j 会根据你指定的唯一属性（如 id、name 等）查找是否已存在，存在则不会重复导入，不存在才会新建。
如果你用 LOAD CSV 或类似批量导入工具，行为同上，取决于你用的是 CREATE 还是 MERGE。
总结：

用 CREATE 会重复导入。
用 MERGE 并指定唯一属性，不会重复导入。
如需避免重复，建议用 MERGE 并为节点/关系设置唯一约束（UNIQUE CONSTRAINT）。

2. 查询语句

查询所有公司节点（Company）：
    MATCH (c:Company) RETURN c LIMIT 20;

查询所有人物节点（Person）：
    MATCH (p:Person) RETURN p LIMIT 20;

查询所有公司之间的投资关系（INVEST）：
    MATCH (a:Company)-[r:INVEST]->(b:Company) RETURN a, r, b LIMIT 20;

查询某个公司及其所有股东（SHAREHOLDER）：
    MATCH (s)-[r:SHAREHOLDER]->(c:Company {id: "公司ID"}) RETURN s, r, c;
请将"公司ID"替换为实际的公司id。

查询某个人或公司作为股东的所有公司：
    MATCH (s {id: "股东ID"})-[r:SHAREHOLDER]->(c:Company) RETURN s, r, c;
请将"股东ID"替换为实际的id。

查询某个公司的多层关系：
    MATCH path=(c:Company {id: 'company_id'})-[:INVEST*1..N]->(related) RETURN path
请将'company_id'替换为实际的公司id，N为你想查询的关系层数。
'''

class Neo4jManager:
    def __init__(self, uri="neo4j://localhost:7687", user="neo4j", password="83939190ys"):
        self.graph = Graph(uri, auth=(user, password))

    def ensure_constraints(self):
        queries = [
            "CREATE CONSTRAINT company_id_unique IF NOT EXISTS FOR (c:Company) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT person_id_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
        ]
        for query in queries:
            self.graph.run(query)
    
    def flush_db(self):
        self.graph.delete_all()

    def add_company(self, company_id, name):
        node = Node("Company", id=str(company_id), name=name)
        self.graph.merge(node, "Company", "id")
        return node

    def add_person(self, person_id, name):
        node = Node("Person", id=str(person_id), name=name)
        self.graph.merge(node, "Person", "id")
        return node
    
    def query_company_id(self, company_id):
        return self.graph.nodes.match("Company", id=str(company_id)).first()

    def query_companies_by_name(self, keywords, limit=20):
        if isinstance(keywords, str):
            keywords = [keywords]
        keywords = [keyword.strip() for keyword in keywords if keyword and keyword.strip()]
        if not keywords:
            return []

        query = '''
        MATCH (c:Company)
        WHERE ANY(keyword IN $keywords WHERE toLower(c.name) CONTAINS toLower(keyword))
        RETURN c.id AS id, c.name AS name
        ORDER BY c.name
        LIMIT $limit
        '''
        return self.graph.run(query, keywords=keywords, limit=limit).data()
    
    def query_person_id(self, person_id):
        return self.graph.nodes.match("Person", id=str(person_id)).first()

    def query_persons_by_name(self, keywords, limit=20):
        if isinstance(keywords, str):
            keywords = [keywords]
        keywords = [keyword.strip() for keyword in keywords if keyword and keyword.strip()]
        if not keywords:
            return []

        query = '''
        MATCH (p:Person)
        WHERE ANY(keyword IN $keywords WHERE toLower(p.name) CONTAINS toLower(keyword))
        RETURN p.id AS id, p.name AS name
        ORDER BY p.name
        LIMIT $limit
        '''
        return self.graph.run(query, keywords=keywords, limit=limit).data()

    def add_investment(self, investor_id, investee_id, percent=None):
        # assume that the investor deem to exist
        investor = self.query_company_id(investor_id)
        investee = self.query_company_id(investee_id)
        if investee:
            # add name properties so that the relationship is more readable in the browser
            rel = Relationship(
                investor,
                "INVEST",
                investee,
                percent=percent,
                investor_name=investor.get("name"),
                investee_name=investee.get("name"),
            )
            self.graph.merge(rel)
        else:
            print(f"investee {investee_id} not found")

    def add_shareholder(self, company_id, shareholder_id, shareholder_type="Company", percent=None):
        # assume that the company deem to exist
        company = self.query_company_id(company_id)
        if shareholder_type == "Company":
            shareholder = self.query_company_id(shareholder_id)
        else:
            shareholder = self.query_person_id(shareholder_id)
        if shareholder:
            rel = Relationship(
                shareholder,
                "SHAREHOLDER",
                company,
                percent=percent,
                shareholder_name=shareholder.get("name"),
                company_name=company.get("name"),
            )
            self.graph.merge(rel)
        else:
            print(f"shareholder {shareholder_id} not found")

    def add_companies_unwind_batch(self, batch_data):
        if not batch_data: return
        query = '''
        UNWIND $batch AS row
        MERGE (c:Company {id: row.id})
        SET c.name = row.name
        '''
        self.graph.run(query, batch=batch_data)

    def add_persons_unwind_batch(self, batch_data):
        if not batch_data: return
        query = '''
        UNWIND $batch AS row
        MERGE (p:Person {id: row.id})
        SET p.name = row.name
        '''
        self.graph.run(query, batch=batch_data)

    def add_investments_unwind_batch(self, batch_data):
        if not batch_data: return
        query = '''
        UNWIND $batch AS row
        MERGE (investor:Company {id: row.investor_id})
        SET investor.name = coalesce(investor.name, row.investor_name)
        MERGE (investee:Company {id: row.investee_id})
        SET investee.name = coalesce(investee.name, row.investee_name)
        MERGE (investor)-[r:INVEST]->(investee)
        SET r.percent = row.percent
        '''
        self.graph.run(query, batch=batch_data)

    def add_shareholders_unwind_batch(self, batch_data):
        if not batch_data: return
        
        person_batch = [b for b in batch_data if b['shareholder_type'] == 1]
        if person_batch:
            q_person = '''
            UNWIND $batch AS row
            MATCH (p:Person {id: row.shareholder_id})
            MATCH (c:Company {id: row.company_id})
            MERGE (p)-[r:SHAREHOLDER]->(c)
            SET r.percent = row.percent
            '''
            self.graph.run(q_person, batch=person_batch)
        
        company_batch = [b for b in batch_data if b['shareholder_type'] != 1]
        if company_batch:
            q_company = '''
            UNWIND $batch AS row
            MATCH (s:Company {id: row.shareholder_id})
            MATCH (c:Company {id: row.company_id})
            MERGE (s)-[r:SHAREHOLDER]->(c)
            SET r.percent = row.percent
            '''
            self.graph.run(q_company, batch=company_batch)

    def get_graph_data(self, limit=100):
        # return a simplified structure with names alongside the raw nodes/relationships
        query = """
        MATCH (a)-[r]->(b)
        RETURN a.id AS a_id, a.name AS a_name,
               r AS relationship,
               b.id AS b_id, b.name AS b_name
        LIMIT $limit
        """
        return self.graph.run(query, limit=limit).data()

## 读取 data/tyc_data 下的 base_info_{id}.json \ investments_{id}.json \ shareholders_{id}.json 文件，写入 Neo4j 图数据库

class DataImporter:
    def __init__(self, neo4j_manager, data_dir="data/tyc_data"):
        self.neo4j_manager = neo4j_manager
        self.data_dir = data_dir
        self.state_file = os.path.join(self.data_dir, ".import_state.json")
        self.batch_size = 5000
        self.queue_maxsize = 1

    def _format_timestamp(self, timestamp):
        if timestamp is None:
            return "未记录"
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    def _load_import_state(self):
        if not os.path.exists(self.state_file):
            return {}
        try:
            with open(self.state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _get_last_import_time(self):
        state = self._load_import_state()
        timestamp = state.get("last_import_time")
        if isinstance(timestamp, (int, float)):
            return float(timestamp)
        return None

    def _save_import_state(self, timestamp):
        state = {
            "last_import_time": timestamp,
            "last_import_time_readable": self._format_timestamp(timestamp),
        }
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    def reset_import_state(self):
        if os.path.exists(self.state_file):
            os.remove(self.state_file)

    def _collect_files(self, prefix, last_import_time=None, limit=None):
        files = []
        for file_name in os.listdir(self.data_dir):
            if not (file_name.startswith(prefix) and file_name.endswith(".json")):
                continue
            file_path = os.path.join(self.data_dir, file_name)
            try:
                modified_time = os.path.getmtime(file_path)
            except OSError:
                continue
            if last_import_time is not None and modified_time <= last_import_time:
                continue
            files.append((file_name, modified_time))

        files.sort(key=lambda item: (item[1], item[0]))
        if limit is not None:
            files = files[:limit]
        return files

    def _prepare_import_files(self, incremental=True):
        last_import_time = self._get_last_import_time() if incremental else None
        base_info_files = self._collect_files("base_info_", last_import_time=last_import_time)
        investment_files = self._collect_files("investments_", last_import_time=last_import_time)
        shareholder_files = self._collect_files("shareholders_", last_import_time=last_import_time)

        selected_files = base_info_files + investment_files + shareholder_files
        latest_import_time = max((modified_time for _, modified_time in selected_files), default=last_import_time)

        return {
            "last_import_time": last_import_time,
            "base_info_files": [file_name for file_name, _ in base_info_files],
            "investment_files": [file_name for file_name, _ in investment_files],
            "shareholder_files": [file_name for file_name, _ in shareholder_files],
            "latest_import_time": latest_import_time,
        }

    def _iter_json_lines(self, file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                for line in f:
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")

    def _queue_batch(self, q, batch):
        if len(batch) >= self.batch_size:
            q.put(batch)
            return []
        return batch

    def _append_unique_node(self, batch, seen_ids, node_id, name, q):
        if not node_id or not name or node_id in seen_ids:
            return batch
        seen_ids.add(node_id)
        batch.append({"id": node_id, "name": name})
        return self._queue_batch(q, batch)

    def _extract_investment_fields(self, inv):
        investee_id = str(inv.get("id")) if inv.get("id") is not None else None
        if investee_id is None:
            tags = inv.get("tags") or []
            if tags:
                company_id = tags[0].get("companyId")
                investee_id = str(company_id) if company_id is not None else None
        return investee_id, inv.get("name"), inv.get("totalPercent")

    def _extract_shareholder_fields(self, sh):
        shareholder_type = sh.get("shareHolderType")
        if shareholder_type == 1:
            shareholder_id = str(sh.get("shareHolderPid")) if sh.get("shareHolderPid") is not None else None
        else:
            shareholder_id = str(sh.get("shareHolderNameId")) if sh.get("shareHolderNameId") is not None else None
        return shareholder_type, shareholder_id, sh.get("shareHolderName", ""), sh.get("percent")

    def _writer_worker(self, q, write_method):
        while True:
            batch = q.get()
            if batch is None:
                q.task_done()
                break
            write_method(batch)
            q.task_done()

    def _flush_queue(self, q, current_batch):
        if current_batch:
            q.put(current_batch)
        q.put(None)
        q.join()

    def import_all(self, incremental=True):
        '''
        分三步导入数据：
        1. 导入节点：先从 base_info 导入公司节点，再从 investments 和 shareholders 导入公司和自然人节点
        2. 导入投资关系：从 investments 导入公司之间的投资关系
        3. 导入股东关系：从 shareholders 导入公司与股东（公司或自然人）之间的股东关系

        队列的工作方式：当主线程积累到 batch_size 条数据时放入队列，写线程从队列中取出数据并调用 Neo4jManager 的批量写入方法。
            这样可以让主线程在写线程处理当前批次时继续读取并积累下一批数据；如果写线程跟不上，主线程会在下一次 put 时阻塞，避免内存里堆积过多待写批次。
        '''
        import_plan = self._prepare_import_files(incremental=incremental)
        base_info_files = import_plan["base_info_files"]
        investment_files = import_plan["investment_files"]
        shareholder_files = import_plan["shareholder_files"]

        total_files = len(base_info_files) + len(investment_files) + len(shareholder_files)
        if incremental:
            print(f"上次最后导入时间: {self._format_timestamp(import_plan['last_import_time'])}")
        if total_files == 0:
            print("没有新的文件需要导入。")
            return False

        print(
            "本次待导入文件: "
            f"base_info={len(base_info_files)}, "
            f"investments={len(investment_files)}, "
            f"shareholders={len(shareholder_files)}"
        )

        # --- 第一步：并发导入公司和自然人节点 ---
        q_co = queue.Queue(maxsize=self.queue_maxsize)
        q_per = queue.Queue(maxsize=self.queue_maxsize)
        
        t_co = threading.Thread(target=self._writer_worker, args=(q_co, self.neo4j_manager.add_companies_unwind_batch))
        t_per = threading.Thread(target=self._writer_worker, args=(q_per, self.neo4j_manager.add_persons_unwind_batch))
        
        t_co.start()
        t_per.start()

        co_batch = []
        per_batch = []
        seen_company_ids = set()
        seen_person_ids = set()

        # 1.1 从 base_info 导入公司
        for file in tqdm.tqdm(base_info_files, desc="导入公司信息"):
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                data = json.load(f)
                company_id = str(data.get("id") or data.get("company_id")) if (data.get("id") or data.get("company_id")) is not None else None
                if not company_id:
                    company_id = file.rsplit("_", 1)[1].split(".", 1)[0]
                name = data.get("name")
                co_batch = self._append_unique_node(co_batch, seen_company_ids, company_id, name, q_co)

        # 1.2 从 shareholders 导入公司和人物
        for file in tqdm.tqdm(shareholder_files, desc="导入股东关系中的公司和人物"):
            file_path = os.path.join(self.data_dir, file)
            for sh in self._iter_json_lines(file_path):
                shareholder_type, shareholder_id, shareholder_name, _ = self._extract_shareholder_fields(sh)
                if shareholder_type == 1:
                    per_batch = self._append_unique_node(per_batch, seen_person_ids, shareholder_id, shareholder_name, q_per)
                else:
                    co_batch = self._append_unique_node(co_batch, seen_company_ids, shareholder_id, shareholder_name, q_co)

        # 等待所有节点导入完成
        self._flush_queue(q_co, co_batch)
        self._flush_queue(q_per, per_batch)
        t_co.join()
        t_per.join()

        # --- 第二步：并发导入投资关系 ---
        q_inv = queue.Queue(maxsize=self.queue_maxsize)
        t_inv = threading.Thread(target=self._writer_worker, args=(q_inv, self.neo4j_manager.add_investments_unwind_batch))
        t_inv.start()
        
        inv_batch = []
        for file in tqdm.tqdm(investment_files, desc="导入投资关系"):
            investor_id = file.split("_", 1)[1].split(".", 1)[0]
            file_path = os.path.join(self.data_dir, file)
            for inv in self._iter_json_lines(file_path):
                investee_id, investee_name, percent = self._extract_investment_fields(inv)
                if investor_id and investee_id:
                    inv_batch.append({
                        "investor_id": investor_id,
                        "investee_id": investee_id,
                        "investee_name": investee_name,
                        "investor_name": None,
                        "percent": percent
                    })
                    inv_batch = self._queue_batch(q_inv, inv_batch)
        
        self._flush_queue(q_inv, inv_batch)
        t_inv.join()

        # --- 第三步：并发导入股东关系 ---
        q_sh = queue.Queue(maxsize=self.queue_maxsize)
        t_sh = threading.Thread(target=self._writer_worker, args=(q_sh, self.neo4j_manager.add_shareholders_unwind_batch))
        t_sh.start()

        sh_batch = []
        for file in tqdm.tqdm(shareholder_files, desc="导入股东关系"):
            company_id = file.split("_", 1)[1].split(".", 1)[0]
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                try:
                    for line in f:
                        try:
                            sh = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        shareholder_type = sh.get("shareHolderType")
                        if shareholder_type == 1:
                            shareholder_id = str(sh.get("shareHolderPid")) if sh.get("shareHolderPid") is not None else None
                        else:
                            shareholder_id = str(sh.get("shareHolderNameId")) if sh.get("shareHolderNameId") is not None else None
                        percent = sh.get("percent")
                        
                        if company_id and shareholder_id:
                            sh_batch.append({
                                "company_id": company_id,
                                "shareholder_id": shareholder_id,
                                "shareholder_type": shareholder_type,
                                "percent": percent
                            })
                            if len(sh_batch) >= self.batch_size:
                                q_sh.put(sh_batch)
                                sh_batch = []
                except Exception as e:
                    print(f"Error reading file {file_path}: {e}")

        self._flush_queue(q_sh, sh_batch)
        t_sh.join()

        self._save_import_state(import_plan["latest_import_time"])
        print(f"已更新最后导入时间: {self._format_timestamp(import_plan['latest_import_time'])}")
        return True

    def import_all_one_by_one(self, incremental=True):
        '''
        单线程逐条导入版本，用于和批量导入的速度做对比。
        导入顺序与 import_all 保持一致，只是不再使用队列和批量写入。
        '''
        import_plan = self._prepare_import_files(incremental=incremental)
        base_info_files = import_plan["base_info_files"]
        investment_files = import_plan["investment_files"]
        shareholder_files = import_plan["shareholder_files"]

        total_files = len(base_info_files) + len(investment_files) + len(shareholder_files)
        if incremental:
            print(f"上次最后导入时间: {self._format_timestamp(import_plan['last_import_time'])}")
        if total_files == 0:
            print("没有新的文件需要导入。")
            return False

        print(
            "本次待导入文件: "
            f"base_info={len(base_info_files)}, "
            f"investments={len(investment_files)}, "
            f"shareholders={len(shareholder_files)}"
        )

        # 1.1 从 base_info 导入公司
        for file in tqdm.tqdm(base_info_files, desc="逐条导入公司信息"):
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                data = json.load(f)
                company_id = str(data.get("id") or data.get("company_id")) if (data.get("id") or data.get("company_id")) is not None else None
                if not company_id:
                    company_id = file.rsplit("_", 1)[1].split(".", 1)[0]
                name = data.get("name")
                if company_id and name:
                    self.neo4j_manager.add_company(company_id, name)

        # 1.2 从 shareholders 导入公司和人物
        for file in tqdm.tqdm(shareholder_files, desc="逐条导入股东关系中的公司和人物"):
            file_path = os.path.join(self.data_dir, file)
            for sh in self._iter_json_lines(file_path):
                shareholder_type, shareholder_id, shareholder_name, _ = self._extract_shareholder_fields(sh)
                if shareholder_id and shareholder_name:
                    if shareholder_type == 1:
                        self.neo4j_manager.add_person(shareholder_id, shareholder_name)
                    else:
                        self.neo4j_manager.add_company(shareholder_id, shareholder_name)

        # 2. 从 investments 导入投资关系
        for file in tqdm.tqdm(investment_files, desc="逐条导入投资关系"):
            investor_id = file.split("_", 1)[1].split(".", 1)[0]
            file_path = os.path.join(self.data_dir, file)
            for inv in self._iter_json_lines(file_path):
                investee_id, investee_name, percent = self._extract_investment_fields(inv)
                if investee_id and investee_name:
                    self.neo4j_manager.add_company(investee_id, investee_name)
                if investor_id and investee_id:
                    self.neo4j_manager.add_investment(investor_id, investee_id, percent)

        # 3. 从 shareholders 导入股东关系
        for file in tqdm.tqdm(shareholder_files, desc="逐条导入股东关系"):
            company_id = file.split("_", 1)[1].split(".", 1)[0]
            file_path = os.path.join(self.data_dir, file)
            for sh in self._iter_json_lines(file_path):
                shareholder_type, shareholder_id, _, percent = self._extract_shareholder_fields(sh)
                shareholder_type_name = "Person" if shareholder_type == 1 else "Company"

                if company_id and shareholder_id:
                    self.neo4j_manager.add_shareholder(
                        company_id,
                        shareholder_id,
                        shareholder_type=shareholder_type_name,
                        percent=percent,
                    )

        self._save_import_state(import_plan["latest_import_time"])
        print(f"已更新最后导入时间: {self._format_timestamp(import_plan['latest_import_time'])}")
        return True

if __name__ == "__main__":
    """
    主函数：批量导入 data/tyc_data 下的数据到 Neo4j。
    用法：
        python neo4j_utils.py
    """
    parser = argparse.ArgumentParser(description="导入 data/tyc_data 下的数据到 Neo4j")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="清空数据库并重置最后导入时间，然后全量重导",
    )
    parser.add_argument(
        "--one-by-one",
        action="store_true",
        help="使用单线程逐条导入模式",
    )
    args = parser.parse_args()

    data_dir = os.path.join(os.path.dirname(__file__), 'data/tyc_data')
    print("开始导入数据到 Neo4j...")
    neo4j_manager = Neo4jManager()
    neo4j_manager.ensure_constraints()
    
    importer = DataImporter(neo4j_manager, data_dir=data_dir)
    if args.reset:
        print("检测到 --reset，先清空数据库并重置最后导入时间。")
        neo4j_manager.flush_db()
        importer.reset_import_state()

    if args.one_by_one:
        imported = importer.import_all_one_by_one(incremental=not args.reset)
    else:
        imported = importer.import_all(incremental=not args.reset)

    if imported:
        print("数据导入完成！")
    print("\n【如何在网页端查看数据】")
    print("1. 启动 Neo4j Desktop 或 Neo4j 服务，确保数据库已运行。")
    print("2. 在浏览器访问：http://localhost:7474/")
    print("3. 默认用户名/密码 neo4j/neo4j（首次登录需修改密码）。")
    print("4. 登录后可在 Cypher 控制台执行如 MATCH (n)-[r]->(m) RETURN n,r,m LIMIT 50; 查看图谱。")


