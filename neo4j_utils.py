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
        MATCH (investor:Company {id: row.investor_id})
        MATCH (investee:Company {id: row.investee_id})
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
        self.batch_size = 5000
        self.queue_maxsize = 1

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

    def import_all(self):
        '''
        分三步导入数据：
        1. 导入节点：先从 base_info 导入公司节点，再从 investments 和 shareholders 导入公司和自然人节点
        2. 导入投资关系：从 investments 导入公司之间的投资关系
        3. 导入股东关系：从 shareholders 导入公司与股东（公司或自然人）之间的股东关系

        队列的工作方式：当主线程积累到 batch_size 条数据时放入队列，写线程从队列中取出数据并调用 Neo4jManager 的批量写入方法。
            这样可以让主线程在写线程处理当前批次时继续读取并积累下一批数据；如果写线程跟不上，主线程会在下一次 put 时阻塞，避免内存里堆积过多待写批次。
        '''
        base_info_files = [f for f in os.listdir(self.data_dir) if f.startswith("base_info_") and f.endswith(".json")]
        investment_files = [f for f in os.listdir(self.data_dir) if f.startswith("investments_") and f.endswith(".json")]
        shareholder_files = [f for f in os.listdir(self.data_dir) if f.startswith("shareholders_") and f.endswith(".json")][:1000]

        # --- 第一步：并发导5入公司和自然人节点 ---
        q_co = queue.Queue(maxsize=self.queue_maxsize)
        q_per = queue.Queue(maxsize=self.queue_maxsize)
        
        t_co = threading.Thread(target=self._writer_worker, args=(q_co, self.neo4j_manager.add_companies_unwind_batch))
        t_per = threading.Thread(target=self._writer_worker, args=(q_per, self.neo4j_manager.add_persons_unwind_batch))
        
        t_co.start()
        t_per.start()

        co_batch = []
        per_batch = []

        # 1.1 从 base_info 导入公司
        for file in tqdm.tqdm(base_info_files, desc="导入公司信息"):
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                data = json.load(f)
                company_id = str(data.get("id") or data.get("company_id")) if (data.get("id") or data.get("company_id")) is not None else None
                if not company_id:
                    company_id = file.rsplit("_", 1)[1].split(".", 1)[0]
                name = data.get("name")
                if company_id and name:
                    co_batch.append({"id": company_id, "name": name})
                    if len(co_batch) >= self.batch_size:
                        q_co.put(co_batch)
                        co_batch = []

        # 1.2 从 investments 导入公司
        for file in tqdm.tqdm(investment_files, desc="导入投资关系中的公司"):
            try:
                with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            inv = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        investee_id = str(inv.get("id")) if inv.get("id") is not None else None
                        if investee_id is None:
                            tags = inv.get("tags", [{}])
                            if len(tags) > 0:
                                investee_id = str(tags[0].get("companyId")) if tags[0].get("companyId") is not None else None
                        investee_name = inv.get("name")
                        if investee_id and investee_name:
                            co_batch.append({"id": investee_id, "name": investee_name})
                            if len(co_batch) >= self.batch_size:
                                q_co.put(co_batch)
                                co_batch = []
            except Exception as e:
                pass

        # 1.3 从 shareholders 导入公司和人物
        for file in tqdm.tqdm(shareholder_files, desc="导入股东关系中的公司和人物"):
            try:
                with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
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
                            
                        shareholder_name = sh.get("shareHolderName", "")
                        if shareholder_id and shareholder_name:
                            if shareholder_type == 1:
                                per_batch.append({"id": shareholder_id, "name": shareholder_name})
                                if len(per_batch) >= self.batch_size:
                                    q_per.put(per_batch)
                                    per_batch = []
                            else:
                                co_batch.append({"id": shareholder_id, "name": shareholder_name})
                                if len(co_batch) >= self.batch_size:
                                    q_co.put(co_batch)
                                    co_batch = []
            except Exception as e:
                pass

        # 刷新队列，等待所有节点导入完成
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
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        inv = json.loads(line)
                    except:
                        continue
                    investee_id = str(inv.get("id")) if inv.get("id") is not None else None
                    if investee_id is None:
                        tags = inv.get("tags", [{}])
                        if len(tags) > 0:
                            investee_id = str(tags[0].get("companyId")) if tags[0].get("companyId") is not None else None
                    percent = inv.get("totalPercent")
                    
                    if investor_id and investee_id:
                        inv_batch.append({
                            "investor_id": investor_id,
                            "investee_id": investee_id,
                            "percent": percent
                        })
                        if len(inv_batch) >= self.batch_size:
                            q_inv.put(inv_batch)
                            inv_batch = []
        
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
                for line in f:
                    try:
                        sh = json.loads(line)
                    except:
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

        self._flush_queue(q_sh, sh_batch)
        t_sh.join()

    def import_all_one_by_one(self):
        '''
        单线程逐条导入版本，用于和批量导入的速度做对比。
        导入顺序与 import_all 保持一致，只是不再使用队列和批量写入。
        '''
        base_info_files = [f for f in os.listdir(self.data_dir) if f.startswith("base_info_") and f.endswith(".json")]
        investment_files = [f for f in os.listdir(self.data_dir) if f.startswith("investments_") and f.endswith(".json")]
        shareholder_files = [f for f in os.listdir(self.data_dir) if f.startswith("shareholders_") and f.endswith(".json")][:1000]

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

        # 1.2 从 investments 导入公司
        for file in tqdm.tqdm(investment_files, desc="逐条导入投资关系中的公司"):
            try:
                with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            inv = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        investee_id = str(inv.get("id")) if inv.get("id") is not None else None
                        if investee_id is None:
                            tags = inv.get("tags", [{}])
                            if len(tags) > 0:
                                investee_id = str(tags[0].get("companyId")) if tags[0].get("companyId") is not None else None
                        investee_name = inv.get("name")
                        if investee_id and investee_name:
                            self.neo4j_manager.add_company(investee_id, investee_name)
            except Exception:
                pass

        # 1.3 从 shareholders 导入公司和人物
        for file in tqdm.tqdm(shareholder_files, desc="逐条导入股东关系中的公司和人物"):
            try:
                with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
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

                        shareholder_name = sh.get("shareHolderName", "")
                        if shareholder_id and shareholder_name:
                            if shareholder_type == 1:
                                self.neo4j_manager.add_person(shareholder_id, shareholder_name)
                            else:
                                self.neo4j_manager.add_company(shareholder_id, shareholder_name)
            except Exception:
                pass

        # 2. 从 investments 导入投资关系
        for file in tqdm.tqdm(investment_files, desc="逐条导入投资关系"):
            investor_id = file.split("_", 1)[1].split(".", 1)[0]
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        inv = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    investee_id = str(inv.get("id")) if inv.get("id") is not None else None
                    if investee_id is None:
                        tags = inv.get("tags", [{}])
                        if len(tags) > 0:
                            investee_id = str(tags[0].get("companyId")) if tags[0].get("companyId") is not None else None
                    percent = inv.get("totalPercent")

                    if investor_id and investee_id:
                        self.neo4j_manager.add_investment(investor_id, investee_id, percent)

        # 3. 从 shareholders 导入股东关系
        for file in tqdm.tqdm(shareholder_files, desc="逐条导入股东关系"):
            company_id = file.split("_", 1)[1].split(".", 1)[0]
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        sh = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    shareholder_type = sh.get("shareHolderType")
                    if shareholder_type == 1:
                        shareholder_id = str(sh.get("shareHolderPid")) if sh.get("shareHolderPid") is not None else None
                        shareholder_type_name = "Person"
                    else:
                        shareholder_id = str(sh.get("shareHolderNameId")) if sh.get("shareHolderNameId") is not None else None
                        shareholder_type_name = "Company"
                    percent = sh.get("percent")

                    if company_id and shareholder_id:
                        self.neo4j_manager.add_shareholder(
                            company_id,
                            shareholder_id,
                            shareholder_type=shareholder_type_name,
                            percent=percent,
                        )

if __name__ == "__main__":
    """
    主函数：批量导入 data/tyc_data 下的数据到 Neo4j。
    用法：
        python neo4j_utils.py
    """
    data_dir = os.path.join(os.path.dirname(__file__), 'data/tyc_data')
    print("开始导入数据到 Neo4j...")
    neo4j_manager = Neo4jManager()
    neo4j_manager.flush_db()
    
    importer = DataImporter(neo4j_manager, data_dir=data_dir)
    # importer.import_all()
    importer.import_all_one_by_one()  # 逐条导入版本，测试用
    print("数据导入完成！")
    print("\n【如何在网页端查看数据】")
    print("1. 启动 Neo4j Desktop 或 Neo4j 服务，确保数据库已运行。")
    print("2. 在浏览器访问：http://localhost:7474/")
    print("3. 默认用户名/密码 neo4j/neo4j（首次登录需修改密码）。")
    print("4. 登录后可在 Cypher 控制台执行如 MATCH (n)-[r]->(m) RETURN n,r,m LIMIT 50; 查看图谱。")


