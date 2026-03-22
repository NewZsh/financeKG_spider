import os
import re

file_path = r"d:\work\financeKG_spider\neo4j_utils.py"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# Insert queue and threading imports
content = content.replace("import tqdm\n", "import tqdm\nimport queue\nimport threading\n")

# Setup the new batch methods in Neo4jManager
manager_batch_methods = """
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

    def get_graph_data(self, limit=100):"""

content = content.replace("    def get_graph_data(self, limit=100):", manager_batch_methods)


# Rewrite DataImporter
new_data_importer = """class DataImporter:
    def __init__(self, neo4j_manager, data_dir="data/tyc_data"):
        self.neo4j_manager = neo4j_manager
        self.data_dir = data_dir
        self.batch_size = 5000

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
        base_info_files = [f for f in os.listdir(self.data_dir) if f.startswith("base_info_") and f.endswith(".json")]
        investment_files = [f for f in os.listdir(self.data_dir) if f.startswith("investments_") and f.endswith(".json")]
        shareholder_files = [f for f in os.listdir(self.data_dir) if f.startswith("shareholders_") and f.endswith(".json")][:1000]

        # --- 第一步：并发导入公司和自然人节点 ---
        q_co = queue.Queue(maxsize=2)
        q_per = queue.Queue(maxsize=2)
        
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
                    company_id = file.split("_", 1)[1].split(".", 1)[0]
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
        q_inv = queue.Queue(maxsize=2)
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
        q_sh = queue.Queue(maxsize=2)
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

if __name__ =="""

content = re.sub(r'class DataImporter:.*?if __name__ ==', new_data_importer, content, flags=re.DOTALL)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("Migration script completed!")
