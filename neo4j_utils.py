from py2neo import Graph, Node, Relationship
import os
import json
import tqdm

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

    def add_investment(self, investor_id, investee_id, percent=None):
        # lookup the company nodes by id so we can attach their names to the relationship
        investor = self.graph.nodes.match("Company", id=str(investor_id)).first()
        investee = self.graph.nodes.match("Company", id=str(investee_id)).first()
        if investor and investee:
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
            if not investor:
                print(f"investor {investor_id} not found")
            if not investee:
                print(f"investee {investee_id} not found")

    def add_shareholder(self, company_id, shareholder_id, shareholder_type="Company", percent=None):
        # resolve nodes so that we can include their names on the edge
        company = self.graph.nodes.match("Company", id=str(company_id)).first()
        if shareholder_type == "Company":
            shareholder = self.graph.nodes.match("Company", id=str(shareholder_id)).first()
        else:
            shareholder = self.graph.nodes.match("Person", id=str(shareholder_id)).first()
        if company and shareholder:
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
            if not company:
                print(f"company {company_id} not found")
            if not shareholder:
                print(f"shareholder {shareholder_id} not found")

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

    def import_all(self):
        base_info_files = [f for f in os.listdir(self.data_dir) if f.startswith("base_info_") and f.endswith(".json")]
        investment_files = [f for f in os.listdir(self.data_dir) if f.startswith("investments_") and f.endswith(".json")]
        shareholder_files = [f for f in os.listdir(self.data_dir) if f.startswith("shareholders_") and f.endswith(".json")]

        # 1. 读取所有 base_info_*.json 文件，导入公司和人物节点
        for file in tqdm.tqdm(base_info_files, desc="导入公司信息"):
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                data = json.load(f)
                # 公司节点
                company_id = str(data.get("id") or data.get("company_id")) if (data.get("id") or data.get("company_id")) is not None else None
                name = data.get("name")
                if company_id and name:
                    self.neo4j_manager.add_company(company_id, name)
        
        # 1.1 先把所有公司导入，再导入投资关系和股东关系
        for file in tqdm.tqdm(investment_files, desc="导入投资关系中的公司"):
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                for line in f:
                    inv = json.loads(line)

                    id1 = str(inv.get("id")) if inv.get("id") is not None else None
                    tags = inv.get("tags", [{}])
                    if len(tags) == 0:
                        # print(f"invalid tags in file {file}") # 冗余检查，理解数据所用。事实上有可能是HK公司，天眼查是有数据的，但是在对外投资关系中天眼查未将其基础数据作为 tags 返回
                        tag = {}
                    else:
                        tag = tags[0]
                    if "companyId" in tag:
                        id2 = str(tag.get("companyId")) if tag.get("companyId") is not None else None
                        if id1 != id2:
                            # 确实有发现不一致，如data/tyc_data/investments_2350756110.json  line251, id:null, companyId:5508484910
                            print(f"warning: id {id1} != companyId {id2} in file {file}, using companyId") 
                        if id1 and id2 and id1 != id2:
                            raise ValueError(f"invalid data: id {id1} id2 {id2} in file {file}") # 冗余检查，没进入过这一行，说明 id 和 companyId 至少有一个是空的，或者两者相等

                    investee_id = str(inv.get("id")) if inv.get("id") is not None else None
                    if investee_id is None:
                        tags = inv.get("tags", [{}])
                        if len(tags) > 0:
                            tag = tags[0]
                            investee_id = str(tag.get("companyId")) if tag.get("companyId") is not None else None
                    investee_name = inv.get("name")
                    if investee_id and investee_name:
                        self.neo4j_manager.add_company(investee_id, investee_name)
                    else:
                        print(f"invalid data: id {investee_id} name {investee_name} in file {file}") # 冗余检查，没进入过这一行，说明 id 和 name 都齐全
        for file in tqdm.tqdm(shareholder_files, desc="导入股东关系中的公司和人物"):
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                for line in f:
                    sh = json.loads(line)
                    shareholder_type = sh.get("shareHolderType")
                    if shareholder_type == 1: # 自然人股东
                        shareholder_id = str(sh.get("shareHolderPid")) if sh.get("shareHolderPid") is not None else None
                    else: # 企业股东/其他
                        shareholder_id = str(sh.get("shareHolderNameId")) if sh.get("shareHolderNameId") is not None else None
                        
                    shareholder_name = sh.get("shareHolderName", "")
                    if shareholder_id and shareholder_name:
                        if shareholder_type == 1:
                            self.neo4j_manager.add_person(shareholder_id, shareholder_name)
                        else:
                            self.neo4j_manager.add_company(shareholder_id, shareholder_name)                            
                    else:
                        print(f"invalid data: id {shareholder_id} name {shareholder_name} in file {file}")

        # 2. 读取所有 investments_*.json 文件，导入投资关系
        for file in tqdm.tqdm(investment_files, desc="导入投资关系"):
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                investor_id = file.split("_", 1)[1].split(".", 1)[0] # 从文件名中提取投资方ID
                for line in f:
                    inv = json.loads(line)
                    investee_id = str(inv.get("id")) if inv.get("id") is not None else None
                    if investee_id is None:
                        tags = inv.get("tags", [{}])
                        if len(tags) > 0:
                            tag = tags[0]
                            investee_id = str(tag.get("companyId")) if tag.get("companyId") is not None else None
                    investee_name = inv.get("name")
                    percent = inv.get("totalPercent")
                    if investor_id and investee_id:
                        self.neo4j_manager.add_investment(investor_id, investee_id, percent)

        # 3. 读取所有 shareholders_*.json 文件，导入股东关系
        for file in tqdm.tqdm(shareholder_files, desc="导入股东关系"):
            with open(os.path.join(self.data_dir, file), "r", encoding="utf-8") as f:
                company_id = file.split("_", 1)[1].split(".", 1)[0]  # 从文件名中提取公司ID
                for line in f:
                    sh = json.loads(line)
                    shareholder_type = sh.get("shareHolderType")
                    if shareholder_type == 1: # 自然人股东
                        shareholder_id = str(sh.get("shareHolderPid")) if sh.get("shareHolderPid") is not None else None
                    else: # 企业股东/其他
                        shareholder_id = str(sh.get("shareHolderNameId")) if sh.get("shareHolderNameId") is not None else None
                    percent = sh.get("percent")
                    shareholder_name = sh.get("shareHolderName", "")
                    if shareholder_id and shareholder_name:
                        self.neo4j_manager.add_shareholder(company_id, shareholder_id, shareholder_type, percent)


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
    importer.import_all()
    print("数据导入完成！")
    print("\n【如何在网页端查看数据】")
    print("1. 启动 Neo4j Desktop 或 Neo4j 服务，确保数据库已运行。")
    print("2. 在浏览器访问：http://localhost:7474/")
    print("3. 默认用户名/密码 neo4j/neo4j（首次登录需修改密码）。")
    print("4. 登录后可在 Cypher 控制台执行如 MATCH (n)-[r]->(m) RETURN n,r,m LIMIT 50; 查看图谱。")


