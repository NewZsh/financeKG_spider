# neo4j_utils.py
from py2neo import Graph, Node, Relationship

class Neo4jManager:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="neo4j"):
        self.graph = Graph(uri, auth=(user, password))

    def add_company(self, company_id, name):
        node = Node("Company", id=company_id, name=name)
        self.graph.merge(node, "Company", "id")
        return node

    def add_person(self, person_id, name):
        node = Node("Person", id=person_id, name=name)
        self.graph.merge(node, "Person", "id")
        return node

    def add_investment(self, investor_id, investee_id):
        investor = self.graph.nodes.match("Company", id=investor_id).first()
        investee = self.graph.nodes.match("Company", id=investee_id).first()
        if investor and investee:
            rel = Relationship(investor, "INVEST", investee)
            self.graph.merge(rel)

    def add_shareholder(self, company_id, shareholder_id, shareholder_type="Company"):
        company = self.graph.nodes.match("Company", id=company_id).first()
        if shareholder_type == "Company":
            shareholder = self.graph.nodes.match("Company", id=shareholder_id).first()
        else:
            shareholder = self.graph.nodes.match("Person", id=shareholder_id).first()
        if company and shareholder:
            rel = Relationship(shareholder, "SHAREHOLDER", company)
            self.graph.merge(rel)

    def get_graph_data(self, limit=100):
        query = """
        MATCH (a)-[r]->(b)
        RETURN a, r, b
        LIMIT $limit
        """
        return self.graph.run(query, limit=limit).data()
