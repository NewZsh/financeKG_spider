from fastapi import APIRouter
from pydantic import BaseModel
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from neo4j_utils import Neo4jManager

router = APIRouter()
neo4j_mgr = Neo4jManager()  # Assuming default localhost configuration

def format_neo4j_data_to_graph(records):
    nodes_map = {}
    edges = []
    
    for record in records:
        for node in record["nodes"]:
            node_id = dict(node).get("id")
            if node_id not in nodes_map:
                labels = list(node.labels)
                nodes_map[node_id] = {
                    "id": str(node_id),
                    "label": dict(node).get("name", str(node_id)),
                    "type": labels[0] if labels else "Unknown",
                    "properties": dict(node)
                }
        
        for rel in record["links"]:
            edges.append({
                "source": str(dict(rel.start_node).get("id")),
                "target": str(dict(rel.end_node).get("id")),
                "label": type(rel).__name__,
                "properties": dict(rel)
            })
            
    # Deduplicate edges based on source, target and label
    unique_edges = []
    seen_edges = set()
    for e in edges:
        edge_id = f"{e['source']}-{e['label']}-{e['target']}"
        if edge_id not in seen_edges:
            seen_edges.add(edge_id)
            unique_edges.append(e)
            
    return {
        "nodes": list(nodes_map.values()),
        "edges": unique_edges
    }

@router.get("/search")
def search_companies(keyword: str):
    query = """
    MATCH (c:Company)
    WHERE c.name CONTAINS $keyword
    RETURN c.id AS id, c.name AS name LIMIT 10
    """
    res = neo4j_mgr.graph.run(query, keyword=keyword).data()
    return {"results": res}

@router.get("/company/{company_id}/graph")
def get_company_graph(company_id: str, hops: int = 2):
    query = """
    MATCH path = (c {id: $company_id})-[*1..2]-(m)
    WITH nodes(path) AS ns, relationships(path) AS rs
    UNWIND ns AS n
    UNWIND rs AS r
    RETURN collect(distinct n) AS nodes, collect(distinct r) AS links
    """
    res = neo4j_mgr.graph.run(query, company_id=company_id).data()
    return format_neo4j_data_to_graph(res) 

@router.get("/examples")
def get_example_companies():
    query = "MATCH (c:Company) RETURN c.id AS id, c.name AS name LIMIT 10"
    res = neo4j_mgr.graph.run(query).data()
    return {"examples": res}
