from fastapi import APIRouter, HTTPException, Query
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
from neo4j_utils import Neo4jManager
from ..services.stock_directory import normalize_company_name, stock_directory_service

router = APIRouter()
neo4j_mgr = Neo4jManager()  # Assuming default localhost configuration


def find_company_by_stock(stock_info):
    candidate_names = []
    for value in [stock_info.get("full_name"), stock_info.get("name")]:
        cleaned = str(value).strip() if value else ""
        if cleaned and cleaned not in candidate_names:
            candidate_names.append(cleaned)

    exact_query = """
    MATCH (c:Company)
    WHERE c.name IN $candidate_names
    RETURN c.id AS id, c.name AS name
    LIMIT 5
    """
    exact_matches = neo4j_mgr.graph.run(exact_query, candidate_names=candidate_names).data()
    if exact_matches:
        exact_matches.sort(key=lambda item: len(str(item.get("name", ""))))
        return exact_matches[0]

    contains_query = """
    MATCH (c:Company)
    WHERE ANY(name IN $candidate_names WHERE c.name CONTAINS name)
    RETURN c.id AS id, c.name AS name
    LIMIT 20
    """
    contains_matches = neo4j_mgr.graph.run(contains_query, candidate_names=candidate_names).data()
    if not contains_matches:
        return None

    normalized_targets = {normalize_company_name(name) for name in candidate_names if name}
    normalized_exact = [
        row for row in contains_matches
        if normalize_company_name(row.get("name")) in normalized_targets
    ]
    if normalized_exact:
        normalized_exact.sort(key=lambda item: len(str(item.get("name", ""))))
        return normalized_exact[0]

    if stock_info.get("full_name"):
        full_name = str(stock_info["full_name"]).strip()
        full_name_matches = [row for row in contains_matches if full_name in str(row.get("name", ""))]
        if len(full_name_matches) == 1:
            return full_name_matches[0]

    short_name = str(stock_info.get("name") or "").strip()
    short_name_matches = [row for row in contains_matches if short_name and short_name in str(row.get("name", ""))]
    if len(short_name_matches) == 1:
        return short_name_matches[0]

    return None

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
    bounded_hops = max(1, min(hops, 2))
    query = f"""
    MATCH path = (c {{id: $company_id}})-[*1..{bounded_hops}]-(m)
    WITH nodes(path) AS ns, relationships(path) AS rs
    UNWIND ns AS n
    UNWIND rs AS r
    RETURN collect(distinct n) AS nodes, collect(distinct r) AS links
    """
    res = neo4j_mgr.graph.run(query, company_id=company_id).data()
    return format_neo4j_data_to_graph(res) 


@router.get("/stock/graph")
def get_stock_graph(
    query_type: str = Query(..., pattern="^(code|name)$"),
    keyword: str = Query(..., min_length=1),
):
    try:
        stock_info = stock_directory_service.lookup(query_type=query_type, keyword=keyword)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"获取股票列表失败: {exc}") from exc

    if not stock_info:
        return {
            "matched": False,
            "has_data": False,
            "message": "未找到匹配的A股公司",
            "stock": None,
            "company": None,
            "graph": {"nodes": [], "edges": []},
        }

    company = find_company_by_stock(stock_info)
    if not company:
        return {
            "matched": True,
            "has_data": False,
            "message": "还没有数据",
            "stock": stock_info,
            "company": None,
            "graph": {"nodes": [], "edges": []},
        }

    graph_data = get_company_graph(company_id=company["id"], hops=2)
    has_data = bool(graph_data["nodes"])
    return {
        "matched": True,
        "has_data": has_data,
        "message": None if has_data else "还没有数据",
        "stock": stock_info,
        "company": company,
        "graph": graph_data,
    }

@router.get("/examples")
def get_example_companies():
    query = "MATCH (c:Company) RETURN c.id AS id, c.name AS name LIMIT 10"
    res = neo4j_mgr.graph.run(query).data()
    return {"examples": res}
