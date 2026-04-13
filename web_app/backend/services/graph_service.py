import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from neo4j_utils import Neo4jManager
from .stock_directory import normalize_company_name

neo4j_mgr = Neo4jManager()


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
                    "properties": dict(node),
                }

        for rel in record["links"]:
            edges.append(
                {
                    "source": str(dict(rel.start_node).get("id")),
                    "target": str(dict(rel.end_node).get("id")),
                    "label": type(rel).__name__,
                    "properties": dict(rel),
                }
            )

    unique_edges = []
    seen_edges = set()
    for edge in edges:
        edge_id = f"{edge['source']}-{edge['label']}-{edge['target']}"
        if edge_id not in seen_edges:
            seen_edges.add(edge_id)
            unique_edges.append(edge)

    return {
        "nodes": list(nodes_map.values()),
        "edges": unique_edges,
    }


def search_companies(keyword: str):
    query = """
    MATCH (c:Company)
    WHERE c.name CONTAINS $keyword
    RETURN c.id AS id, c.name AS name LIMIT 10
    """
    return neo4j_mgr.graph.run(query, keyword=keyword).data()


def get_company_graph(company_id: str, hops: int = 2):
    bounded_hops = max(1, min(hops, 2))
    query = f"""
    MATCH path = (c {{id: $company_id}})-[*1..{bounded_hops}]-(m)
    WITH nodes(path) AS ns, relationships(path) AS rs
    UNWIND ns AS n
    UNWIND rs AS r
    RETURN collect(distinct n) AS nodes, collect(distinct r) AS links
    """
    result = neo4j_mgr.graph.run(query, company_id=company_id).data()
    return format_neo4j_data_to_graph(result)


def get_example_companies(limit: int = 10):
    query = "MATCH (c:Company) RETURN c.id AS id, c.name AS name LIMIT $limit"
    return neo4j_mgr.graph.run(query, limit=limit).data()