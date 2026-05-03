from .graph_service import find_company_by_stock, get_company_graph
from stock.directory import stock_directory_service
from financeKG_spider.stock.sync_market_data import build_stock_market_payload, empty_market_payload


def lookup_stock_info(query_type: str, keyword: str):
    return stock_directory_service.lookup(query_type=query_type, keyword=keyword)


def build_stock_graph_payload(stock_info):
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


def get_stock_lookup_payload(query_type: str, keyword: str):
    stock_info = lookup_stock_info(query_type=query_type, keyword=keyword)
    return {
        "matched": bool(stock_info),
        "message": None if stock_info else "未找到匹配的A股公司",
        "stock": stock_info,
    }


def get_stock_graph_payload(query_type: str, keyword: str):
    stock_info = lookup_stock_info(query_type=query_type, keyword=keyword)
    if not stock_info:
        return {
            "matched": False,
            "has_data": False,
            "message": "未找到匹配的A股公司",
            "stock": None,
            "company": None,
            "graph": {"nodes": [], "edges": []},
        }

    return build_stock_graph_payload(stock_info)


def get_stock_detail_payload(query_type: str, keyword: str):
    stock_info = lookup_stock_info(query_type=query_type, keyword=keyword)
    if not stock_info:
        return {
            "matched": False,
            "message": "未找到匹配的A股公司",
            "stock": None,
            "company": None,
            "has_data": False,
            "graph": {"nodes": [], "edges": []},
            "market": empty_market_payload(),
        }

    graph_payload = build_stock_graph_payload(stock_info)
    market_payload = build_stock_market_payload(stock_info)
    return {
        **graph_payload,
        "market": market_payload,
    }