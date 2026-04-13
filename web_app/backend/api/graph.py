from fastapi import APIRouter

from ..services.graph_service import get_company_graph, get_example_companies, search_companies

router = APIRouter()


@router.get("/search")
def search_graph_companies(keyword: str):
    return {"results": search_companies(keyword)}


@router.get("/company/{company_id}/graph")
def get_company_graph_view(company_id: str, hops: int = 2):
    return get_company_graph(company_id=company_id, hops=hops)


@router.get("/examples")
def get_graph_examples():
    return {"examples": get_example_companies()}
