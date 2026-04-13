from fastapi import APIRouter, HTTPException, Query

from ..services.stock_service import (
    get_stock_detail_payload,
    get_stock_graph_payload,
    get_stock_lookup_payload,
)

router = APIRouter()


def _run_stock_operation(operation, *args, **kwargs):
    try:
        return operation(*args, **kwargs)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=f"股票目录服务不可用: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"股票服务执行失败: {exc}") from exc


@router.get("/lookup")
def lookup_stock(
    query_type: str = Query(..., pattern="^(code|name)$"),
    keyword: str = Query(..., min_length=1),
):
    return _run_stock_operation(get_stock_lookup_payload, query_type, keyword)


@router.get("/graph")
def get_stock_graph(
    query_type: str = Query(..., pattern="^(code|name)$"),
    keyword: str = Query(..., min_length=1),
):
    return _run_stock_operation(get_stock_graph_payload, query_type, keyword)


@router.get("/detail")
def get_stock_detail(
    query_type: str = Query(..., pattern="^(code|name)$"),
    keyword: str = Query(..., min_length=1),
):
    return _run_stock_operation(get_stock_detail_payload, query_type, keyword)