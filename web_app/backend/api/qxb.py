from fastapi import APIRouter

router = APIRouter(prefix="/api/qxb_spider", tags=["qxb_spider"])

@router.get('/')
async def qxb_spider_status():
    return {"status": "ok", "message": "QXB Spider is running"}
