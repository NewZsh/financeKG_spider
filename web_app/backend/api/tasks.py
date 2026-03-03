from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os
from typing import Optional

router = APIRouter(prefix="/api/tasks", tags=["tasks"])

class UploadResponse(BaseModel):
    success: bool
    message: Optional[str]


@router.post('/tyc/upload', response_model=UploadResponse)
async def upload_keywords(file: UploadFile = File(...)):
    # Save uploaded file into existing dashboard folder structure
    try:
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        # assume financeKG_spider root is two levels up
        root = os.path.abspath(os.path.join(cur_dir, '..', '..', '..'))
        from tyc.spider import TYCSpider
        tyc = TYCSpider(None)
        direc = tyc.s_cfg.get('keywords_direc')
        if not direc:
            direc = os.path.join(root, 'data', 'tyc_keywords')
        os.makedirs(direc, exist_ok=True)
        # pick filename
        existing = [n for n in os.listdir(direc) if n.endswith('.txt') and not n.startswith('.')]
        filename = f"keywords_{len(existing)}.txt"
        dest = os.path.join(direc, filename)
        contents = await file.read()
        with open(dest, 'wb') as f:
            f.write(contents)
        # return count
        lines = [ln.strip() for ln in contents.decode('utf-8').split('\n') if ln.strip()]
        return UploadResponse(success=True, message=f"成功上传 {len(lines)} 个关键词")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
