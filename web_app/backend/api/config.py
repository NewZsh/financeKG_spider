from fastapi import APIRouter, Request
from base_spider import base_spider
import json

router = APIRouter(prefix="/api/config", tags=["config"])

@router.get('/')
async def get_config():
    return base_spider().cfg

@router.post('/')
async def update_config(request: Request):
    try:
        # 支持 application/json 或 form-data
        if request.headers.get('content-type', '').startswith('application/json'):
            new_cfg = await request.json()
        else:
            form = await request.form()
            new_cfg_str = form.get('config')
            if not new_cfg_str:
                return {"success": False, "error": "Missing config parameter"}
            new_cfg = json.loads(new_cfg_str)
        # 保存到文件
        spider = base_spider()
        with open(spider.cfg_file, 'w', encoding='utf-8') as f:
            json.dump(new_cfg, f, indent=4, ensure_ascii=False)
        spider.cfg = new_cfg
        return {"success": True, "message": "配置已更新"}
    except Exception as e:
        return {"success": False, "error": str(e)}
