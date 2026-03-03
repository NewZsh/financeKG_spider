from fastapi import APIRouter, HTTPException
from typing import List
import os, sqlite3, json

router = APIRouter(prefix="/api/tyc", tags=["tyc"])

# helper to locate keyword directory and finished file

def _get_paths():
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    # Web App Backend: .../web_app/backend/api
    # Root: .../financeKG_spider
    root = os.path.abspath(os.path.join(cur_dir, '..', '..', '..'))
    
    cfg_path = os.path.join(root, 'spider.cfg')
    cfg = {}
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
        except:
            pass
            
    tyc_cfg = cfg.get('TYCSpider', {})
    
    direc = tyc_cfg.get('keywords_direc')
    if direc:
        # Resolve relative path against root
        if not os.path.isabs(direc):
            direc = os.path.join(root, direc)
    else:
        direc = os.path.join(root, 'data', 'tyc_keywords')
        
    finished_fn = tyc_cfg.get('keywords_finised_fn')
    if finished_fn:
        if not os.path.isabs(finished_fn):
            finished_fn = os.path.join(root, finished_fn)
    else:
         finished_fn = os.path.join(direc, 'keywords_finished.txt')
         
    return direc, finished_fn


@router.get('/files', response_model=List[str])
async def list_keyword_files():
    direc, _ = _get_paths()
    if not os.path.isdir(direc):
        return []
    return [fn for fn in os.listdir(direc) if fn.endswith('.txt') and not fn.startswith('.')]


@router.get('/keywords/stats')
async def keyword_counts():
    """返回总关键词数量和已完成数量"""
    direc, finished_fn = _get_paths()
    total = 0
    finished = 0
    if os.path.isdir(direc):
        for fn in os.listdir(direc):
            if fn.endswith('.txt') and not fn.startswith('.') and not fn.startswith('.processed'):
                try:
                    with open(os.path.join(direc, fn), 'r', encoding='utf-8') as f:
                        total += sum(1 for line in f if line.strip())
                except Exception:
                    pass
    if os.path.isfile(finished_fn):
        try:
            with open(finished_fn, 'r', encoding='utf-8') as f:
                finished = sum(1 for line in f if line.strip())
        except Exception:
            pass
    return {"total": total, "finished": finished}


@router.get('/stats')
async def stats():
    try:
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        root = os.path.abspath(os.path.join(cur_dir, '..', '..', '..'))
        db_file = os.path.join(root, 'data', 'spider_progress.db')
        
        conn = sqlite3.connect(db_file)
        # Set busy timeout to avoid immediate lock errors
        conn.execute("PRAGMA busy_timeout = 3000")
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM record")
        total_records = cursor.fetchone()[0]
        cursor.execute("""
            SELECT src, COUNT(*) as count, SUM(visit_times) as total_visits
            FROM record
            GROUP BY src
        """)
        src_stats = cursor.fetchall()
        cursor.execute("""
            SELECT src, id, entity_type, visit_time, visit_times
            FROM record
            ORDER BY visit_time DESC
            LIMIT 20
        """)
        recent_records = cursor.fetchall()
        conn.close()
        return {
            "total_records": total_records,
            "src_stats": src_stats,
            "recent_records": recent_records,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete('/files/{filename}')
async def delete_file(filename: str):
    direc, _ = _get_paths()
    path = os.path.join(direc, filename)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="file not found")
    try:
        os.remove(path)
        return {"deleted": filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
