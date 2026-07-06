#!/usr/bin/env python3
"""
基于 s.py 策略的每日盘后复盘与盘中实时监控工具。

## 策略规则说明 (Strategy Rules)

### 1. 选股与突破扫描 (Breakout Scan)
*   **范围**：全市场纯沪深主板（过滤创业、科创、北交所及 ST 股），上市满 180 天。
*   **形态**：均线多头排列 (MA5 > MA10 > MA20)，股价站稳均线。
*   **动能**：当日涨幅 > 4% (较开盘或较昨收) 且成交量显著放大 (Z-Score > 1.2 且大于昨日)。

### 2. 观察池与洗盘过滤 (Watch & Wash)
*   **有效期**：突破后 15 个交易日内。
*   **洗盘要求**：
    *   **禁忌**：期间严禁出现跌停（主板判定为 -9.9%）。
    *   **量能控制**：阴线洗盘时成交量不得超过突破当日的大阳线量。
    *   **定性**：寻找“洗盘阴线”作为买入前置条件。

### 3. 买入触发逻辑 (Buy Trigger)
*   **环境风控**：大盘中期趋势向上 (HS300 现价 >= MA60)。
*   **实时条件**：昨日判定为“完美洗盘阴线”，今日盘中价格上破昨天开盘价 (Trigger Price)。

### 4. 卖出与持仓风控 (Sell & Risk)
*   **移动止盈**：最高浮盈达到 10% 后激活。
    *   浮盈 < 30% 时，回撤至 70% 止盈。
    *   浮盈 >= 30% 时，回撤至 60% 止盈。
*   **刚性止损**：亏损达 -5% 时清仓。
*   **离场信号**：盘中价格跌破 MA20。

## 逻辑流程图 (UML Sequence)

```mermaid
graph TD
    A[盘后复盘 Review] --> B{全市场扫描}
    B -- 满足突破 --> C[加入突破列表]
    B -- 历史突破+洗盘中 --> D[维护观察池]
    
    E[实时监控 Monitor] --> F{环境检查: HS300 > MA60?}
    F -- Yes --> G[扫描观察池]
    G -- 价格 > 昨开 --> H[提示买入信号]
    
    E --> I[扫描持仓股]
    I -- 浮盈 > 10% & 回撤 --> J[提示移动止盈]
    I -- 亏损 < -5% --> K[提示刚性止损]
    I -- 价格 < MA20 --> L[提示避险卖出]
```

## 功能说明
1. [复盘] 扫描全市场符合 s.py 突破形态与洗盘阶段的股票。
2. [监控] 实时监控观察池买入信号与持仓股止盈止损信号。
"""

import sqlite3
import pandas as pd
import numpy as np
import json
import argparse
import urllib.request
import time
from datetime import datetime, timedelta
from pathlib import Path

# 基础路径配置
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "stock.db"
REVIEW_DIR = DATA_DIR / "stock"
HOLDINGS_FILE = DATA_DIR / "holdings_s.json"
API_TEMPLATE = "http://qt.gtimg.cn/q={}"

# 策略参数 (同步 s.py)
MA_SHORT = 5
MA_MID = 10
MA_LONG = 20
VOL_Z_SCORE_THRESHOLD = 1.2
PRICE_GAIN_THRESHOLD = 1.04
MIN_LISTED_DAYS = 180

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        if isinstance(obj, (np.integer, int)):
            return int(obj)
        if isinstance(obj, (np.floating, float)):
            return float(obj)
        if isinstance(obj, (np.ndarray, list)):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

def to_symbol(code: str) -> str:
    """统一代码格式：前缀 sh/sz"""
    code_str = str(code).strip().lower()
    # 移除现有前缀
    code_clean = code_str.replace('sh', '').replace('sz', '')
    if code_clean.startswith(('6', '9', '5')):
        return f"sh{code_clean}"
    return f"sz{code_clean}"

def clean_code(symbol: str) -> str:
    """去掉前缀，仅保留 6 位数字代码"""
    return str(symbol).lower().replace('sh', '').replace('sz', '')

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ==================== 1. 复盘逻辑 (Review) ====================

def load_data(review_date: str):
    """加载指定日期及周边的行情数据"""
    conn = get_db_connection()
    
    # 筛选主板非 ST 股
    query_stocks = """
    SELECT code, name, board FROM stocks 
    WHERE is_st = 0 
    AND NOT (code LIKE '300%' OR code LIKE '301%' OR code LIKE '688%' OR code LIKE '8%' OR code LIKE '4%')
    """
    stocks_df = pd.read_sql_query(query_stocks, conn)
    
    dates_query = "SELECT DISTINCT trade_date FROM daily_bars WHERE trade_date <= ? ORDER BY trade_date DESC LIMIT 100"
    dates = pd.read_sql_query(dates_query, conn, params=[review_date])['trade_date'].tolist()
    if not dates:
        conn.close()
        return None, None
    
    placeholders = ','.join(['?'] * len(dates))
    query_bars = f"""
    SELECT code, trade_date, open, close, high, low, volume 
    FROM daily_bars 
    WHERE trade_date IN ({placeholders})
    """
    bars_df = pd.read_sql_query(query_bars, conn, params=dates)
    conn.close()
    
    return stocks_df, bars_df

def analyze_s_strategy(stocks_df, bars_df, review_date):
    results = {"breakout": [], "watching": []}
    grouped = bars_df.groupby('code')
    
    for code, group in grouped:
        if code not in stocks_df['code'].values:
            continue
            
        name = stocks_df[stocks_df['code'] == code]['name'].values[0]
        df = group.sort_values('trade_date').reset_index(drop=True)
        
        current_idx_list = df[df['trade_date'] == review_date].index
        if len(current_idx_list) == 0: continue
        idx = current_idx_list[0]
        
        # 1. 突破扫描 (当天)
        sub_df = df.iloc[:idx+1]
        if len(sub_df) < 35: continue
        
        last_close = sub_df['close'].iloc[-1]
        last_open = sub_df['open'].iloc[-1]
        last_vol = sub_df['volume'].iloc[-1]
        prev_close = sub_df['close'].iloc[-2]
        prev_vol = sub_df['volume'].iloc[-2]
        
        ma5 = sub_df['close'].rolling(5).mean().iloc[-1]
        ma10 = sub_df['close'].rolling(10).mean().iloc[-1]
        ma20 = sub_df['close'].rolling(20).mean().iloc[-1]
        
        vol_series = sub_df['volume'].iloc[-22:-1]
        z_score = (last_vol - vol_series.mean()) / vol_series.std() if vol_series.std() > 0 else 0
        
        is_ma_ok = last_close > ma5 > ma10 > ma20
        is_price_ok = (last_close / last_open > PRICE_GAIN_THRESHOLD) or (last_close / prev_close > PRICE_GAIN_THRESHOLD)
        is_vol_ok = z_score > VOL_Z_SCORE_THRESHOLD and last_vol > prev_vol
        
        if is_ma_ok and is_price_ok and is_vol_ok:
            results["breakout"].append({
                "code": code, "name": name, "close": last_close,
                "pct": round((last_close/prev_close - 1)*100, 2),
                "z_score": round(z_score, 2)
            })
            continue 
            
        # 2. 观察池维护 (洗盘回溯)
        for lookback in range(1, 16):
            if idx - lookback < 30: break
            
            c_df = df.iloc[:idx - lookback + 1]
            c_last_close, c_last_open, c_last_vol = c_df['close'].iloc[-1], c_df['open'].iloc[-1], c_df['volume'].iloc[-1]
            c_prev_close, c_prev_vol = c_df['close'].iloc[-2], c_df['volume'].iloc[-2]
            
            c_ma5 = c_df['close'].rolling(5).mean().iloc[-1]
            c_ma10 = c_df['close'].rolling(10).mean().iloc[-1]
            c_ma20 = c_df['close'].rolling(20).mean().iloc[-1]
            
            c_vol_s = c_df['volume'].iloc[-22:-1]
            c_z_score = (c_last_vol - c_vol_s.mean()) / c_vol_s.std() if c_vol_s.std() > 0 else 0
            
            if (c_last_close > c_ma5 > c_ma10 > c_ma20) and \
               ((c_last_close/c_last_open > PRICE_GAIN_THRESHOLD) or (c_last_close/c_prev_close > PRICE_GAIN_THRESHOLD)) and \
               (c_z_score > VOL_Z_SCORE_THRESHOLD and c_last_vol > c_prev_vol):
                
                # 检查洗盘期
                wash_period = df.iloc[idx-lookback+1 : idx+1]
                valid_wash = True
                for i, row in wash_period.iterrows():
                    if (row['close'] / df.iloc[i-1]['close']) <= 0.901: # 跌停
                        valid_wash = False; break
                    if row['close'] < row['open'] and row['volume'] > c_last_vol: # 阴线放量
                        valid_wash = False; break
                
                if valid_wash:
                    results["watching"].append({
                        "code": code, "name": name, "breakout_date": c_df['trade_date'].iloc[-1],
                        "wash_days": lookback, "last_open": last_open, "last_close": last_close,
                        "is_wash_perfect": last_close < last_open
                    })
                break
    return results

def run_review(review_date):
    print(f"[{datetime.now()}] 正在复盘 ({review_date})...")
    stocks_df, bars_df = load_data(review_date)
    if stocks_df is None:
        print("未找到数据，请先同步行情。")
        return
    results = analyze_s_strategy(stocks_df, bars_df, review_date)
    
    # 打印报告
    print(f"\n# s.py 策略复盘 ({review_date})")
    print("-" * 40)
    print(f"今日突破: {len(results['breakout'])} 只")
    print(f"观察池待命: {len(results['watching'])} 只")
    
    output_file = REVIEW_DIR / f"review_s_{review_date.replace('-', '')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, cls=NpEncoder, ensure_ascii=False, indent=2)
    print(f"结果已保存至: {output_file}")

# ==================== 2. 监控逻辑 (Monitor) ====================

def fetch_realtime(codes: list[str]):
    if not codes: return {}
    symbols = ",".join([to_symbol(c) for c in codes])
    try:
        url = API_TEMPLATE.format(symbols)
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = resp.read().decode('gbk')
            results = {}
            for line in data.split(';'):
                if len(line) < 50: continue
                p = line.split('~')
                results[clean_code(p[2])] = {"name": p[1], "current": float(p[3]), "open": float(p[5]), "pct": float(p[32])}
            return results
    except Exception as e:
        print(f"Fetch error: {e}"); return {}

def get_ma20(code, review_date):
    """从数据库获取该股最新的 MA20"""
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(
            "SELECT close FROM daily_bars WHERE code = ? AND trade_date <= ? ORDER BY trade_date DESC LIMIT 20",
            conn, params=[code, review_date]
        )
        conn.close()
        return df['close'].mean() if len(df) >= 20 else None
    except: return None

def get_hs300_trend(review_date):
    try:
        conn = get_db_connection()
        df = pd.read_sql_query(
            "SELECT close FROM daily_bars WHERE code = '000300' AND trade_date <= ? ORDER BY trade_date DESC LIMIT 60",
            conn, params=[review_date]
        )
        conn.close()
        return df['close'].iloc[0] >= df['close'].mean() if len(df) == 60 else True
    except: return True

def run_monitor(review_date, interval=10):
    # 1. 加载观察池
    json_path = REVIEW_DIR / f"review_s_{review_date.replace('-', '')}.json"
    watch_pool = []
    if json_path.exists():
        with open(json_path, 'r', encoding='utf-8') as f:
            watch_pool = [i for i in json.load(f).get("watching", []) if i.get('is_wash_perfect')]
    
    # 2. 加载持仓配置
    holdings = {}
    if HOLDINGS_FILE.exists():
        try:
            raw_holdings = json.loads(HOLDINGS_FILE.read_text(encoding='utf-8'))
            for code_key, val in raw_holdings.items():
                code = clean_code(code_key)
                holdings[code] = {
                    "buy_price": val['buy_price'],
                    "ma20": get_ma20(code, review_date),
                    "max_pnl": 0.0
                }
        except Exception as e:
            print(f"Error loading holdings: {e}")
    
    monitor_codes = list(set([i['code'] for i in watch_pool] + list(holdings.keys()) + ["000300"]))
    watch_info = {i['code']: i for i in watch_pool}
    triggered = set()
    trend_ok = get_hs300_trend(review_date)
    last_trend_check = datetime.now()

    print(f"[{datetime.now()}] 实时监控启动 | 监控: {len(monitor_codes)-1} 只 | 大盘: {'OK' if trend_ok else 'WEAK'}")

    while True:
        now = datetime.now()
        cur_t = now.strftime("%H:%M")
        if (now - last_trend_check).seconds > 600:
            trend_ok = get_hs300_trend(review_date); last_trend_check = now
            
        real_data = fetch_realtime(monitor_codes)
        hs300 = real_data.get("000300")
        print(f"[{cur_t}] HS300: {hs300['current'] if hs300 else '??'} ({hs300['pct'] if hs300 else '??'}%) | 指数趋势: {'OK' if trend_ok else 'WEAK'}", end='\r')

        # 买入逻辑
        if trend_ok:
            for item in watch_pool:
                code = item['code']
                if code in triggered or code not in real_data: continue
                info = real_data[code]
                if info['current'] > item['last_open'] > 0:
                    print(f"\n🚀 [买入信号] {info['name']} ({code}) {info['current']} > {item['last_open']} (昨开)")
                    triggered.add(code)

        # 卖出逻辑 (持仓)
        for code, h in holdings.items():
            if code not in real_data: continue
            info = real_data[code]
            cur_p = info['current']
            pnl = (cur_p - h['buy_price']) / h['buy_price']
            h['max_pnl'] = max(h.get('max_pnl', 0.0), pnl)
            
            reason = None
            if h['max_pnl'] >= 0.10:
                stop = h['max_pnl'] * (0.7 if h['max_pnl'] <= 0.30 else 0.6)
                if pnl <= stop: reason = f"移动止盈 (回撤位:{stop*100:.1f}%)"
            elif pnl <= -0.05: reason = "止损线 (-5%)"
            elif h['ma20'] and cur_p < h['ma20']: reason = f"跌破MA20 ({h['ma20']:.2f})"
            
            if reason and code not in triggered:
                print(f"\n🚨 [卖出提示] {info['name']} ({code}) 原因: {reason} | 当前盈亏: {pnl*100:.1f}%")
                triggered.add(code)

        time.sleep(interval)

# ==================== 3. 入口 ====================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="s.py 策略工具箱")
    parser.add_argument("--date", type=str, help="日期 YYYY-MM-DD")
    parser.add_argument("--mode", choices=["review", "monitor", "both"], default="review", help="模式")
    args = parser.parse_args()
    
    target_date = args.date or datetime.now().strftime("%Y-%m-%d")
    
    if args.mode in ["review", "both"]:
        run_review(target_date)
    
    if args.mode in ["monitor", "both"]:
        run_monitor(target_date)
