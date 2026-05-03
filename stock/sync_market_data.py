"""同步 A 股行情到 SQLite，并从 DB 读取前端所需的市场数据载荷。

参数：
    --date: 同步日期，格式 YYYYMMDD 或 YYYY-MM-DD，默认为当天
    --mode: incremental 仅拉当日，full 全量刷新到当日，默认为 incremental   
    --limit: 仅同步前 N 只股票，便于调试，默认为 0（不限制）
    --force-full-adjust: 强制所有股票重刷前复权历史，默认为 False

用法：
    - 增量获取当日行情（适合每天定时执行），写入数据库地址为：data/stock.db
        python -m stock.sync_market_data --date 2026-05-01 --mode incremental

    - 全量刷新历史行情到当日（适合首次运行或需要重置数据时执行，注意可能需要较长时间），写入数据库地址为：data/stock.db
        python -m stock.sync_market_data --date 2026-05-01 --mode full

    - 调试时仅同步前 10 只股票的增量数据：
        python -m stock.sync_market_data --date 2026-05-01 --mode incremental --limit 10
    
    - 调试时强制全量刷新前复权历史：
        python -m stock.sync_market_data --date 2026-05-01 --mode full --force-full-adjust
"""

from __future__ import annotations

import argparse
import copy
import json
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import akshare
import pandas as pd
import requests

MARKET_CACHE_TTL_SECONDS = 10 * 60
FULL_REFRESH_START = "20100101"
TENCENT_BATCH_SIZE = 50
INTRADAY_LOOKBACK_DAYS = 5
INTRADAY_FETCH_WINDOW_DAYS = 14

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SCHEMA_PATH = DATA_DIR / "schema" / "stock.sql"
DB_PATH = DATA_DIR / "stock.db"

BOARD_CATEGORIES = {
    "主板-沪（60）": lambda code: code.startswith("60"),
    "主板-深（00）": lambda code: code.startswith("000") or code.startswith("001"),
    "中小板（002/003）": lambda code: code.startswith("002") or code.startswith("003"),
    "创业板（300）": lambda code: code.startswith("30"),
    "科创板（688）": lambda code: code.startswith("688"),
    "北交所（8/4/9）": lambda code: code.startswith(("8", "4", "9")),
}

class Utils:
    """提供通用的工具和转换函数"""
    
    @staticmethod
    def now_ts() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    def parse_sync_date(value: Optional[str]) -> Tuple[str, str]:
        if not value:
            dt = datetime.now()
        else:
            raw = value.strip()
            if len(raw) == 8 and raw.isdigit():
                dt = datetime.strptime(raw, "%Y%m%d")
            else:
                dt = datetime.strptime(raw, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d"), dt.strftime("%Y%m%d")

    @staticmethod
    def normalize_trade_date(value: Any) -> str:
        return pd.Timestamp(value).strftime("%Y-%m-%d")

    @staticmethod
    def classify_board(code: str) -> str:
        for name, check in BOARD_CATEGORIES.items():
            if check(code):
                return name
        return "其他"

    @staticmethod
    def is_st(name: str) -> bool:
        if not isinstance(name, str):
            return True
        return "ST" in name.upper() or "*ST" in name

    @staticmethod
    def to_symbol(code: str) -> str:
        """补充 sh 或 sz 前缀"""
        return f"sh{code}" if code.startswith(("6", "688")) else f"sz{code}"

    @staticmethod
    def safe_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if pd.isna(number):
            return None
        return round(number, 4)

    @staticmethod
    def retry_call(fetcher, *, label: str, attempts: int = 3, delay_seconds: float = 1.2) -> Any:
        """具备重试机制的数据抓取包装器"""
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                return fetcher()
            except Exception as exc:
                last_error = exc
                if attempt == attempts:
                    break
                time.sleep(delay_seconds * attempt)
        raise RuntimeError(f"{label}在重试 {attempts} 次后仍失败: {last_error}") from last_error


class DbManager:
    """管理 SQLite 数据库连接与初始化"""
    @staticmethod
    def get_connection() -> sqlite3.Connection:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        return conn


class MarketDataFetcher:
    """封装全部对于 AkShare 和腾讯接口的网络请求拉取逻辑"""
    
    @staticmethod
    def get_all_stock_codes() -> pd.DataFrame:
        df = akshare.stock_info_a_code_name()
        df["board"] = df["code"].apply(Utils.classify_board)
        df["is_st"] = df["name"].apply(Utils.is_st).astype(int)
        return df[["code", "name", "board", "is_st"]].copy()

    @staticmethod
    def fetch_tencent_realtime(codes: List[str]) -> Dict[str, dict]:
        result: Dict[str, dict] = {}
        tencent_codes = [Utils.to_symbol(code) for code in codes]
        for index in range(0, len(tencent_codes), TENCENT_BATCH_SIZE):
            batch = tencent_codes[index:index + TENCENT_BATCH_SIZE]
            try:
                response = requests.get(f"https://qt.gtimg.cn/q={','.join(batch)}", timeout=15)
                if response.status_code != 200:
                    continue
                for line in [item for item in response.text.strip().split(";") if "~" in item]:
                    parts = line.split("~")
                    if len(parts) < 50:
                        continue
                    code_str = parts[2]
                    try:
                        price = float(parts[3]) if parts[3] else 0
                        open_price = float(parts[5]) if parts[5] else 0
                        pct_change = float(parts[32]) if parts[32] else 0
                        high_price = float(parts[33]) if parts[33] else 0
                        low_price = float(parts[34]) if parts[34] else 0
                        amount_wan = float(parts[37]) if parts[37] else 0
                        float_mv_yi = float(parts[44]) if parts[44] else 0
                    except (TypeError, ValueError):
                        continue
                    if price <= 0:
                        continue
                    result[code_str] = {
                        "name": parts[1],
                        "price": price,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "pct_change": pct_change,
                        "amount_wan": amount_wan,
                        "float_mv_yi": float_mv_yi,
                    }
            except requests.RequestException:
                continue
            if index > 0 and index % 500 == 0:
                time.sleep(0.2)
        return result

    @staticmethod
    def fetch_daily_bars(code: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        df = Utils.retry_call(
            lambda: akshare.stock_zh_a_daily(symbol=Utils.to_symbol(code), start_date=start_date, end_date=end_date, adjust=adjust),
            label=f"{code} 日线行情获取({adjust})"
        )
        return MarketDataFetcher._normalize_daily_bars_df(df)

    @staticmethod
    def fetch_qfq_factor_events(code: str) -> List[Tuple[str, float]]:
        df = Utils.retry_call(
            lambda: akshare.stock_zh_a_daily(symbol=Utils.to_symbol(code), adjust="qfq-factor"),
            label=f"{code} 复权因子获取"
        )
        if df is None or df.empty:
            return []
        data = df.copy()
        data["date"] = data["date"].apply(Utils.normalize_trade_date)
        data["qfq_factor"] = pd.to_numeric(data["qfq_factor"], errors="coerce")
        data = data.dropna(subset=["date", "qfq_factor"]).sort_values("date")
        return [(row["date"], round(float(row["qfq_factor"]), 12)) for _, row in data.iterrows()]

    @staticmethod
    def fetch_dividend_detail(code: str, event_date: str) -> List[Tuple[str, str]]:
        try:
            df = Utils.retry_call(
                lambda: akshare.stock_history_dividend_detail(symbol=code, indicator="分红", date=event_date),
                label=f"{code} 分红详情获取"
            )
        except Exception:
            return []
        if df is None or df.empty:
            return []
        items: List[Tuple[str, str]] = []
        for _, row in df.iterrows():
            item = str(row.get("item", "")).strip()
            value = str(row.get("value", "")).strip()
            if item:
                items.append((item, value))
        return items

    @staticmethod
    def fetch_intraday_bars(code: str, start_datetime: str, end_datetime: str) -> pd.DataFrame:
        df = Utils.retry_call(
            lambda: akshare.stock_zh_a_hist_min_em(
                symbol=code, start_date=start_datetime, end_date=end_datetime,
                period="1", adjust=""
            ),
            label=f"{code} 分时行情获取",
        )
        return MarketDataFetcher._normalize_intraday_df(df)

    @staticmethod
    def fetch_trade_days() -> set[str]:
        try:
            df = Utils.retry_call(lambda: akshare.tool_trade_date_hist_sina(), label="交易日历获取", attempts=2, delay_seconds=0.8)
        except Exception:
            return set()
        if df is None or df.empty:
            return set()
        date_column = "trade_date" if "trade_date" in df.columns else df.columns[0]
        return {pd.Timestamp(v).strftime("%Y-%m-%d") for v in df[date_column].tolist()}

    @staticmethod
    def _normalize_daily_bars_df(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
        if "date" not in df.columns:
            df = df.reset_index()
            if "date" not in df.columns and "index" in df.columns:
                df = df.rename(columns={"index": "date"})
        rename_map = {"vol": "volume", "成交量": "volume", "成交额": "amount"}
        df = df.rename(columns=rename_map)
        needed_columns = ["date", "open", "high", "low", "close", "volume", "amount"]
        missing = [col for col in needed_columns if col not in df.columns]
        if missing:
            raise ValueError(f"K line data missing columns: {missing}")
        data = df[needed_columns].copy()
        data["date"] = data["date"].apply(Utils.normalize_trade_date)
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            data[col] = pd.to_numeric(data[col], errors="coerce")
        return data.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")

    @staticmethod
    def _normalize_intraday_df(df: pd.DataFrame) -> pd.DataFrame:
        columns = ["trade_timestamp", "trade_date", "trade_time", "open", "close", "high", "low", 
                   "avg_price", "volume", "amount", "change_pct", "change_amount"]
        if df is None or df.empty:
            return pd.DataFrame(columns=columns)
        rename_map = {
            "时间": "trade_timestamp", "开盘": "open", "收盘": "close", "最高": "high",
            "最低": "low", "均价": "avg_price", "成交量": "volume", "成交额": "amount",
            "涨跌幅": "change_pct", "涨跌额": "change_amount",
        }
        data = df.rename(columns=rename_map).copy()
        missing = [col for col in ["trade_timestamp", "open", "close", "high", "low", "volume", "amount"] if col not in data.columns]
        if missing:
            raise ValueError(f"Intraday data missing columns: {missing}")

        data["trade_timestamp"] = pd.to_datetime(data["trade_timestamp"], errors="coerce")
        data = data.dropna(subset=["trade_timestamp"])
        data["trade_date"] = data["trade_timestamp"].dt.strftime("%Y-%m-%d")
        data["trade_time"] = data["trade_timestamp"].dt.strftime("%H:%M:%S")
        data["trade_timestamp"] = data["trade_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        for col in ["open", "close", "high", "low", "avg_price", "volume", "amount", "change_pct", "change_amount"]:
            data[col] = pd.to_numeric(data[col], errors="coerce") if col in data.columns else None

        return data[columns].sort_values("trade_timestamp").drop_duplicates(subset=["trade_timestamp"], keep="last")


class StockMarketSyncEngine:
    """
    行情同步引擎，负责控制整个市场的股票列表刷新及行情的全量或增量爬取写入
    """
    def __init__(self, mode: str = "incremental", limit: int = 0, force_full_adjust: bool = False, request_pause: float = 0.15):
        self.mode = mode
        self.limit = limit
        self.force_full_adjust = force_full_adjust
        self.request_pause = request_pause
        self.db_lock = threading.Lock()
        self.summary_lock = threading.Lock()
        self.summary = {
            "mode": mode, "universe_count": 0, "synced_codes": 0, "raw_bar_rows": 0,
            "qfq_bar_rows": 0, "intraday_bar_rows": 0, "distribution_rows": 0,
            "spot_enriched_codes": 0, "full_refresh_codes": [], "adjustment_event_updates": 0, "errors": []
        }

    def run_sync(self, review_date: Optional[str] = None) -> dict:
        """主同步逻辑"""
        trade_date, compact_date = Utils.parse_sync_date(review_date)
        end_date = compact_date
        is_full = self.mode == "full"
        daily_start_date = FULL_REFRESH_START if is_full else compact_date
        
        target_dt = datetime.strptime(trade_date, "%Y-%m-%d")
        intra_start = (target_dt - timedelta(days=INTRADAY_FETCH_WINDOW_DAYS)).strftime("%Y-%m-%d 09:30:00") if is_full else f"{trade_date} 09:30:00"
        intra_end = (target_dt + timedelta(days=1)).strftime("%Y-%m-%d 15:00:00") if is_full else f"{trade_date} 15:00:00"

        self.summary["review_date"] = trade_date
        self.summary["db_path"] = str(DB_PATH)

        conn = DbManager.get_connection()
        stocks_df = MarketDataFetcher.get_all_stock_codes()
        self.summary["universe_count"] = self._upsert_stocks(conn, stocks_df)
        
        sync_df = stocks_df.head(self.limit).copy() if self.limit > 0 else stocks_df.copy()
        
        # 拉取当天的腾讯分时切片以补齐最新收盘市值等信息
        tencent_spot = MarketDataFetcher.fetch_tencent_realtime(sync_df["code"].tolist())

        total_tasks = len(sync_df)
        completed_tasks = 0

        # 清除针对这批股票的缓存
        for _, row in sync_df.iterrows():
            StockMarketDataReader.clear_cache(str(row["code"]))

        for row in sync_df.itertuples(index=False):
            self._process_single_stock(
                conn=conn, code=str(row.code), trade_date=trade_date, daily_start_date=daily_start_date, end_date=end_date,
                intra_start=intra_start, intra_end=intra_end,
                tencent_spot=tencent_spot,
                is_full=is_full
            )
            completed_tasks += 1
            sys.stdout.write(f"\r同步进度: [{completed_tasks}/{total_tasks}] {completed_tasks/total_tasks*100:.1f}%")
            sys.stdout.flush()
        
        sys.stdout.write("\n")
        conn.close()
        return self.summary

    def _process_single_stock(self, conn: sqlite3.Connection, code: str, trade_date: str, daily_start_date: str, end_date: str,
                              intra_start: str, intra_end: str, tencent_spot: dict, is_full: bool):
        try:
            spot = tencent_spot.get(code)
            if spot and spot.get("float_mv_yi"):
                with self.db_lock:
                    self._upsert_latest_market_value(conn, code, trade_date, spot["float_mv_yi"])

            api_events = MarketDataFetcher.fetch_qfq_factor_events(code)
            with self.db_lock:
                db_events = self._get_existing_adjustment_events(conn, code)
            
            new_event_dates = sorted(set(date for date, _ in api_events) - set(date for date, _ in db_events))
            needs_full = is_full or self.force_full_adjust or api_events != db_events

            if api_events and api_events != db_events:
                with self.db_lock:
                    self._replace_adjustment_events(conn, code, api_events)
                with self.summary_lock:
                    self.summary["adjustment_event_updates"] += 1

            for ev_date in new_event_dates:
                items = MarketDataFetcher.fetch_dividend_detail(code, ev_date)
                if items:
                    with self.db_lock:
                        self._replace_dividend_details(conn, code, ev_date, items)

            raw_bars = MarketDataFetcher.fetch_daily_bars(code, start_date=daily_start_date, end_date=end_date, adjust="")
            spot_for_merge = spot if trade_date == datetime.now().strftime("%Y-%m-%d") else None
            
            if spot_for_merge:
                raw_bars = self._merge_daily_spot(trade_date, raw_bars, spot_for_merge)
                
            with self.db_lock:
                raw_rows = self._upsert_daily_bars(conn, code, "none", raw_bars)
            with self.summary_lock:
                self.summary["raw_bar_rows"] += raw_rows
                if spot_for_merge:
                    self.summary["spot_enriched_codes"] += 1

            if needs_full:
                qfq_bars = MarketDataFetcher.fetch_daily_bars(code, start_date=FULL_REFRESH_START, end_date=end_date, adjust="qfq")
                with self.db_lock:
                    qfq_rows = self._replace_qfq_history(conn, code, qfq_bars)
                with self.summary_lock:
                    self.summary["qfq_bar_rows"] += qfq_rows
                    self.summary["full_refresh_codes"].append(code)
            else:
                qfq_bars = MarketDataFetcher.fetch_daily_bars(code, start_date=daily_start_date, end_date=end_date, adjust="qfq")
                with self.db_lock:
                    qfq_rows = self._upsert_daily_bars(conn, code, "qfq", qfq_bars)
                with self.summary_lock:
                    self.summary["qfq_bar_rows"] += qfq_rows

            intra_df = MarketDataFetcher.fetch_intraday_bars(code, intra_start, intra_end)
            if not intra_df.empty:
                with self.db_lock:
                    intra_rows = self._replace_intraday_bars(conn, code, intra_df)
                recent_intra_df = intra_df[intra_df["trade_date"].isin(sorted(intra_df["trade_date"].unique())[-INTRADAY_LOOKBACK_DAYS:])]
                distributions = self._build_daily_distributions(recent_intra_df)
                with self.db_lock:
                    dist_rows = self._replace_daily_distributions(conn, code, distributions)
                with self.summary_lock:
                    self.summary["intraday_bar_rows"] += intra_rows
                    self.summary["distribution_rows"] += dist_rows

            with self.summary_lock:
                self.summary["synced_codes"] += 1
            
        except Exception as exc:
            with self.summary_lock:
                self.summary["errors"].append(f"{code}: {exc}")
        if self.request_pause > 0:
            time.sleep(self.request_pause)

    # Database logic
    def _upsert_stocks(self, conn: sqlite3.Connection, stocks_df: pd.DataFrame) -> int:
        rows = [(r.code, r.name, r.board, int(r.is_st), Utils.now_ts()) for r in stocks_df.itertuples(index=False)]
        conn.executemany("""
            INSERT INTO stocks(code, name, board, is_st, updated_at) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET name=excluded.name, board=excluded.board, is_st=excluded.is_st, updated_at=excluded.updated_at
        """, rows)
        conn.commit()
        return len(rows)

    def _upsert_latest_market_value(self, conn: sqlite3.Connection, code: str, trade_date: str, float_mv_yi: float) -> None:
        conn.execute("""
            INSERT INTO latest_market_value(code, trade_date, float_mv_yi, fetched_at) VALUES (?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET trade_date=excluded.trade_date, float_mv_yi=excluded.float_mv_yi, fetched_at=excluded.fetched_at
        """, (code, trade_date, float_mv_yi, Utils.now_ts()))

    def _upsert_daily_bars(self, conn: sqlite3.Connection, code: str, adjust_type: str, bars_df: pd.DataFrame) -> int:
        if bars_df.empty: return 0
        rows = [(code, r.date, adjust_type, r.open, r.high, r.low, r.close, r.volume, r.amount) for r in bars_df.itertuples(index=False)]
        conn.executemany("""
            INSERT INTO daily_bars(code, trade_date, adjust_type, open, high, low, close, volume, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code, trade_date, adjust_type) DO UPDATE SET open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, volume=excluded.volume, amount=excluded.amount
        """, rows)
        conn.commit()
        return len(rows)

    def _replace_qfq_history(self, conn: sqlite3.Connection, code: str, bars_df: pd.DataFrame) -> int:
        if bars_df.empty: return 0
        rows = [(code, r.date, "qfq", r.open, r.high, r.low, r.close, r.volume, r.amount) for r in bars_df.itertuples(index=False)]
        with conn:
            conn.execute("DELETE FROM daily_bars WHERE code = ? AND adjust_type = 'qfq'", (code,))
            conn.executemany("INSERT INTO daily_bars(code, trade_date, adjust_type, open, high, low, close, volume, amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        return len(rows)

    def _get_existing_adjustment_events(self, conn: sqlite3.Connection, code: str) -> List[Tuple[str, float]]:
        rows = conn.execute("SELECT event_date, qfq_factor FROM adjustment_events WHERE code = ? ORDER BY event_date", (code,)).fetchall()
        return [(row["event_date"], round(float(row["qfq_factor"]), 12)) for row in rows]

    def _replace_adjustment_events(self, conn: sqlite3.Connection, code: str, events: List[Tuple[str, float]]) -> None:
        with conn:
            conn.execute("DELETE FROM adjustment_events WHERE code = ?", (code,))
            conn.executemany("INSERT INTO adjustment_events(code, event_date, qfq_factor, fetched_at) VALUES (?, ?, ?, ?)",
                             [(code, ev, fac, Utils.now_ts()) for ev, fac in events])

    def _replace_dividend_details(self, conn: sqlite3.Connection, code: str, event_date: str, items: List[Tuple[str, str]]) -> None:
        with conn:
            conn.execute("DELETE FROM corporate_actions WHERE code = ? AND event_date = ? AND action_type = 'dividend'", (code, event_date))
            conn.executemany("INSERT INTO corporate_actions(code, event_date, action_type, item, value, fetched_at) VALUES (?, ?, 'dividend', ?, ?, ?)",
                             [(code, event_date, item, val, Utils.now_ts()) for item, val in items])

    def _replace_intraday_bars(self, conn: sqlite3.Connection, code: str, intra_df: pd.DataFrame) -> int:
        if intra_df.empty: return 0
        trade_dates = sorted({str(v) for v in intra_df["trade_date"].dropna()})
        rows = [(code, r.trade_date, r.trade_time, r.trade_timestamp, r.open, r.close, r.high, r.low, r.avg_price, r.volume, r.amount, r.change_pct, r.change_amount, Utils.now_ts()) for r in intra_df.itertuples(index=False)]
        with conn:
            for td in trade_dates:
                conn.execute("DELETE FROM intraday_bars WHERE code = ? AND trade_date = ?", (code, td))
            conn.executemany("""
                INSERT INTO intraday_bars(code, trade_date, trade_time, trade_timestamp, open, close, high, low, avg_price, volume, amount, change_pct, change_amount, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
        return len(rows)

    def _replace_daily_distributions(self, conn: sqlite3.Connection, code: str, distributions: List[dict]) -> int:
        if not distributions: return 0
        ts = Utils.now_ts()
        with conn:
            conn.executemany("DELETE FROM daily_price_distributions WHERE code = ? AND trade_date = ?", [(code, i["date"]) for i in distributions])
            conn.executemany("""
                INSERT INTO daily_price_distributions(code, trade_date, summary_json, buy_sell_bins_json, price_histogram_json, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [(code, i["date"], json.dumps(i["summary"], ensure_ascii=False), json.dumps(i["buy_sell_bins"], ensure_ascii=False), json.dumps(i["price_histogram"], ensure_ascii=False), ts) for i in distributions])
        return len(distributions)

    def _merge_daily_spot(self, trade_date: str, bars_df: pd.DataFrame, spot: dict) -> pd.DataFrame:
        data = bars_df.copy()
        amount = float(spot["amount_wan"]) * 10000.0 if spot.get("amount_wan") is not None else None
        row_payload = {
            "date": trade_date, "open": spot.get("open"), "high": spot.get("high"),
            "low": spot.get("low"), "close": spot.get("price"), "volume": None, "amount": amount,
        }
        if data.empty:
            return pd.DataFrame([row_payload])
        mask = data["date"] == trade_date
        if mask.any():
            for col, val in row_payload.items():
                if col != "date" and val is not None:
                    data.loc[mask, col] = val
            return data
        data = pd.concat([data, pd.DataFrame([row_payload])], ignore_index=True)
        return data.sort_values("date").drop_duplicates(subset=["date"], keep="last")

    def _build_daily_distributions(self, intra_df: pd.DataFrame, bin_count: int = 8) -> List[dict]:
        dists = []
        for trade_date, group in intra_df.groupby("trade_date"):
            valid = group.dropna(subset=["close", "volume"]).copy()
            if valid.empty: continue

            prices = valid["close"].astype(float).tolist()
            low_p, high_p = min(prices), max(prices)
            effective_bins = 1 if high_p <= low_p else bin_count
            step = max((high_p - low_p) / effective_bins, 0.01)

            buy_sell_bins, price_hist = [], []
            for i in range(effective_bins):
                lp = low_p + i * step
                up = high_p if i == effective_bins - 1 else lp + step
                lbl = f"{lp:.2f}-{up:.2f}"
                buy_sell_bins.append({"label": lbl, "lower_price": round(lp, 4), "upper_price": round(up, 4), "buy_volume": 0.0, "sell_volume": 0.0, "neutral_volume": 0.0, "total_volume": 0.0})
                price_hist.append({"label": lbl, "lower_price": round(lp, 4), "upper_price": round(up, 4), "count": 0, "volume": 0.0})

            for row in valid.to_dict("records"):
                price, vol = float(row["close"]), float(row.get("volume") or 0)
                b_idx = 0 if effective_bins == 1 else min(int((price - low_p) / step), effective_bins - 1)
                c_o, c_c = row.get("open"), row.get("close")
                
                if c_o is not None and c_c is not None and c_c > c_o: buy_sell_bins[b_idx]["buy_volume"] += vol
                elif c_o is not None and c_c is not None and c_c < c_o: buy_sell_bins[b_idx]["sell_volume"] += vol
                else: buy_sell_bins[b_idx]["neutral_volume"] += vol
                
                buy_sell_bins[b_idx]["total_volume"] += vol
                price_hist[b_idx]["count"] += 1
                price_hist[b_idx]["volume"] += vol
                
            dists.append({
                "date": str(trade_date),
                "summary": {
                    "open": Utils.safe_float(valid.iloc[0].get("open")),
                    "close": Utils.safe_float(valid.iloc[-1].get("close")),
                    "high": round(high_p, 4), "low": round(low_p, 4),
                    "total_volume": round(float(valid["volume"].fillna(0).sum()), 4),
                    "total_amount": round(float(valid["amount"].fillna(0).sum()), 4)
                },
                "buy_sell_bins": buy_sell_bins, "price_histogram": price_hist
            })
        dists.sort(key=lambda i: i["date"])
        return dists


class StockMarketDataReader:
    """提供给下游查询和组装结构化前端市场数据载荷的读取器"""
    
    _market_cache_lock = threading.Lock()
    _market_cache: Dict[str, dict] = {}
    _trade_day_cache_lock = threading.Lock()
    _trade_day_cache: Optional[set[str]] = None

    @classmethod
    def get_empty_payload(cls) -> Dict[str, object]:
        return {"daily_series": [], "candle_windows": {"day": [], "five_day": [], "twenty_day": []}, "intraday_series": [], "daily_distributions": [], "warnings": []}

    @classmethod
    def clear_cache(cls, code: str):
        with cls._market_cache_lock:
            cls._market_cache.pop(code, None)

    @classmethod
    def is_trade_day(cls, value: Optional[str] = None) -> bool:
        trade_date, _ = Utils.parse_sync_date(value)
        with cls._trade_day_cache_lock:
            if cls._trade_day_cache is None:
                cls._trade_day_cache = MarketDataFetcher.fetch_trade_days()
        if cls._trade_day_cache:
            return trade_date in cls._trade_day_cache
        return datetime.strptime(trade_date, "%Y-%m-%d").weekday() < 5

    @staticmethod
    def has_market_data_for_date(value: Optional[str] = None) -> dict:
        trade_date, _ = Utils.parse_sync_date(value)
        conn = DbManager.get_connection()
        try:
            db_vars = {
                "daily_bar_count": int(conn.execute("SELECT COUNT(*) FROM daily_bars WHERE trade_date = ? AND adjust_type = 'none'", (trade_date,)).fetchone()[0]),
                "intraday_bar_count": int(conn.execute("SELECT COUNT(*) FROM intraday_bars WHERE trade_date = ?", (trade_date,)).fetchone()[0]),
                "distribution_count": int(conn.execute("SELECT COUNT(*) FROM daily_price_distributions WHERE trade_date = ?", (trade_date,)).fetchone()[0])
            }
            return {
                "trade_date": trade_date, "db_path": str(DB_PATH),
                **db_vars, "is_ready": db_vars["daily_bar_count"] > 0 and db_vars["intraday_bar_count"] > 0
            }
        finally:
            conn.close()

    @classmethod
    def build_stock_market_payload(cls, stock_info: dict) -> dict:
        code = str(stock_info["code"])
        
        with cls._market_cache_lock:
            entry = cls._market_cache.get(code)
            if entry and time.time() - entry["timestamp"] <= MARKET_CACHE_TTL_SECONDS:
                return copy.deepcopy(entry["payload"])

        payload = cls.get_empty_payload()
        conn = DbManager.get_connection()
        try:
            payload["daily_series"] = cls._load_daily_series(conn, code)
            payload["candle_windows"]["day"] = payload["daily_series"][-1:]
            payload["candle_windows"]["five_day"] = payload["daily_series"][-5:]
            payload["candle_windows"]["twenty_day"] = payload["daily_series"][-20:]
            payload["intraday_series"] = cls._load_recent_intraday_series(conn, code)
            payload["daily_distributions"] = cls._load_daily_distributions(conn, code)
        finally:
            conn.close()

        if not payload["daily_series"]: payload["warnings"].append("数据库中没有该股票的日线行情")
        if not payload["intraday_series"]: payload["warnings"].append("数据库中没有该股票最近分时行情")
        if not payload["daily_distributions"]: payload["warnings"].append("数据库中没有该股票的价格分布数据")

        with cls._market_cache_lock:
            cls._market_cache[code] = {"timestamp": time.time(), "payload": copy.deepcopy(payload)}
        return payload

    @staticmethod
    def _load_daily_series(conn: sqlite3.Connection, code: str, limit: int = 240) -> List[dict]:
        rows = conn.execute("SELECT trade_date AS date, open, close, high, low, volume, amount FROM daily_bars WHERE code = ? AND adjust_type = 'none' ORDER BY trade_date DESC LIMIT ?", (code, limit)).fetchall()
        return [{"date": str(r["date"]), "open": Utils.safe_float(r["open"]), "close": Utils.safe_float(r["close"]), "high": Utils.safe_float(r["high"]), "low": Utils.safe_float(r["low"]), "volume": Utils.safe_float(r["volume"]), "amount": Utils.safe_float(r["amount"]), "amplitude": None, "change_pct": None, "change_amount": None, "turnover_rate": None} for r in reversed(rows)]

    @staticmethod
    def _load_recent_intraday_series(conn: sqlite3.Connection, code: str, lookback_days: int = INTRADAY_LOOKBACK_DAYS) -> List[dict]:
        date_rows = conn.execute("SELECT DISTINCT trade_date FROM intraday_bars WHERE code = ? ORDER BY trade_date DESC LIMIT ?", (code, lookback_days)).fetchall()
        trade_dates = [str(r["trade_date"]) for r in reversed(date_rows)]
        if not trade_dates: return []
        rows = conn.execute(f"SELECT trade_timestamp, trade_date, trade_time, open, close, high, low, avg_price, volume, amount, change_pct, change_amount FROM intraday_bars WHERE code = ? AND trade_date IN ({','.join('?'*len(trade_dates))}) ORDER BY trade_timestamp", [code, *trade_dates]).fetchall()
        return [{"timestamp": str(r["trade_timestamp"]), "date": str(r["trade_date"]), "time": str(r["trade_time"])[:5], "open": Utils.safe_float(r["open"]), "close": Utils.safe_float(r["close"]), "high": Utils.safe_float(r["high"]), "low": Utils.safe_float(r["low"]), "avg_price": Utils.safe_float(r["avg_price"]), "volume": Utils.safe_float(r["volume"]), "amount": Utils.safe_float(r["amount"]), "change_pct": Utils.safe_float(r["change_pct"]), "change_amount": Utils.safe_float(r["change_amount"])} for r in rows]

    @staticmethod
    def _load_daily_distributions(conn: sqlite3.Connection, code: str, lookback_days: int = INTRADAY_LOOKBACK_DAYS) -> List[dict]:
        rows = conn.execute("SELECT trade_date, summary_json, buy_sell_bins_json, price_histogram_json FROM daily_price_distributions WHERE code = ? ORDER BY trade_date DESC LIMIT ?", (code, lookback_days)).fetchall()
        return [{"date": str(r["trade_date"]), "summary": json.loads(r["summary_json"]), "buy_sell_bins": json.loads(r["buy_sell_bins_json"]), "price_histogram": json.loads(r["price_histogram_json"])} for r in reversed(rows)]


def main() -> None:
    parser = argparse.ArgumentParser(description="同步 A 股行情到 SQLite")
    parser.add_argument("--date", type=str, default=None, help="同步日期，格式 YYYYMMDD 或 YYYY-MM-DD")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental", help="incremental 仅拉当日，full 全量刷新到当日")
    parser.add_argument("--limit", type=int, default=0, help="仅同步前 N 只股票，便于调试")
    parser.add_argument("--force-full-adjust", action="store_true", help="强制所有股票重刷前复权历史")
    args = parser.parse_args()

    engine = StockMarketSyncEngine(
        mode=args.mode,
        limit=args.limit,
        force_full_adjust=args.force_full_adjust
    )
    
    summary = engine.run_sync(review_date=args.date)

    print(
        f"同步完成[{summary['mode']}]: 股票 {summary['synced_codes']} 只, 当日现货补齐 {summary['spot_enriched_codes']} 只, "
        f"日线 {summary['raw_bar_rows']} 行, 前复权 {summary['qfq_bar_rows']} 行, "
        f"分时 {summary['intraday_bar_rows']} 行, 分布 {summary['distribution_rows']} 条"
    )
    if summary["full_refresh_codes"]:
        print(f"触发复权重刷: {', '.join(summary['full_refresh_codes'][:20])}")
    if summary["errors"]:
        print("异常:")
        for item in summary["errors"][:20]:
            print(f"  - {item}")


if __name__ == "__main__":
    main()
