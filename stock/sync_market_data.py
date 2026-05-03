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
import importlib
import json
import sqlite3
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

ak = None

MARKET_CACHE_TTL_SECONDS = 10 * 60
FULL_REFRESH_START = "19900101"
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

SCHEMA_MIGRATIONS = {
    "daily_bars": {
        "float_mv_yi": "REAL",
    },
}

_market_cache_lock = threading.Lock()
_market_cache: Dict[str, Dict[str, object]] = {}
_trade_day_cache_lock = threading.Lock()
_trade_day_cache: set[str] | None = None


def empty_market_payload() -> Dict[str, object]:
    return {
        "daily_series": [],
        "candle_windows": {"day": [], "five_day": [], "twenty_day": []},
        "intraday_series": [],
        "daily_distributions": [],
        "warnings": [],
    }


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ensure_schema_migrations(conn: sqlite3.Connection) -> None:
    for table_name, columns in SCHEMA_MIGRATIONS.items():
        existing_columns = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        for column_name, column_type in columns.items():
            if column_name in existing_columns:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
    conn.commit()


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    ensure_schema_migrations(conn)
    return conn


def ensure_akshare_available() -> None:
    global ak
    if ak is None:
        try:
            ak = importlib.import_module("akshare")
        except ImportError as exc:
            raise RuntimeError("akshare 未安装，无法执行行情同步") from exc


def parse_sync_date(value: str | None) -> tuple[str, str]:
    if not value:
        dt = datetime.now()
    else:
        raw = value.strip()
        if len(raw) == 8 and raw.isdigit():
            dt = datetime.strptime(raw, "%Y%m%d")
        else:
            dt = datetime.strptime(raw, "%Y-%m-%d")
    return dt.strftime("%Y-%m-%d"), dt.strftime("%Y%m%d")


def normalize_trade_date(value: object) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def classify_board(code: str) -> str:
    for name, check in BOARD_CATEGORIES.items():
        if check(code):
            return name
    return "其他"


def is_st(name: str) -> bool:
    if not isinstance(name, str):
        return True
    upper_name = name.upper()
    return "ST" in upper_name or "*ST" in name


def to_symbol(code: str) -> str:
    if code.startswith(("6", "688")):
        return f"sh{code}"
    return f"sz{code}"


def _safe_float(value):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return round(number, 4)


def _retry_akshare_call(fetcher, *, label: str, attempts: int = 3, delay_seconds: float = 1.2):
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


def _get_cached_market_payload(symbol: str, allow_stale: bool = False):
    with _market_cache_lock:
        entry = _market_cache.get(symbol)
        if not entry:
            return None
        age = time.time() - float(entry["timestamp"])
        if not allow_stale and age > MARKET_CACHE_TTL_SECONDS:
            return None
        payload = copy.deepcopy(entry["payload"])
        if allow_stale and age > MARKET_CACHE_TTL_SECONDS:
            payload["warnings"] = [
                *payload.get("warnings", []),
                f"当前展示的是 {int(age // 60)} 分钟前的缓存行情数据",
            ]
        return payload


def _set_cached_market_payload(symbol: str, payload) -> None:
    with _market_cache_lock:
        _market_cache[symbol] = {
            "timestamp": time.time(),
            "payload": copy.deepcopy(payload),
        }


def _clear_market_cache_for_code(code: str) -> None:
    with _market_cache_lock:
        _market_cache.pop(str(code), None)


def get_all_stock_codes() -> pd.DataFrame:
    ensure_akshare_available()
    df = ak.stock_info_a_code_name()
    df["board"] = df["code"].apply(classify_board)
    df["is_st"] = df["name"].apply(is_st).astype(int)
    return df[["code", "name", "board", "is_st"]].copy()


def fetch_tencent_realtime(codes: list[str]) -> dict[str, dict]:
    result: dict[str, dict] = {}
    tencent_codes = [to_symbol(code) for code in codes]
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
                code = parts[2]
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
                result[code] = {
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


def normalize_daily_bars_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount", "float_mv_yi"])
    if "date" not in df.columns:
        df = df.reset_index()
        if "date" not in df.columns and "index" in df.columns:
            df = df.rename(columns={"index": "date"})
    rename_map = {"vol": "volume", "成交量": "volume", "成交额": "amount"}
    df = df.rename(columns=rename_map)
    needed_columns = ["date", "open", "high", "low", "close", "volume", "amount"]
    missing = [column for column in needed_columns if column not in df.columns]
    if missing:
        raise ValueError(f"K line data missing columns: {missing}")
    data = df[needed_columns].copy()
    data["float_mv_yi"] = None
    data["date"] = data["date"].apply(normalize_trade_date)
    for column in ["open", "high", "low", "close", "volume", "amount"]:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return data


def merge_daily_spot_snapshot(
    code: str,
    trade_date: str,
    bars_df: pd.DataFrame,
    spot: dict | None,
) -> pd.DataFrame:
    if spot is None:
        return bars_df

    data = bars_df.copy()
    amount = float(spot["amount_wan"]) * 10000.0 if spot.get("amount_wan") is not None else None
    row_payload = {
        "date": trade_date,
        "open": spot.get("open"),
        "high": spot.get("high"),
        "low": spot.get("low"),
        "close": spot.get("price"),
        "volume": None,
        "amount": amount,
        "float_mv_yi": spot.get("float_mv_yi"),
    }

    if data.empty:
        return pd.DataFrame([row_payload])

    mask = data["date"] == trade_date
    if mask.any():
        for column, value in row_payload.items():
            if column == "date" or value is None:
                continue
            data.loc[mask, column] = value
        if row_payload["float_mv_yi"] is not None:
            data.loc[mask, "float_mv_yi"] = row_payload["float_mv_yi"]
        return data

    data = pd.concat([data, pd.DataFrame([row_payload])], ignore_index=True)
    data = data.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return data


def fetch_daily_bars(code: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
    ensure_akshare_available()
    df = ak.stock_zh_a_daily(symbol=to_symbol(code), start_date=start_date, end_date=end_date, adjust=adjust)
    return normalize_daily_bars_df(df)


def fetch_qfq_factor_events(code: str) -> list[tuple[str, float]]:
    ensure_akshare_available()
    df = ak.stock_zh_a_daily(symbol=to_symbol(code), adjust="qfq-factor")
    if df is None or df.empty:
        return []
    data = df.copy()
    data["date"] = data["date"].apply(normalize_trade_date)
    data["qfq_factor"] = pd.to_numeric(data["qfq_factor"], errors="coerce")
    data = data.dropna(subset=["date", "qfq_factor"]).sort_values("date")
    return [(row["date"], round(float(row["qfq_factor"]), 12)) for _, row in data.iterrows()]


def fetch_dividend_detail(code: str, event_date: str) -> list[tuple[str, str]]:
    ensure_akshare_available()
    try:
        df = ak.stock_history_dividend_detail(symbol=code, indicator="分红", date=event_date)
    except Exception:
        return []
    if df is None or df.empty:
        return []
    items: list[tuple[str, str]] = []
    for _, row in df.iterrows():
        item = str(row.get("item", "")).strip()
        value = str(row.get("value", "")).strip()
        if not item:
            continue
        items.append((item, value))
    return items


def normalize_intraday_df(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "trade_timestamp",
        "trade_date",
        "trade_time",
        "open",
        "close",
        "high",
        "low",
        "avg_price",
        "volume",
        "amount",
        "change_pct",
        "change_amount",
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)

    rename_map = {
        "时间": "trade_timestamp",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "均价": "avg_price",
        "成交量": "volume",
        "成交额": "amount",
        "涨跌幅": "change_pct",
        "涨跌额": "change_amount",
    }
    data = df.rename(columns=rename_map).copy()
    missing = [column for column in ["trade_timestamp", "open", "close", "high", "low", "volume", "amount"] if column not in data.columns]
    if missing:
        raise ValueError(f"Intraday data missing columns: {missing}")

    data["trade_timestamp"] = pd.to_datetime(data["trade_timestamp"], errors="coerce")
    data = data.dropna(subset=["trade_timestamp"])
    data["trade_date"] = data["trade_timestamp"].dt.strftime("%Y-%m-%d")
    data["trade_time"] = data["trade_timestamp"].dt.strftime("%H:%M:%S")
    data["trade_timestamp"] = data["trade_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

    for column in ["open", "close", "high", "low", "avg_price", "volume", "amount", "change_pct", "change_amount"]:
        if column in data.columns:
            data[column] = pd.to_numeric(data[column], errors="coerce")
        else:
            data[column] = None

    data = data[columns].sort_values("trade_timestamp").drop_duplicates(subset=["trade_timestamp"], keep="last")
    return data


def fetch_intraday_bars(code: str, start_datetime: str, end_datetime: str) -> pd.DataFrame:
    ensure_akshare_available()
    df = _retry_akshare_call(
        lambda: ak.stock_zh_a_hist_min_em(
            symbol=code,
            start_date=start_datetime,
            end_date=end_datetime,
            period="1",
            adjust="",
        ),
        label=f"{code} 分时行情获取",
    )
    return normalize_intraday_df(df)


def upsert_stocks(conn: sqlite3.Connection, stocks_df: pd.DataFrame) -> int:
    rows = [
        (row.code, row.name, row.board, int(row.is_st), now_ts())
        for row in stocks_df.itertuples(index=False)
    ]
    conn.executemany(
        """
        INSERT INTO stocks(code, name, board, is_st, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
            name = excluded.name,
            board = excluded.board,
            is_st = excluded.is_st,
            updated_at = excluded.updated_at
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def upsert_daily_bars(conn: sqlite3.Connection, code: str, adjust_type: str, bars_df: pd.DataFrame) -> int:
    if bars_df.empty:
        return 0
    fetched_at = now_ts()
    rows = [
        (
            code,
            row.date,
            adjust_type,
            row.open,
            row.high,
            row.low,
            row.close,
            row.volume,
            row.amount,
            row.float_mv_yi if hasattr(row, "float_mv_yi") else None,
            fetched_at,
        )
        for row in bars_df.itertuples(index=False)
    ]
    conn.executemany(
        """
        INSERT INTO daily_bars(code, trade_date, adjust_type, open, high, low, close, volume, amount, float_mv_yi, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code, trade_date, adjust_type) DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            amount = excluded.amount,
            float_mv_yi = COALESCE(excluded.float_mv_yi, daily_bars.float_mv_yi),
            fetched_at = excluded.fetched_at
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def replace_qfq_history(conn: sqlite3.Connection, code: str, bars_df: pd.DataFrame) -> int:
    if bars_df.empty:
        return 0
    fetched_at = now_ts()
    rows = [
        (code, row.date, "qfq", row.open, row.high, row.low, row.close, row.volume, row.amount, None, fetched_at)
        for row in bars_df.itertuples(index=False)
    ]
    with conn:
        conn.execute("DELETE FROM daily_bars WHERE code = ? AND adjust_type = 'qfq'", (code,))
        conn.executemany(
            """
            INSERT INTO daily_bars(code, trade_date, adjust_type, open, high, low, close, volume, amount, float_mv_yi, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def get_existing_adjustment_events(conn: sqlite3.Connection, code: str) -> list[tuple[str, float]]:
    rows = conn.execute(
        "SELECT event_date, qfq_factor FROM adjustment_events WHERE code = ? ORDER BY event_date",
        (code,),
    ).fetchall()
    return [(row["event_date"], round(float(row["qfq_factor"]), 12)) for row in rows]


def replace_adjustment_events(conn: sqlite3.Connection, code: str, events: list[tuple[str, float]]) -> None:
    fetched_at = now_ts()
    with conn:
        conn.execute("DELETE FROM adjustment_events WHERE code = ?", (code,))
        conn.executemany(
            "INSERT INTO adjustment_events(code, event_date, qfq_factor, fetched_at) VALUES (?, ?, ?, ?)",
            [(code, event_date, factor, fetched_at) for event_date, factor in events],
        )


def replace_dividend_details(conn: sqlite3.Connection, code: str, event_date: str, items: list[tuple[str, str]]) -> None:
    fetched_at = now_ts()
    with conn:
        conn.execute(
            "DELETE FROM corporate_actions WHERE code = ? AND event_date = ? AND action_type = 'dividend'",
            (code, event_date),
        )
        conn.executemany(
            "INSERT INTO corporate_actions(code, event_date, action_type, item, value, fetched_at) VALUES (?, ?, 'dividend', ?, ?, ?)",
            [(code, event_date, item, value, fetched_at) for item, value in items],
        )


def build_daily_distributions(intraday_df: pd.DataFrame, bin_count: int = 8) -> list[dict]:
    distributions: list[dict] = []
    for trade_date, group in intraday_df.groupby("trade_date"):
        valid = group.dropna(subset=["close", "volume"]).copy()
        if valid.empty:
            continue

        prices = valid["close"].astype(float).tolist()
        low_price = min(prices)
        high_price = max(prices)
        effective_bin_count = 1 if high_price <= low_price else bin_count
        step = max((high_price - low_price) / effective_bin_count, 0.01)

        buy_sell_bins = []
        price_histogram = []
        for index in range(effective_bin_count):
            lower_price = low_price + index * step
            upper_price = high_price if index == effective_bin_count - 1 else lower_price + step
            label = f"{lower_price:.2f}-{upper_price:.2f}"
            buy_sell_bins.append(
                {
                    "label": label,
                    "lower_price": round(lower_price, 4),
                    "upper_price": round(upper_price, 4),
                    "buy_volume": 0.0,
                    "sell_volume": 0.0,
                    "neutral_volume": 0.0,
                    "total_volume": 0.0,
                }
            )
            price_histogram.append(
                {
                    "label": label,
                    "lower_price": round(lower_price, 4),
                    "upper_price": round(upper_price, 4),
                    "count": 0,
                    "volume": 0.0,
                }
            )

        for row in valid.to_dict("records"):
            price = float(row["close"])
            volume = float(row.get("volume") or 0)
            bucket_index = 0 if effective_bin_count == 1 else min(int((price - low_price) / step), effective_bin_count - 1)
            candle_open = row.get("open")
            candle_close = row.get("close")
            if candle_open is not None and candle_close is not None and candle_close > candle_open:
                buy_sell_bins[bucket_index]["buy_volume"] += volume
            elif candle_open is not None and candle_close is not None and candle_close < candle_open:
                buy_sell_bins[bucket_index]["sell_volume"] += volume
            else:
                buy_sell_bins[bucket_index]["neutral_volume"] += volume
            buy_sell_bins[bucket_index]["total_volume"] += volume
            price_histogram[bucket_index]["count"] += 1
            price_histogram[bucket_index]["volume"] += volume

        total_volume = float(valid["volume"].fillna(0).sum())
        total_amount = float(valid["amount"].fillna(0).sum())
        distributions.append(
            {
                "date": str(trade_date),
                "summary": {
                    "open": _safe_float(valid.iloc[0].get("open")),
                    "close": _safe_float(valid.iloc[-1].get("close")),
                    "high": round(high_price, 4),
                    "low": round(low_price, 4),
                    "total_volume": round(total_volume, 4),
                    "total_amount": round(total_amount, 4),
                },
                "buy_sell_bins": buy_sell_bins,
                "price_histogram": price_histogram,
            }
        )
    distributions.sort(key=lambda item: item["date"])
    return distributions


def replace_intraday_bars(conn: sqlite3.Connection, code: str, intraday_df: pd.DataFrame) -> int:
    if intraday_df.empty:
        return 0
    fetched_at = now_ts()
    trade_dates = sorted({str(value) for value in intraday_df["trade_date"].dropna().tolist()})
    rows = [
        (
            code,
            row.trade_date,
            row.trade_time,
            row.trade_timestamp,
            row.open,
            row.close,
            row.high,
            row.low,
            row.avg_price,
            row.volume,
            row.amount,
            row.change_pct,
            row.change_amount,
            fetched_at,
        )
        for row in intraday_df.itertuples(index=False)
    ]
    with conn:
        for trade_date in trade_dates:
            conn.execute("DELETE FROM intraday_bars WHERE code = ? AND trade_date = ?", (code, trade_date))
        conn.executemany(
            """
            INSERT INTO intraday_bars(
                code, trade_date, trade_time, trade_timestamp, open, close, high, low,
                avg_price, volume, amount, change_pct, change_amount, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return len(rows)


def replace_daily_distributions(conn: sqlite3.Connection, code: str, distributions: list[dict]) -> int:
    if not distributions:
        return 0
    fetched_at = now_ts()
    with conn:
        conn.executemany(
            "DELETE FROM daily_price_distributions WHERE code = ? AND trade_date = ?",
            [(code, item["date"]) for item in distributions],
        )
        conn.executemany(
            """
            INSERT INTO daily_price_distributions(
                code, trade_date, summary_json, buy_sell_bins_json, price_histogram_json, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    code,
                    item["date"],
                    json.dumps(item["summary"], ensure_ascii=False),
                    json.dumps(item["buy_sell_bins"], ensure_ascii=False),
                    json.dumps(item["price_histogram"], ensure_ascii=False),
                    fetched_at,
                )
                for item in distributions
            ],
        )
    return len(distributions)


def _load_trade_day_cache() -> set[str]:
    global _trade_day_cache
    with _trade_day_cache_lock:
        if _trade_day_cache is not None:
            return _trade_day_cache
        try:
            ensure_akshare_available()
            df = _retry_akshare_call(lambda: ak.tool_trade_date_hist_sina(), label="交易日历获取", attempts=2, delay_seconds=0.8)
        except Exception:
            _trade_day_cache = set()
            return _trade_day_cache
        if df is None or df.empty:
            _trade_day_cache = set()
            return _trade_day_cache
        date_column = "trade_date" if "trade_date" in df.columns else df.columns[0]
        _trade_day_cache = {pd.Timestamp(value).strftime("%Y-%m-%d") for value in df[date_column].tolist()}
        return _trade_day_cache


def is_trade_day(value: str | None = None) -> bool:
    trade_date, _ = parse_sync_date(value)
    cached_days = _load_trade_day_cache()
    if cached_days:
        return trade_date in cached_days
    dt = datetime.strptime(trade_date, "%Y-%m-%d")
    return dt.weekday() < 5


def has_market_data_for_date(value: str | None = None) -> dict:
    trade_date, _ = parse_sync_date(value)
    conn = get_db_connection()
    try:
        daily_bar_count = int(conn.execute("SELECT COUNT(*) FROM daily_bars WHERE trade_date = ? AND adjust_type = 'none'", (trade_date,)).fetchone()[0])
        intraday_bar_count = int(conn.execute("SELECT COUNT(*) FROM intraday_bars WHERE trade_date = ?", (trade_date,)).fetchone()[0])
        distribution_count = int(conn.execute("SELECT COUNT(*) FROM daily_price_distributions WHERE trade_date = ?", (trade_date,)).fetchone()[0])
        return {
            "trade_date": trade_date,
            "db_path": str(DB_PATH),
            "daily_bar_count": daily_bar_count,
            "intraday_bar_count": intraday_bar_count,
            "distribution_count": distribution_count,
            "is_ready": daily_bar_count > 0 and intraday_bar_count > 0,
        }
    finally:
        conn.close()


def run_sync(
    review_date: str | None = None,
    mode: str = "incremental",
    limit: int = 0,
    force_full_adjust: bool = False,
    request_pause: float = 0.15,
) -> dict:
    trade_date, compact_date = parse_sync_date(review_date)
    end_date = compact_date
    is_full_refresh = mode == "full"
    daily_start_date = FULL_REFRESH_START if is_full_refresh else compact_date
    intraday_start = (
        (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=INTRADAY_FETCH_WINDOW_DAYS)).strftime("%Y-%m-%d 09:30:00")
        if is_full_refresh
        else f"{trade_date} 09:30:00"
    )
    intraday_end = (
        (datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d 15:00:00")
        if is_full_refresh
        else f"{trade_date} 15:00:00"
    )

    conn = get_db_connection()
    summary = {
        "review_date": trade_date,
        "db_path": str(DB_PATH),
        "mode": mode,
        "universe_count": 0,
        "synced_codes": 0,
        "raw_bar_rows": 0,
        "qfq_bar_rows": 0,
        "intraday_bar_rows": 0,
        "distribution_rows": 0,
        "spot_enriched_codes": 0,
        "full_refresh_codes": [],
        "adjustment_event_updates": 0,
        "errors": [],
    }

    stocks_df = get_all_stock_codes()
    summary["universe_count"] = upsert_stocks(conn, stocks_df)
    sync_df = stocks_df.copy()
    if limit > 0:
        sync_df = sync_df.head(limit).copy()

    tencent_spot = fetch_tencent_realtime(sync_df["code"].tolist()) if trade_date == datetime.now().strftime("%Y-%m-%d") else {}

    for row in sync_df.itertuples(index=False):
        code = row.code
        try:
            api_events = fetch_qfq_factor_events(code)
            db_events = get_existing_adjustment_events(conn, code)
            new_event_dates = sorted(set(event_date for event_date, _ in api_events) - set(event_date for event_date, _ in db_events))
            needs_full_refresh = is_full_refresh or force_full_adjust or api_events != db_events

            if api_events and api_events != db_events:
                replace_adjustment_events(conn, code, api_events)
                summary["adjustment_event_updates"] += 1

            for event_date in new_event_dates:
                detail_items = fetch_dividend_detail(code, event_date)
                if detail_items:
                    replace_dividend_details(conn, code, event_date, detail_items)

            raw_bars = fetch_daily_bars(code, start_date=daily_start_date, end_date=end_date, adjust="")
            raw_bars = merge_daily_spot_snapshot(code, trade_date, raw_bars, tencent_spot.get(code))
            summary["raw_bar_rows"] += upsert_daily_bars(conn, code, "none", raw_bars)
            if tencent_spot.get(code):
                summary["spot_enriched_codes"] += 1

            if needs_full_refresh:
                qfq_bars = fetch_daily_bars(code, start_date=FULL_REFRESH_START, end_date=end_date, adjust="qfq")
                summary["qfq_bar_rows"] += replace_qfq_history(conn, code, qfq_bars)
                summary["full_refresh_codes"].append(code)
            else:
                qfq_bars = fetch_daily_bars(code, start_date=daily_start_date, end_date=end_date, adjust="qfq")
                summary["qfq_bar_rows"] += upsert_daily_bars(conn, code, "qfq", qfq_bars)

            intraday_df = fetch_intraday_bars(code, intraday_start, intraday_end)
            if not intraday_df.empty:
                summary["intraday_bar_rows"] += replace_intraday_bars(conn, code, intraday_df)
                recent_intraday_df = intraday_df[intraday_df["trade_date"].isin(sorted(intraday_df["trade_date"].unique())[-INTRADAY_LOOKBACK_DAYS:])]
                distributions = build_daily_distributions(recent_intraday_df)
                summary["distribution_rows"] += replace_daily_distributions(conn, code, distributions)

            summary["synced_codes"] += 1
            _clear_market_cache_for_code(code)
        except Exception as exc:
            summary["errors"].append(f"{code}: {exc}")
        time.sleep(request_pause)

    conn.close()
    return summary


def _load_daily_series(conn: sqlite3.Connection, code: str, limit: int = 240) -> list[dict]:
    rows = conn.execute(
        """
        SELECT trade_date AS date, open, close, high, low, volume, amount
        FROM daily_bars
        WHERE code = ? AND adjust_type = 'none'
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (code, limit),
    ).fetchall()
    result = []
    for row in reversed(rows):
        result.append(
            {
                "date": str(row["date"]),
                "open": _safe_float(row["open"]),
                "close": _safe_float(row["close"]),
                "high": _safe_float(row["high"]),
                "low": _safe_float(row["low"]),
                "volume": _safe_float(row["volume"]),
                "amount": _safe_float(row["amount"]),
                "amplitude": None,
                "change_pct": None,
                "change_amount": None,
                "turnover_rate": None,
            }
        )
    return result


def _load_recent_intraday_series(conn: sqlite3.Connection, code: str, lookback_days: int = INTRADAY_LOOKBACK_DAYS) -> list[dict]:
    date_rows = conn.execute(
        """
        SELECT DISTINCT trade_date
        FROM intraday_bars
        WHERE code = ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (code, lookback_days),
    ).fetchall()
    trade_dates = [str(row["trade_date"]) for row in reversed(date_rows)]
    if not trade_dates:
        return []
    placeholders = ",".join("?" for _ in trade_dates)
    rows = conn.execute(
        f"""
        SELECT trade_timestamp, trade_date, trade_time, open, close, high, low, avg_price, volume, amount, change_pct, change_amount
        FROM intraday_bars
        WHERE code = ? AND trade_date IN ({placeholders})
        ORDER BY trade_timestamp
        """,
        [code, *trade_dates],
    ).fetchall()
    return [
        {
            "timestamp": str(row["trade_timestamp"]),
            "date": str(row["trade_date"]),
            "time": str(row["trade_time"])[:5],
            "open": _safe_float(row["open"]),
            "close": _safe_float(row["close"]),
            "high": _safe_float(row["high"]),
            "low": _safe_float(row["low"]),
            "avg_price": _safe_float(row["avg_price"]),
            "volume": _safe_float(row["volume"]),
            "amount": _safe_float(row["amount"]),
            "change_pct": _safe_float(row["change_pct"]),
            "change_amount": _safe_float(row["change_amount"]),
        }
        for row in rows
    ]


def _load_daily_distributions(conn: sqlite3.Connection, code: str, lookback_days: int = INTRADAY_LOOKBACK_DAYS) -> list[dict]:
    rows = conn.execute(
        """
        SELECT trade_date, summary_json, buy_sell_bins_json, price_histogram_json
        FROM daily_price_distributions
        WHERE code = ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (code, lookback_days),
    ).fetchall()
    result = []
    for row in reversed(rows):
        result.append(
            {
                "date": str(row["trade_date"]),
                "summary": json.loads(row["summary_json"]),
                "buy_sell_bins": json.loads(row["buy_sell_bins_json"]),
                "price_histogram": json.loads(row["price_histogram_json"]),
            }
        )
    return result


def build_stock_market_payload(stock_info):
    code = str(stock_info["code"])
    cached_payload = _get_cached_market_payload(symbol=code)
    if cached_payload:
        return cached_payload

    payload = empty_market_payload()
    conn = get_db_connection()
    try:
        payload["daily_series"] = _load_daily_series(conn, code)
        payload["candle_windows"]["day"] = payload["daily_series"][-1:]
        payload["candle_windows"]["five_day"] = payload["daily_series"][-5:]
        payload["candle_windows"]["twenty_day"] = payload["daily_series"][-20:]
        payload["intraday_series"] = _load_recent_intraday_series(conn, code)
        payload["daily_distributions"] = _load_daily_distributions(conn, code)
    finally:
        conn.close()

    if not payload["daily_series"]:
        payload["warnings"].append("数据库中没有该股票的日线行情")
    if not payload["intraday_series"]:
        payload["warnings"].append("数据库中没有该股票最近分时行情")
    if not payload["daily_distributions"]:
        payload["warnings"].append("数据库中没有该股票的价格分布数据")

    _set_cached_market_payload(symbol=code, payload=payload)
    return payload


def build_sync_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="同步 A 股行情到 SQLite")
    parser.add_argument("--date", type=str, default=None, help="同步日期，格式 YYYYMMDD 或 YYYY-MM-DD")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental", help="incremental 仅拉当日，full 全量刷新到当日")
    parser.add_argument("--limit", type=int, default=0, help="仅同步前 N 只股票，便于调试")
    parser.add_argument("--force-full-adjust", action="store_true", help="强制所有股票重刷前复权历史")
    return parser


def main() -> None:
    args = build_sync_arg_parser().parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    summary = run_sync(
        review_date=args.date,
        mode=args.mode,
        limit=args.limit,
        force_full_adjust=args.force_full_adjust,
    )
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