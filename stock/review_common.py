#!/usr/bin/env python3
"""股票每日复盘主入口，用于计算技术指标、打分排序并生成复盘推荐报告。

参数：
    --date: 复盘日期，格式 YYYYMMDD 或 YYYY-MM-DD，默认为最新交易日
    --top: Markdown 中展示的 TOP N，默认为 10
    --limit: 仅分析前 N 只候选股（用于调试），默认为 0（不限制）
    --all-boards: 包含创业板、科创板等全部板块，默认为只看主板
    --skip-sync: 跳过当日同步完整性检查，直接使用 SQLite 现有数据
    --force-run: 忽略启动时间和当日运行记录，强制执行复盘

用法：
    - 对指定日期进行增量数据检查并分析打分，生成复盘报告：
        python -m stock.review_common --date 2026-04-30
        
    - 数据已单独拉取完毕，直接使用数据库数据生成 2026-04-30 的全市场（含科创/创业板）推荐池，并且报告展示前 20 只：
        python -m stock.review_common --date 2026-04-30 --top 20 --skip-sync --all-boards

    - 自动触发启动检查（根据时间判断是否需要更新）：
        python -m stock.review_common
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import time
import traceback
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests


SIGNAL_WEIGHTS = {
    "golden_cross_first": 50,
    "golden_cross_second": 35,
    "golden_cross_high": 10,
    "second_cross_breakout_ready": 20,
    "volume_light": 5,
    "volume_breakout": 25,
    "volume_moderate": 15,
    "volume_strong": 35,
    "pullback_shrink_twice": 30,
    "ma_angle_strong": 20,
    "ma_angle_moderate": 10,
    "rsi_healthy": 10,
    "rsi_ok": 5,
    "bias_low": 10,
    "bias_ok": 5,
}

GROUP_SCORE_CAPS = {
    "trend_confirmation": 30,
    "position_quality": 10,
}

DEDUP_MIN_SIGNAL_SCORE = 85
SPACE_TO_HIGH_TIGHT_THRESHOLD = 0.10
SPACE_TO_HIGH_WARN_THRESHOLD = 0.20
SPACE_TO_HIGH_TIGHT_PENALTY = -15
SPACE_TO_HIGH_BREAKOUT_READY_PENALTY = -5
SPACE_TO_HIGH_WARN_PENALTY = -8
BREAKOUT_READY_MIN_VOLUME_RATIO = 1.8
BREAKOUT_READY_MAX_BIAS_RATIO = 0.07
BREAKOUT_READY_MAX_RSI = 65

MIN_FLOAT_MV = 20
MAX_FLOAT_MV = 100000000
MIN_DAILY_AMOUNT = 5000

MAX_BIAS_RATIO = 0.10
MAX_RSI = 75
VOLUME_BREAKOUT = 1.5
VOLUME_MODERATE = 1.0
VOLUME_WEAK = 0.7
SHRINKING_DOWN_LOOKBACK = 60
SHRINKING_DOWN_MIN_COUNT = 2
SHRINKING_DOWN_RATIO = 0.8
BREAKOUT_VOLUME_VS_PULLBACK_RATIO = 2.0

LOOKBACK_DAYS = 120
MA_SHORT = 5
MA_LONG = 20
SYNC_WINDOW_DAYS = 45
FULL_REFRESH_START = "19900101"
TENCENT_BATCH_SIZE = 50
AUTO_RUN_CUTOFF = dt_time(hour=15, minute=30)


BOARD_CATEGORIES = {
    "主板-沪（60）": lambda code: code.startswith("60"),
    "主板-深（00）": lambda code: code.startswith("000") or code.startswith("001"),
    "中小板（002/003）": lambda code: code.startswith("002") or code.startswith("003"),
    "创业板（300）": lambda code: code.startswith("30"),
    "科创板（688）": lambda code: code.startswith("688"),
    "北交所（8/4/9）": lambda code: code.startswith(("8", "4", "9")),
}


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REVIEW_DIR = DATA_DIR / "stock"
SCHEMA_PATH = DATA_DIR / "schema" / "stock.sql"
DB_PATH = DATA_DIR / "stock.db"
ALLOWED_BOARDS = ["主板-沪（60）", "主板-深（00）", "中小板（002/003）"]


SCHEMA_MIGRATIONS = {
    "latest_market_value": {
        "amount_wan": "REAL",
    },
}

MIN_LIQUIDITY_RATIO_PCT = 2.0


def get_db_connection() -> sqlite3.Connection:
    ensure_runtime_paths()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    conn.executescript(schema_sql)
    ensure_schema_migrations(conn)
    return conn


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_trade_date(value: object) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def parse_review_date(value: str | None) -> tuple[str, str]:
    if not value:
        dt = datetime.now()
    else:
        raw = value.strip()
        if len(raw) == 8 and raw.isdigit():
            dt = datetime.strptime(raw, "%Y%m%d")
        else:
            dt = datetime.strptime(raw, "%Y-%m-%d")
    return dt.strftime("%Y-%m-%d"), dt.strftime("%Y%m%d")


def ensure_runtime_paths() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)


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


def calc_liquidity_ratio_pct(amount_wan: object, float_mv_yi: object) -> float | None:
    try:
        amount_value = float(amount_wan)
        float_mv_value = float(float_mv_yi)
    except (TypeError, ValueError):
        return None
    if amount_value <= 0 or float_mv_value <= 0:
        return None
    return round(amount_value / (float_mv_value * 100.0), 2)


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


def get_all_stock_codes() -> pd.DataFrame:
    return load_sync_api().get_all_stock_codes()


def load_indicator_api():
    try:
        import compute_indicators as indicator_api
    except ImportError:
        from . import compute_indicators as indicator_api
    return indicator_api


def load_sync_api():
    try:
        import sync_market_data as sync_api
    except ImportError:
        from . import sync_market_data as sync_api
    return sync_api


def load_indicator_module():
    indicator_api = load_indicator_api()
    return indicator_api.LOOKBACK_DAYS, indicator_api.analyze_stock


def fetch_tencent_realtime(codes: list[str]) -> dict[str, dict]:
    return load_sync_api().fetch_tencent_realtime(codes)


def fetch_daily_bars(code: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
    return load_sync_api().fetch_daily_bars(code, start_date=start_date, end_date=end_date, adjust=adjust)


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
    return load_sync_api().upsert_daily_bars(conn, code, adjust_type, bars_df)


def replace_qfq_history(conn: sqlite3.Connection, code: str, bars_df: pd.DataFrame) -> int:
    return load_sync_api().replace_qfq_history(conn, code, bars_df)


def select_sync_candidates(stocks_df: pd.DataFrame, realtime: dict[str, dict]) -> list[dict]:
    candidates = []
    for row in stocks_df.itertuples(index=False):
        quote = realtime.get(row.code)
        if not quote:
            continue
        liquidity_ratio_pct = calc_liquidity_ratio_pct(quote.get("amount_wan"), quote.get("float_mv_yi"))
        if liquidity_ratio_pct is None or liquidity_ratio_pct < MIN_LIQUIDITY_RATIO_PCT:
            continue
        if not (MIN_FLOAT_MV <= quote["float_mv_yi"] <= MAX_FLOAT_MV):
            continue
        candidates.append(
            {
                "code": row.code,
                "name": quote.get("name", row.name),
                "board": row.board,
                "amount_wan": quote["amount_wan"],
                "float_mv_yi": quote["float_mv_yi"],
                "liquidity_ratio_pct": liquidity_ratio_pct,
            }
        )
    return candidates


def run_sync(
    review_date: str | None = None,
    limit: int = 0,
    request_pause: float = 0.15,
) -> dict:
    sync_api = load_sync_api()
    engine = sync_api.StockMarketSyncEngine(
        limit=limit,
        request_pause=request_pause,
    )
    return engine.run_sync(
        start_date=review_date,
        end_date=review_date,
    )


def normalize_kline_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])

    if "date" not in df.columns:
        df = df.reset_index()
        if "date" not in df.columns and "index" in df.columns:
            df = df.rename(columns={"index": "date"})

    rename_map = {
        "vol": "volume",
        "成交量": "volume",
    }
    df = df.rename(columns=rename_map)
    if "amount" not in df.columns and "volume" in df.columns:
        df["amount"] = df["volume"]
    needed_columns = ["date", "open", "high", "low", "close", "volume", "amount"]
    missing = [column for column in needed_columns if column not in df.columns]
    if missing:
        raise ValueError(f"K line data missing columns: {missing}")

    data = df[needed_columns].copy()
    data["date"] = data["date"].apply(normalize_trade_date)
    for column in ["open", "high", "low", "close", "volume", "amount"]:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data = data.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return data


def calc_ma(series: pd.Series, window: int) -> pd.Series:
    return load_indicator_api().calc_ma(series, window)


def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    return load_indicator_api().calc_rsi(series, period=period)


def calc_ma_angle(ma_short_val: float, ma_long_val: float, ma_short_prev: float, ma_long_prev: float) -> float:
    return load_indicator_api().calc_ma_angle(ma_short_val, ma_long_val, ma_short_prev, ma_long_prev)


def detect_golden_cross_type(ma5: pd.Series, ma20: pd.Series, close: pd.Series) -> str:
    return load_indicator_api().detect_golden_cross_type(ma5, ma20, close)


def get_shrinking_down_day_volumes(
    close: pd.Series,
    volume: pd.Series,
    lookback: int = SHRINKING_DOWN_LOOKBACK,
) -> list[float]:
    return load_indicator_api().get_shrinking_down_day_volumes(close, volume, lookback=lookback)


def count_shrinking_down_days(close: pd.Series, volume: pd.Series, lookback: int = SHRINKING_DOWN_LOOKBACK) -> int:
    return load_indicator_api().count_shrinking_down_days(close, volume, lookback=lookback)


def summarize_score_components(score_components: dict[str, list[tuple[str, int]]]) -> tuple[int, list[tuple[str, int]], dict[str, int]]:
    return load_indicator_api().summarize_score_components(score_components)


def build_dedup_score_components(score_components: dict[str, list[tuple[str, int]]]) -> dict[str, list[tuple[str, int]]]:
    return load_indicator_api().build_dedup_score_components(score_components)


def is_breakout_ready_second_cross_setup(
    cross_type: str,
    vol_ratio: float,
    rsi_val: float,
    bias: float,
    space_to_high: float,
) -> bool:
    return load_indicator_api().is_breakout_ready_second_cross_setup(
        cross_type,
        vol_ratio,
        rsi_val,
        bias,
        space_to_high,
    )


def build_space_to_high_penalty_components(space_to_high: float, breakout_ready_second_cross: bool = False) -> dict[str, list[tuple[str, int]]]:
    return load_indicator_api().build_space_to_high_penalty_components(
        space_to_high,
        breakout_ready_second_cross=breakout_ready_second_cross,
    )


def analyze_stock(
    code: str,
    name: str,
    kline: pd.DataFrame,
    scoring_mode: str = "dedup",
    float_mv_yi: float | None = None,
    liquidity_ratio_pct: float | None = None,
) -> dict | None:
    return load_indicator_api().analyze_stock(
        code,
        name,
        kline,
        scoring_mode=scoring_mode,
        float_mv_yi=float_mv_yi,
        liquidity_ratio_pct=liquidity_ratio_pct,
    )


def serialize_signals(signals: list[tuple[str, int]]) -> str:
    return json.dumps(signals, ensure_ascii=False)


def deserialize_signals(value: str) -> list[tuple[str, int]]:
    if not value:
        return []
    data = json.loads(value)
    return [(str(item[0]), int(item[1])) for item in data]


def to_csv_rows(results: list[dict]) -> list[dict]:
    rows = []
    for result in results:
        rows.append(
            {
                "代码": result["code"],
                "名称": result["name"],
                "板块": result["board"],
                "现价": result["close"],
                "今开": result.get("open", ""),
                "最高": result.get("high", ""),
                "最低": result.get("low", ""),
                "今收": result.get("close", ""),
                "涨跌幅%": result["pct_change"],
                "综合评分": result["score"],
                "金叉类型": result["cross_type_cn"],
                "量比": result["vol_ratio"],
                "RSI": result["rsi"],
                "乖离率%": result["bias"],
                "MA5": result["ma5"],
                "MA20": result["ma20"],
                "多头排列": "是" if result.get("bullish_alignment") else "否",
                "成交额(万)": result.get("amount_wan", ""),
                "流通市值亿": result.get("float_mv_yi", ""),
                "流动性%": result.get("liquidity_ratio_pct", ""),
                "成交量": result.get("volume", ""),
                "信号": "; ".join(item[0] for item in sorted(result["signals"], key=lambda pair: pair[1], reverse=True)),
            }
        )
    return rows


def load_candidates(conn: sqlite3.Connection, review_date: str, include_all_boards: bool) -> list[sqlite3.Row]:
    board_condition = ""
    params: list[object] = [review_date]
    if not include_all_boards:
        placeholders = ",".join("?" for _ in ALLOWED_BOARDS)
        board_condition = f"AND s.board IN ({placeholders})"
        params.extend(ALLOWED_BOARDS)

    query = f"""
        WITH ordered_daily AS (
            SELECT
                code,
                trade_date,
                open,
                high,
                low,
                close,
                volume,
                LAG(close) OVER (PARTITION BY code ORDER BY trade_date) AS previous_close_calc,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS rn
            FROM daily_bars
            WHERE trade_date <= ?
        ),
        latest_market AS (
            SELECT
                code,
                amount_wan,
                float_mv_yi,
                CASE
                    WHEN amount_wan IS NOT NULL AND amount_wan > 0 AND float_mv_yi IS NOT NULL AND float_mv_yi > 0
                        THEN amount_wan / (float_mv_yi * 100.0)
                    ELSE NULL
                END AS liquidity_ratio_pct
            FROM latest_market_value
        ),
        latest_daily AS (
            SELECT *
            FROM ordered_daily
            WHERE rn = 1
        )
        SELECT
            s.code,
            s.name,
            s.board,
            ld.close,
            ld.open,
            ld.high,
            ld.low,
            CASE
                WHEN ld.previous_close_calc IS NOT NULL AND ld.previous_close_calc > 0 THEN ((ld.close / ld.previous_close_calc) - 1) * 100
                ELSE 0
            END AS pct_change,
            ld.volume AS volume,
            lmv.amount_wan,
            lmv.float_mv_yi,
            lmv.liquidity_ratio_pct
        FROM stocks s
        INNER JOIN latest_daily ld ON ld.code = s.code
        LEFT JOIN latest_market lmv ON lmv.code = s.code
        WHERE s.is_st = 0
          {board_condition}
          AND lmv.liquidity_ratio_pct IS NOT NULL
          AND lmv.liquidity_ratio_pct >= ?
          AND (lmv.float_mv_yi IS NULL OR lmv.float_mv_yi BETWEEN ? AND ?)
        ORDER BY s.code
    """
    params.extend([MIN_LIQUIDITY_RATIO_PCT, MIN_FLOAT_MV, MAX_FLOAT_MV])
    return conn.execute(query, params).fetchall()


def load_qfq_bars(conn: sqlite3.Connection, code: str, required_rows: int) -> pd.DataFrame:
    query = """
        SELECT trade_date AS date, open, high, low, close, volume, volume AS amount
        FROM daily_bars
        WHERE code = ?
        ORDER BY trade_date
    """
    df = pd.read_sql_query(query, conn, params=[code])
    if df.empty:
        return df
    return df.tail(required_rows)


def store_indicator_snapshots(conn: sqlite3.Connection, review_date: str, results: list[dict]) -> None:
    with conn:
        conn.execute("DELETE FROM indicator_snapshots WHERE run_date = ?", (review_date,))
        conn.executemany(
            """
            INSERT INTO indicator_snapshots(
                run_date, code, score, cross_type, cross_type_cn,
                vol_ratio, angle, rsi, bias, space_to_high, ma5, ma20, bullish_alignment,
                signals, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    review_date,
                    item["code"],
                    item["score"],
                    item["cross_type"],
                    item["cross_type_cn"],
                    item["vol_ratio"],
                    item["angle"],
                    item["rsi"],
                    item["bias"],
                    item["space_to_high"],
                    item["ma5"],
                    item["ma20"],
                    int(bool(item["bullish_alignment"])),
                    serialize_signals(item["signals"]),
                    now_ts(),
                )
                for item in results
            ],
        )


def run_compute(review_date: str | None = None, limit: int = 0, include_all_boards: bool = False) -> dict:
    lookback_days, analyze_stock = load_indicator_module()
    trade_date, _ = parse_review_date(review_date)
    conn = get_db_connection()
    candidates = load_candidates(conn, trade_date, include_all_boards)
    if limit > 0:
        candidates = candidates[:limit]

    results: list[dict] = []
    analyzed_count = 0
    missing_kline = 0

    for row in candidates:
        kline = load_qfq_bars(conn, row["code"], required_rows=lookback_days + 80)
        if kline.empty or len(kline) < lookback_days // 2:
            missing_kline += 1
            continue
        analyzed_count += 1
        float_mv_yi = row["float_mv_yi"] if "float_mv_yi" in row.keys() else None
        liquidity_ratio_pct = row["liquidity_ratio_pct"] if "liquidity_ratio_pct" in row.keys() else None
        result = analyze_stock(
            row["code"],
            row["name"],
            kline,
            float_mv_yi=float_mv_yi,
            liquidity_ratio_pct=liquidity_ratio_pct,
        )
        if result is None:
            continue
        result["board"] = row["board"]
        result["close"] = round(float(row["close"]), 2) if row["close"] is not None else result["close"]
        result["open"] = round(float(row["open"]), 2) if row["open"] is not None else None
        result["high"] = round(float(row["high"]), 2) if row["high"] is not None else None
        result["low"] = round(float(row["low"]), 2) if row["low"] is not None else None
        result["pct_change"] = round(float(row["pct_change"]), 2) if row["pct_change"] is not None else 0.0
        result["amount_wan"] = round(float(row["amount_wan"]), 2) if row["amount_wan"] is not None else None
        result["float_mv_yi"] = round(float(row["float_mv_yi"]), 2) if row["float_mv_yi"] is not None else None
        result["liquidity_ratio_pct"] = round(float(liquidity_ratio_pct), 2) if liquidity_ratio_pct is not None else None
        result["volume"] = round(float(row["volume"]), 2) if row["volume"] is not None else None
        results.append(result)

    results.sort(key=lambda item: item["score"], reverse=True)
    store_indicator_snapshots(conn, trade_date, results)
    conn.close()

    return {
        "review_date": trade_date,
        "candidate_count": len(candidates),
        "analyzed_count": analyzed_count,
        "missing_kline_count": missing_kline,
        "signal_count": len(results),
        "results": results,
    }


def load_snapshots(review_date: str | None = None) -> list[dict]:
    trade_date, _ = parse_review_date(review_date)
    conn = get_db_connection()
    rows = conn.execute(
        """
        WITH ordered_daily AS (
            SELECT
                code,
                trade_date,
                open,
                high,
                low,
                close,
                volume,
                float_mv_yi,
                LAG(close) OVER (PARTITION BY code ORDER BY trade_date) AS previous_close_calc,
                ROW_NUMBER() OVER (PARTITION BY code ORDER BY trade_date DESC) AS rn
            FROM daily_bars
            WHERE trade_date <= ?
        ),
        latest_daily AS (
            SELECT *
            FROM ordered_daily
            WHERE rn = 1
        )
        SELECT
            snap.*,
            s.name,
            s.board,
            ld.open,
            ld.high,
            ld.low,
            ld.close,
            ld.volume AS volume,
            ld.float_mv_yi,
            CASE
                WHEN ld.previous_close_calc IS NOT NULL AND ld.previous_close_calc > 0 THEN ((ld.close / ld.previous_close_calc) - 1) * 100
                ELSE 0
            END AS pct_change
        FROM indicator_snapshots snap
        INNER JOIN stocks s ON s.code = snap.code
        LEFT JOIN latest_daily ld ON ld.code = snap.code
        WHERE snap.run_date = ?
        ORDER BY snap.score DESC, snap.code ASC
        """,
        (trade_date, trade_date),
    ).fetchall()
    conn.close()
    result = []
    for row in rows:
        item = dict(row)
        item["signals"] = deserialize_signals(item["signals"])
        item["bullish_alignment"] = bool(item["bullish_alignment"])
        result.append(item)
    return result


class ReviewWriter:
    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.lines: list[str] = []
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, line: str = "") -> None:
        self.lines.append(line)
        self.output_path.write_text("\n".join(self.lines).strip() + "\n", encoding="utf-8")

    def heading(self, title: str, level: int = 2) -> None:
        self.append(f"{'#' * level} {title}")
        self.append()


def markdown_table(rows: list[list[object]], headers: list[str]) -> str:
    if not rows:
        return "_无数据_"

    def fmt(value: object) -> str:
        return str(value).replace("|", "\\|").replace("\n", " ")

    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(fmt(item) for item in row) + " |")
    return "\n".join(lines)


def record_review_run(
    review_date: str,
    started_at: str,
    finished_at: str,
    status: str,
    markdown_path: Path,
    csv_path: Path,
    sync_summary: dict,
    compute_summary: dict,
    notes: str = "",
) -> None:
    conn = get_db_connection()
    with conn:
        conn.execute(
            """
            INSERT INTO review_runs(
                review_date, started_at, finished_at, status, universe_count, candidate_count,
                analyzed_count, signal_count, markdown_path, csv_path, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(review_date) DO UPDATE SET
                started_at = excluded.started_at,
                finished_at = excluded.finished_at,
                status = excluded.status,
                universe_count = excluded.universe_count,
                candidate_count = excluded.candidate_count,
                analyzed_count = excluded.analyzed_count,
                signal_count = excluded.signal_count,
                markdown_path = excluded.markdown_path,
                csv_path = excluded.csv_path,
                notes = excluded.notes
            """,
            (
                review_date,
                started_at,
                finished_at,
                status,
                sync_summary.get("universe_count"),
                compute_summary.get("candidate_count"),
                compute_summary.get("analyzed_count"),
                compute_summary.get("signal_count"),
                str(markdown_path),
                str(csv_path),
                notes,
            ),
        )
    conn.close()


def get_review_run_record(review_date: str) -> dict | None:
    conn = get_db_connection()
    try:
        row = conn.execute(
            """
            SELECT review_date, started_at, finished_at, status, markdown_path, csv_path, notes
            FROM review_runs
            WHERE review_date = ?
            """,
            (review_date,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def ensure_review_market_data_ready(review_date: str) -> dict:
    sync_api = load_sync_api()
    trade_date, compact_date = parse_review_date(review_date)
    status = sync_api.has_market_data_for_date(trade_date)
    status["is_trade_day"] = sync_api.is_trade_day(trade_date)
    status["suggested_command"] = f"python -m stock.sync_market_data --start-date {trade_date} --end-date {trade_date}"
    if not status["is_trade_day"]:
        status["is_ready_for_review"] = True
        status["check_message"] = f"{trade_date} 非交易日，跳过当日同步校验，直接使用数据库中最近交易日数据。"
        return status
    if status["is_ready"]:
        status["is_ready_for_review"] = True
        status["check_message"] = (
            f"{trade_date} 已检测到当日同步数据：日线 {status['daily_bar_count']} 行，"
            f"分时 {status['intraday_bar_count']} 行，分布 {status['distribution_count']} 条。"
        )
        return status
    status["is_ready_for_review"] = False
    status["check_message"] = (
        f"{trade_date} 还没有同步完成当日行情数据：日线 {status['daily_bar_count']} 行，"
        f"分时 {status['intraday_bar_count']} 行，分布 {status['distribution_count']} 条。"
        f"请先执行 {status['suggested_command']}，然后再运行每日选股。"
    )
    return status


def evaluate_auto_run(now: datetime | None = None) -> tuple[bool, str, str]:
    current = now or datetime.now()
    review_date = current.strftime("%Y-%m-%d")
    if current.time() < AUTO_RUN_CUTOFF:
        return False, review_date, f"还没到运行时间，当前时间 {current.strftime('%H:%M:%S')}，需在 15:30 之后执行。"

    sync_status = ensure_review_market_data_ready(review_date)
    if not sync_status["is_ready_for_review"]:
        return False, review_date, sync_status["check_message"]

    run_record = get_review_run_record(review_date)
    if run_record and run_record.get("status") == "success":
        finished_at = run_record.get("finished_at") or run_record.get("started_at") or "未知时间"
        return False, review_date, f"今天已经运行完成，完成时间 {finished_at}。"

    return True, review_date, f"{sync_status['check_message']} 开始执行 {review_date} 的每日选股。"


def write_review_markdown(
    writer: ReviewWriter,
    review_date: str,
    started_at: str,
    finished_at: str,
    sync_summary: dict,
    compute_summary: dict,
    top_n: int,
) -> None:
    results = compute_summary["results"]
    writer.append(f"# Review {review_date.replace('-', '')}")
    writer.append()
    writer.append(f"- 开始时间: {started_at}")
    writer.append(f"- 结束时间: {finished_at}")
    writer.append(f"- SQLite: {sync_summary['db_path']}")
    writer.append()

    writer.heading("执行过程")
    writer.append("1. review_common 本次未直接抓取行情，直接读取 SQLite 中已同步的数据。")
    writer.append(sync_summary.get("check_message", "2. 使用数据库中的当日或最近交易日行情。"))
    writer.append(
        f"3. 计算指标：候选 {compute_summary['candidate_count']} 只，实际分析 {compute_summary['analyzed_count']} 只，生成信号 {compute_summary['signal_count']} 只。"
    )
    if sync_summary["errors"]:
        writer.append()
        writer.append("异常记录:")
        for item in sync_summary["errors"][:20]:
            writer.append(f"- {item}")
    writer.append()

    writer.heading("同步摘要")
    writer.append(f"- SQLite: {sync_summary['db_path']}")
    writer.append(f"- 交易日: {'是' if sync_summary.get('is_trade_day') else '否'}")
    writer.append(f"- 当日日线行数: {sync_summary.get('daily_bar_count', 0)}")
    writer.append(f"- 当日分时行数: {sync_summary.get('intraday_bar_count', 0)}")
    writer.append(f"- 当日价格分布条数: {sync_summary.get('distribution_count', 0)}")
    writer.append()

    writer.heading("信号摘要")
    if not results:
        writer.append("今日没有符合条件的买入信号。")
        writer.append()
        return

    top_rows = []
    for index, item in enumerate(results[:top_n], start=1):
        top_rows.append(
            [
                index,
                item["code"],
                item["name"],
                item["board"],
                f"{item['close']:.2f}",
                f"{item.get('open', 0) or 0:.2f}",
                f"{item.get('high', 0) or 0:.2f}",
                f"{item.get('low', 0) or 0:.2f}",
                f"{item['pct_change']:+.2f}%",
                item["score"],
                item["cross_type_cn"],
                ", ".join(signal[0] for signal in item["signals"][:3]),
            ]
        )
    writer.append(markdown_table(top_rows, ["排名", "代码", "名称", "板块", "现价", "今开", "最高", "最低", "涨跌", "评分", "金叉类型", "核心信号"]))
    writer.append()

    for board in sorted({item["board"] for item in results}):
        board_rows = [item for item in results if item["board"] == board]
        writer.heading(board, level=3)
        rows = []
        for item in board_rows[:25]:
            rows.append(
                [
                    item["code"],
                    item["name"],
                    f"{item['close']:.2f}",
                    f"{item.get('open', 0) or 0:.2f}",
                    f"{item.get('high', 0) or 0:.2f}",
                    f"{item.get('low', 0) or 0:.2f}",
                    f"{item['pct_change']:+.2f}%",
                    item["score"],
                    f"{item['vol_ratio']:.1f}x",
                    f"{item['rsi']:.1f}",
                    f"{item['bias']:.2f}%",
                    f"{item.get('float_mv_yi') or 0:.1f}",
                ]
            )
        writer.append(markdown_table(rows, ["代码", "名称", "现价", "今开", "最高", "最低", "涨跌", "评分", "量比", "RSI", "乖离", "流通市值亿"]))
        writer.append()


def run_daily_review(
    review_date: str | None = None,
    top_n: int = 10,
    limit: int = 0,
    include_all_boards: bool = False,
    skip_sync: bool = False,
    recent_days: int = 45,
) -> dict:
    """
    复盘主流程：
1. 确保当天的市场数据已经同步完成，或者当天非交易日
2. 从 SQLite 中加载候选股票列表和对应的行情数据
3. 计算技术指标和选股信号
4. 将结果输出到 Markdown 和 CSV 文件

参数说明：
- review_date: 复盘日期，格式 YYYYMMDD 或 YYYY-MM-DD，默认为当天
- top_n: Markdown 中展示的 TOP N 只股票，默认为 10
- limit: 仅分析前 N 只候选股，默认为 0（不限制）
- include_all_boards: 是否包含创业板、科创板等全部板块，默认为 False（仅主板）
- skip_sync: 是否跳过当日同步完整性检查，直接使用 SQLite 现有数据，默认为 False
- recent_days: 保留兼容参数，不再触发实际行情同步，默认为 45
    """
    trade_date, compact_date = parse_review_date(review_date)
    markdown_path = REVIEW_DIR / f"review{compact_date}.md"
    csv_path = REVIEW_DIR / f"review{compact_date}.csv"
    writer = ReviewWriter(markdown_path)
    started_at = now_ts()

    writer.append(f"# Review {compact_date}")
    writer.append()
    writer.append("_运行中..._")

    sync_summary = {
        "review_date": trade_date,
        "db_path": str(DB_PATH),
        "daily_bar_count": 0,
        "intraday_bar_count": 0,
        "distribution_count": 0,
        "is_trade_day": True,
        "is_ready": False,
        "is_ready_for_review": False,
        "check_message": "",
        "errors": [],
    }

    try:
        if not skip_sync:
            sync_summary = ensure_review_market_data_ready(trade_date)
            print(f"[1/3] {sync_summary['check_message']}")
            if not sync_summary["is_ready_for_review"]:
                raise RuntimeError(sync_summary["check_message"])
        else:
            conn = get_db_connection()
            conn.close()
            sync_summary["check_message"] = f"{trade_date} 手动跳过当日同步校验，直接读取现有 SQLite。"
            print(f"[1/3] {sync_summary['check_message']}")

        print(f"[2/3] 计算技术指标: {trade_date}")
        compute_summary = run_compute(review_date=trade_date, limit=limit, include_all_boards=include_all_boards)

        csv_rows = to_csv_rows(compute_summary["results"])
        pd.DataFrame(csv_rows).to_csv(csv_path, index=False, encoding="utf-8-sig")

        finished_at = now_ts()
        writer.lines = []
        write_review_markdown(writer, trade_date, started_at, finished_at, sync_summary, compute_summary, top_n)
        record_review_run(
            review_date=trade_date,
            started_at=started_at,
            finished_at=finished_at,
            status="success",
            markdown_path=markdown_path,
            csv_path=csv_path,
            sync_summary=sync_summary,
            compute_summary=compute_summary,
        )

        print(f"[3/3] 输出完成: {markdown_path.name}, {csv_path.name}")
        print(
            f"复盘完成: 候选 {compute_summary['candidate_count']} 只, 已分析 {compute_summary['analyzed_count']} 只, "
            f"信号 {compute_summary['signal_count']} 只"
        )
        return {
            "review_date": trade_date,
            "markdown_path": markdown_path,
            "csv_path": csv_path,
            "sync_summary": sync_summary,
            "compute_summary": compute_summary,
        }
    except Exception as exc:
        finished_at = now_ts()
        writer.lines = [f"# Review {compact_date}", "", f"- 开始时间: {started_at}", f"- 结束时间: {finished_at}", "", "## 异常", "", f"- {exc}", "", "```", traceback.format_exc().strip(), "```"]
        writer.output_path.write_text("\n".join(writer.lines) + "\n", encoding="utf-8")
        record_review_run(
            review_date=trade_date,
            started_at=started_at,
            finished_at=finished_at,
            status="failed",
            markdown_path=markdown_path,
            csv_path=csv_path,
            sync_summary=sync_summary,
            compute_summary={"candidate_count": 0, "analyzed_count": 0, "signal_count": 0},
            notes=str(exc),
        )
        raise


def run_daily_review_on_startup(
    top_n: int = 10,
    limit: int = 0,
    include_all_boards: bool = False,
    recent_days: int = 45,
) -> dict | None:
    should_run, review_date, message = evaluate_auto_run()
    print(message)
    if not should_run:
        return None

    return run_daily_review(
        review_date=review_date,
        top_n=top_n,
        limit=limit,
        include_all_boards=include_all_boards,
        skip_sync=False,
        recent_days=recent_days,
    )


def build_sync_arg_parser() -> argparse.ArgumentParser:
    return load_sync_api().build_sync_arg_parser()


def main_sync_market_data() -> None:
    load_sync_api().main()


def build_daily_review_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="股票每日复盘主入口")
    parser.add_argument("--date", type=str, default=None, help="复盘日期，格式 YYYYMMDD 或 YYYY-MM-DD")
    parser.add_argument("--top", type=int, default=10, help="Markdown 中展示的 TOP N")
    parser.add_argument("--limit", type=int, default=0, help="仅分析前 N 只候选股")
    parser.add_argument("--all-boards", action="store_true", help="包含创业板、科创板等全部板块")
    parser.add_argument("--skip-sync", action="store_true", help="跳过当日同步完整性检查，直接使用 SQLite 现有数据")
    parser.add_argument("--recent-days", type=int, default=45, help="保留兼容参数，不再触发实际行情同步")
    parser.add_argument("--force-run", action="store_true", help="忽略启动时间和当日运行记录，直接执行今日复盘")
    return parser


def main_daily_review() -> None:
    args = build_daily_review_arg_parser().parse_args()
    if args.date is None and not args.skip_sync and not args.force_run:
        run_daily_review_on_startup(
            top_n=args.top,
            limit=args.limit,
            include_all_boards=args.all_boards,
            recent_days=args.recent_days,
        )
        return

    run_daily_review(
        review_date=args.date,
        top_n=args.top,
        limit=args.limit,
        include_all_boards=args.all_boards,
        skip_sync=args.skip_sync,
        recent_days=args.recent_days,
    )

if __name__ == "__main__":
    main_daily_review()