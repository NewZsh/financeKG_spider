#!/usr/bin/env python3
"""首板低开套利复盘脚本。

功能：
1. 复盘指定交易日，筛出“前一日首板 + 当日低开 -3%~-4% + 相对低位”的实际买点。
2. 生成“今日首板且收盘仍低于 MA60”的次日观察池。

用法：
    python -m stock.review_first_limit_low_open --date 2026-06-03
    python -m stock.review_first_limit_low_open --date 2026-06-03 --skip-sync --top 20
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REVIEW_DIR = DATA_DIR / "stock"
SCHEMA_PATH = DATA_DIR / "schema" / "stock.sql"
DB_PATH = DATA_DIR / "stock.db"

LOOKBACK_RANGE_DAYS = 430
LISTING_AGE_DAYS = 365
LOW_OPEN_MIN_PCT = -4.0
LOW_OPEN_MAX_PCT = -3.0

EXCLUDED_BOARDS = {"科创板（688）", "北交所（8/4/9）"}
SQLITE_BUSY_TIMEOUT_MS = 10000


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


def get_db_connection() -> sqlite3.Connection:
    ensure_runtime_paths()
    conn = sqlite3.connect(DB_PATH, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    return conn


def round_price(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def limit_up_ratio(code: str, trade_date: str) -> float:
    if code.startswith("300") and trade_date >= "2020-08-24":
        return 0.20
    if code.startswith("688"):
        return 0.20
    if code.startswith(("8", "4", "9")):
        return 0.30
    return 0.10


def is_limit_up(code: str, trade_date: str, close_price: float, previous_close: float) -> bool:
    if previous_close <= 0:
        return False
    theoretical_price = round_price(previous_close * (1 + limit_up_ratio(code, trade_date)))
    return close_price >= theoretical_price - 0.01


def resolve_effective_trade_date(conn: sqlite3.Connection, requested_date: str) -> str:
    row = conn.execute(
        "SELECT MAX(trade_date) AS trade_date FROM daily_bars WHERE trade_date <= ?",
        (requested_date,),
    ).fetchone()
    if not row or row["trade_date"] is None:
        raise RuntimeError(f"SQLite 中没有 {requested_date} 及以前的日线数据")
    return str(row["trade_date"])


def ensure_trade_date_data(conn: sqlite3.Connection, trade_date: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM daily_bars WHERE trade_date = ?",
        (trade_date,),
    ).fetchone()
    count = int(row["cnt"]) if row else 0
    if count <= 0:
        raise RuntimeError(f"{trade_date} 尚未同步到 SQLite，请先执行 python -m stock.sync_market_data --start-date {trade_date} --end-date {trade_date}")
    return count


def load_market_frame(conn: sqlite3.Connection, trade_date: str) -> pd.DataFrame:
    listing_cutoff = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=LISTING_AGE_DAYS)).strftime("%Y-%m-%d")
    history_start = (datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=LOOKBACK_RANGE_DAYS)).strftime("%Y-%m-%d")
    query = """
        WITH first_trade AS (
            SELECT code, MIN(trade_date) AS first_trade_date
            FROM daily_bars
            GROUP BY code
        ),
        universe AS (
            SELECT s.code, s.name, s.board, ft.first_trade_date
            FROM stocks s
            INNER JOIN first_trade ft ON ft.code = s.code
            WHERE s.is_st = 0
              AND s.board NOT IN ('科创板（688）', '北交所（8/4/9）')
              AND ft.first_trade_date <= ?
        )
        SELECT
            d.code,
            u.name,
            u.board,
            u.first_trade_date,
            d.trade_date,
            d.open,
            d.high,
            d.low,
            d.close,
            d.volume
        FROM daily_bars d
        INNER JOIN universe u ON u.code = d.code
        WHERE d.trade_date BETWEEN ? AND ?
        ORDER BY d.code, d.trade_date
    """
    return pd.read_sql_query(query, conn, params=[listing_cutoff, history_start, trade_date])


def analyze_trade_signals(bars: pd.DataFrame, trade_date: str) -> tuple[list[dict], list[dict], dict]:
    trade_signals: list[dict] = []
    watchlist: list[dict] = []
    reviewed_codes = 0

    for code, group in bars.groupby("code", sort=False):
        df = group.sort_values("trade_date").reset_index(drop=True)
        target_rows = df.index[df["trade_date"] == trade_date].tolist()
        if not target_rows:
            continue

        idx = target_rows[-1]
        if idx < 60:
            continue

        reviewed_codes += 1
        today = df.iloc[idx]
        yesterday = df.iloc[idx - 1]
        previous_close_for_yesterday = float(df.iloc[idx - 2]["close"]) if idx >= 2 else 0.0
        previous_limit_up = False
        if idx >= 3:
            previous_limit_up = is_limit_up(
                str(code),
                str(df.iloc[idx - 1]["trade_date"]),
                float(df.iloc[idx - 2]["close"]),
                float(df.iloc[idx - 3]["close"]),
            )

        yesterday_limit_up = is_limit_up(
            str(code),
            str(yesterday["trade_date"]),
            float(yesterday["close"]),
            previous_close_for_yesterday,
        )
        first_board = yesterday_limit_up and not previous_limit_up

        window_60 = df.iloc[idx - 60:idx]
        range_high = float(window_60["high"].max())
        range_low = float(window_60["low"].min())
        range_mid = (range_high + range_low) / 2
        open_gap_pct = ((float(today["open"]) / float(yesterday["close"])) - 1) * 100 if float(yesterday["close"]) > 0 else None

        if first_board and open_gap_pct is not None and LOW_OPEN_MIN_PCT <= open_gap_pct <= LOW_OPEN_MAX_PCT and float(today["open"]) <= range_mid:
            trade_signals.append(
                {
                    "signal_type": "entry_signal",
                    "trade_date": trade_date,
                    "code": str(code),
                    "name": str(today["name"]),
                    "board": str(today["board"]),
                    "first_trade_date": str(today["first_trade_date"]),
                    "prev_trade_date": str(yesterday["trade_date"]),
                    "open": round(float(today["open"]), 2),
                    "close": round(float(today["close"]), 2),
                    "prev_close": round(float(yesterday["close"]), 2),
                    "gap_open_pct": round(open_gap_pct, 2),
                    "range_high_60": round(range_high, 2),
                    "range_low_60": round(range_low, 2),
                    "range_mid_60": round(range_mid, 2),
                    "position_below_mid": round(range_mid - float(today["open"]), 2),
                    "yesterday_limit_up": True,
                    "first_board": True,
                }
            )

        if idx >= 59:
            today_ma60 = float(df.iloc[idx - 59:idx + 1]["close"].mean())
            today_limit_up = is_limit_up(
                str(code),
                str(today["trade_date"]),
                float(today["close"]),
                float(yesterday["close"]),
            )
            today_first_board = today_limit_up and not yesterday_limit_up
            if today_first_board and float(today["close"]) < today_ma60:
                watchlist.append(
                    {
                        "signal_type": "watchlist_today_first_board_below_ma60",
                        "trade_date": trade_date,
                        "code": str(code),
                        "name": str(today["name"]),
                        "board": str(today["board"]),
                        "close": round(float(today["close"]), 2),
                        "prev_close": round(float(yesterday["close"]), 2),
                        "pct_change": round((float(today["close"]) / float(yesterday["close"]) - 1) * 100, 2) if float(yesterday["close"]) > 0 else 0.0,
                        "ma60": round(today_ma60, 2),
                        "ma60_gap_pct": round((float(today["close"]) / today_ma60 - 1) * 100, 2) if today_ma60 > 0 else 0.0,
                        "range_high_60": round(float(df.iloc[idx - 59:idx + 1]["high"].max()), 2),
                        "range_low_60": round(float(df.iloc[idx - 59:idx + 1]["low"].min()), 2),
                    }
                )

    trade_signals.sort(key=lambda item: (item["gap_open_pct"], item["position_below_mid"] ))
    watchlist.sort(key=lambda item: item["ma60_gap_pct"])
    summary = {
        "reviewed_codes": reviewed_codes,
        "entry_signal_count": len(trade_signals),
        "watchlist_count": len(watchlist),
    }
    return trade_signals, watchlist, summary


def to_csv_rows(entry_signals: list[dict], watchlist: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for item in entry_signals:
        rows.append(item.copy())
    for item in watchlist:
        rows.append(item.copy())
    return rows


def write_markdown(
    writer: ReviewWriter,
    requested_date: str,
    effective_date: str,
    sync_skipped: bool,
    db_path: Path,
    daily_count: int,
    summary: dict,
    entry_signals: list[dict],
    watchlist: list[dict],
    top_n: int,
) -> None:
    writer.append(f"# Review First Limit Low Open {effective_date.replace('-', '')}")
    writer.append()
    writer.append(f"- 请求日期: {requested_date}")
    writer.append(f"- 实际复盘交易日: {effective_date}")
    writer.append(f"- SQLite: {db_path}")
    writer.append(f"- 同步校验: {'已跳过' if sync_skipped else '已检查'}")
    writer.append(f"- 当日日线行数: {daily_count}")
    writer.append(f"- 有效股票池: {summary['reviewed_codes']}")
    writer.append(f"- 实际低开买点: {summary['entry_signal_count']}")
    writer.append(f"- 次日观察池: {summary['watchlist_count']}")
    writer.append()

    writer.heading("策略规则")
    writer.append("1. 股票池过滤 ST、科创板、北交所，以及上市未满一年的次新股。")
    writer.append("2. 实际买点要求前一交易日涨停且为首板，当日开盘相对昨收低开 3% 到 4%，并且开盘价处于过去 60 个交易日价格区间下半区。")
    writer.append("3. 观察池只保留当日首板且收盘仍低于 MA60 的股票，用于次日低开跟踪。")
    writer.append()

    writer.heading("实际低开买点")
    if entry_signals:
        rows = []
        for index, item in enumerate(entry_signals[:top_n], start=1):
            rows.append(
                [
                    index,
                    item["code"],
                    item["name"],
                    item["board"],
                    f"{item['prev_close']:.2f}",
                    f"{item['open']:.2f}",
                    f"{item['gap_open_pct']:+.2f}%",
                    f"{item['range_low_60']:.2f}",
                    f"{item['range_high_60']:.2f}",
                    f"{item['range_mid_60']:.2f}",
                ]
            )
        writer.append(markdown_table(rows, ["排名", "代码", "名称", "板块", "昨收", "今开", "低开幅度", "60日低", "60日高", "区间中轴"]))
    else:
        writer.append("今日没有符合‘前一日首板 + 今日低开 -3%~-4% + 相对低位’的买点。")
    writer.append()

    writer.heading("今日首板且低于MA60观察池")
    if watchlist:
        rows = []
        for index, item in enumerate(watchlist[:top_n], start=1):
            rows.append(
                [
                    index,
                    item["code"],
                    item["name"],
                    item["board"],
                    f"{item['close']:.2f}",
                    f"{item['pct_change']:+.2f}%",
                    f"{item['ma60']:.2f}",
                    f"{item['ma60_gap_pct']:+.2f}%",
                ]
            )
        writer.append(markdown_table(rows, ["排名", "代码", "名称", "板块", "收盘", "涨跌幅", "MA60", "距离MA60"]))
    else:
        writer.append("今日没有符合‘首板且收盘低于 MA60’的观察标的。")
    writer.append()


def run_review(review_date: str | None = None, top_n: int = 20, skip_sync: bool = False) -> dict:
    requested_date, compact_date = parse_review_date(review_date)
    markdown_path = REVIEW_DIR / f"review_first_limit_low_open_{compact_date}.md"
    csv_path = REVIEW_DIR / f"review_first_limit_low_open_{compact_date}.csv"
    writer = ReviewWriter(markdown_path)

    conn = get_db_connection()
    try:
        effective_date = resolve_effective_trade_date(conn, requested_date)
        daily_count = ensure_trade_date_data(conn, effective_date)
        if not skip_sync and effective_date != requested_date:
            raise RuntimeError(f"请求日期 {requested_date} 不是已落库交易日，当前仅找到最近交易日 {effective_date}。如需直接使用最近交易日，请显式传入该日期或加 --skip-sync 后自行确认。")

        market_frame = load_market_frame(conn, effective_date)
        if market_frame.empty:
            raise RuntimeError(f"{effective_date} 没有可用股票池数据")

        entry_signals, watchlist, summary = analyze_trade_signals(market_frame, effective_date)
        pd.DataFrame(to_csv_rows(entry_signals, watchlist)).to_csv(csv_path, index=False, encoding="utf-8-sig")

        writer.lines = []
        write_markdown(
            writer=writer,
            requested_date=requested_date,
            effective_date=effective_date,
            sync_skipped=skip_sync,
            db_path=DB_PATH,
            daily_count=daily_count,
            summary=summary,
            entry_signals=entry_signals,
            watchlist=watchlist,
            top_n=top_n,
        )

        print(f"输出完成: {markdown_path.name}, {csv_path.name}")
        print(
            f"复盘完成: 有效股票池 {summary['reviewed_codes']} 只, "
            f"实际低开买点 {summary['entry_signal_count']} 只, "
            f"观察池 {summary['watchlist_count']} 只"
        )
        return {
            "requested_date": requested_date,
            "effective_date": effective_date,
            "markdown_path": markdown_path,
            "csv_path": csv_path,
            "summary": summary,
        }
    finally:
        conn.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="首板低开套利复盘脚本")
    parser.add_argument("--date", type=str, default=None, help="复盘日期，格式 YYYYMMDD 或 YYYY-MM-DD")
    parser.add_argument("--top", type=int, default=20, help="Markdown 展示前 N 只")
    parser.add_argument("--skip-sync", action="store_true", help="跳过日期一致性保护，直接读取 SQLite 现有数据")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    run_review(review_date=args.date, top_n=args.top, skip_sync=args.skip_sync)


if __name__ == "__main__":
    main()