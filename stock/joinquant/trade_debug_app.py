#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import math
import re
import sqlite3
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import pandas as pd
from flask import Flask, abort, jsonify, render_template, request


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "stock.db"
LOG_DIR = BASE_DIR / "jqlog"
DEFAULT_PRE_DAYS = 10
DEFAULT_POST_DAYS = 0

TRADE_LINE_RE = re.compile(r"^(?P<name>.+?)\((?P<code>\d{6})\.(?P<exchange>[A-Z]+)\)$")

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))
app.config["JSON_AS_ASCII"] = False
app.config["JSON_SORT_KEYS"] = False


@dataclass(frozen=True)
class ParsedTrade:
    trade_date: str
    trade_time: str
    symbol_text: str
    name: str
    code: str
    exchange: str
    side: str
    order_type: str
    quantity: int
    price: float | None
    gross_amount: float
    realized_pnl: float
    fee: float
    executed: bool
    status_text: str
    net_pnl: float


def get_db_connection() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"stock.db 不存在: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def available_logs() -> list[Path]:
    if not LOG_DIR.exists():
        return []
    return sorted([path for path in LOG_DIR.glob("*.log") if path.is_file()])


def normalize_log_name(log_name: str | None) -> str:
    logs = available_logs()
    if not logs:
        raise FileNotFoundError(f"未找到日志目录或日志文件: {LOG_DIR}")

    if not log_name:
        return logs[0].name

    candidate = LOG_DIR / Path(log_name).name
    if not candidate.exists():
        raise FileNotFoundError(f"未找到日志文件: {candidate.name}")
    return candidate.name


def parse_float(text: str) -> float | None:
    raw = text.strip().replace(",", "")
    if not raw or raw == "--":
        return None
    return float(raw)


def parse_int_shares(text: str) -> int:
    raw = text.strip().replace(",", "").replace("股", "")
    if not raw or raw == "--":
        return 0
    return int(float(raw))


def parse_symbol(symbol_text: str) -> tuple[str, str, str]:
    match = TRADE_LINE_RE.match(symbol_text.strip())
    if not match:
        raise ValueError(f"无法解析标的字段: {symbol_text}")
    return match.group("name"), match.group("code"), match.group("exchange")


def parse_trade_line(line: str) -> ParsedTrade | None:
    if not line.strip() or line.startswith("http"):
        return None

    parts = [part.strip() for part in line.split("\t")]
    parts = [part for part in parts if part]
    if len(parts) != 10:
        return None

    name, code, exchange = parse_symbol(parts[2])
    quantity = parse_int_shares(parts[5])
    price = parse_float(parts[6])
    gross_amount = parse_float(parts[7]) or 0.0
    realized_pnl = parse_float(parts[8]) or 0.0
    fee = parse_float(parts[9]) or 0.0
    executed = quantity != 0 and price is not None
    status_text = "成交" if executed else "未成交"

    return ParsedTrade(
        trade_date=pd.Timestamp(parts[0]).strftime("%Y-%m-%d"),
        trade_time=parts[1],
        symbol_text=parts[2],
        name=name,
        code=code,
        exchange=exchange,
        side=parts[3],
        order_type=parts[4],
        quantity=quantity,
        price=price,
        gross_amount=gross_amount,
        realized_pnl=realized_pnl,
        fee=fee,
        executed=executed,
        status_text=status_text,
        net_pnl=realized_pnl - fee,
    )


@lru_cache(maxsize=16)
def load_trade_log(log_name: str, mtime_ns: int) -> list[ParsedTrade]:
    del mtime_ns
    log_path = LOG_DIR / log_name
    trades: list[ParsedTrade] = []
    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            trade = parse_trade_line(line)
            if trade is not None:
                trades.append(trade)
    trades.sort(key=lambda item: (item.trade_date, item.trade_time, item.code, item.side))
    return trades


def get_log_trades(log_name: str) -> list[ParsedTrade]:
    safe_name = normalize_log_name(log_name)
    path = LOG_DIR / safe_name
    return load_trade_log(safe_name, path.stat().st_mtime_ns)


def build_log_summary(log_name: str) -> dict:
    trades = get_log_trades(log_name)
    by_code: dict[str, dict] = {}
    daily_total_net: dict[str, float] = {}

    for trade_index, trade in enumerate(trades):
        symbol = by_code.setdefault(
            trade.code,
            {
                "code": trade.code,
                "name": trade.name,
                "exchange": trade.exchange,
                "symbol_text": trade.symbol_text,
                "buy_count": 0,
                "sell_count": 0,
                "trade_count": 0,
                "executed_count": 0,
                "failed_count": 0,
                "realized_pnl": 0.0,
                "fee": 0.0,
                "net_pnl": 0.0,
                "first_trade_date": trade.trade_date,
                "last_trade_date": trade.trade_date,
                "first_buy_date": None,
                "first_buy_time": None,
                "first_buy_order": None,
            },
        )
        symbol["trade_count"] += 1
        symbol["executed_count"] += 1 if trade.executed else 0
        symbol["failed_count"] += 0 if trade.executed else 1
        symbol["buy_count"] += 1 if trade.side == "买" else 0
        symbol["sell_count"] += 1 if trade.side == "卖" else 0
        symbol["realized_pnl"] += trade.realized_pnl
        symbol["fee"] += trade.fee
        symbol["net_pnl"] += trade.net_pnl
        symbol["first_trade_date"] = min(symbol["first_trade_date"], trade.trade_date)
        symbol["last_trade_date"] = max(symbol["last_trade_date"], trade.trade_date)
        if trade.side == "买" and trade.executed and symbol["first_buy_order"] is None:
            symbol["first_buy_date"] = trade.trade_date
            symbol["first_buy_time"] = trade.trade_time
            symbol["first_buy_order"] = trade_index
        daily_total_net[trade.trade_date] = daily_total_net.get(trade.trade_date, 0.0) + trade.net_pnl

    symbols = sorted(
        by_code.values(),
        key=lambda item: (
            item["first_buy_order"] is None,
            item["first_buy_order"] if item["first_buy_order"] is not None else float("inf"),
            item["first_trade_date"],
            item["code"],
        ),
    )
    total_net_pnl = sum(item["net_pnl"] for item in symbols)
    total_realized_pnl = sum(item["realized_pnl"] for item in symbols)
    total_fee = sum(item["fee"] for item in symbols)
    total_executed = sum(item["executed_count"] for item in symbols)
    total_failed = sum(item["failed_count"] for item in symbols)

    return {
        "log_name": normalize_log_name(log_name),
        "symbol_count": len(symbols),
        "trade_count": len(trades),
        "executed_count": total_executed,
        "failed_count": total_failed,
        "realized_pnl": round(total_realized_pnl, 2),
        "fee": round(total_fee, 2),
        "net_pnl": round(total_net_pnl, 2),
        "symbols": symbols,
        "daily_total_net": daily_total_net,
    }


def load_bars_with_indicators(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    query = """
        SELECT d.trade_date, d.open, d.high, d.low, d.close, d.volume, s.name
        FROM daily_bars d
        LEFT JOIN stocks s ON s.code = d.code
        WHERE d.code = ? AND d.trade_date BETWEEN ? AND ?
        ORDER BY d.trade_date
    """
    with get_db_connection() as conn:
        frame = pd.read_sql_query(query, conn, params=[code, start_date, end_date])

    if frame.empty:
        raise LookupError(f"stock.db 中没有 {code} 在 {start_date} ~ {end_date} 的日线数据")

    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    for window in (5, 10, 20, 30):
        frame[f"ma{window}"] = frame["close"].rolling(window).mean()
    return frame


def format_trade_hover_lines(trades: list[ParsedTrade]) -> str:
    if not trades:
        return "当日委托: 无"

    lines = []
    for trade in trades:
        price_text = "--" if trade.price is None else f"{trade.price:.2f}"
        line = (
            f"{trade.trade_time} {trade.side} {trade.quantity}股 @ {price_text} "
            f"{trade.status_text} | 类型:{trade.order_type} | 平仓盈亏:{trade.realized_pnl:.2f} | 手续费:{trade.fee:.2f}"
        )
        lines.append(html.escape(line))
    return "<br>".join(lines)


def sanitize_json_value(value):
    if isinstance(value, dict):
        return {key: sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_json_value(item) for item in value]
    if pd.isna(value):
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def build_chart_payload(log_name: str, code: str) -> dict:
    trades = [trade for trade in get_log_trades(log_name) if trade.code == code]
    if not trades:
        raise LookupError(f"日志 {log_name} 中没有股票 {code} 的交易记录")

    all_summary = build_log_summary(log_name)
    all_trades = get_log_trades(log_name)

    global_trade_dates = pd.to_datetime([trade.trade_date for trade in all_trades])
    start_date = (global_trade_dates.min() - pd.Timedelta(days=DEFAULT_PRE_DAYS)).strftime("%Y-%m-%d")
    end_date = (global_trade_dates.max() + pd.Timedelta(days=DEFAULT_POST_DAYS)).strftime("%Y-%m-%d")
    bars = load_bars_with_indicators(code, start_date, end_date)

    symbol_daily_net: dict[str, float] = {}
    symbol_daily_realized: dict[str, float] = {}
    symbol_trade_map: dict[str, list[ParsedTrade]] = {}
    for trade in trades:
        symbol_daily_net[trade.trade_date] = symbol_daily_net.get(trade.trade_date, 0.0) + trade.net_pnl
        symbol_daily_realized[trade.trade_date] = symbol_daily_realized.get(trade.trade_date, 0.0) + trade.realized_pnl
        symbol_trade_map.setdefault(trade.trade_date, []).append(trade)

    overall_daily_net: dict[str, float] = {}
    for trade in all_trades:
        overall_daily_net[trade.trade_date] = overall_daily_net.get(trade.trade_date, 0.0) + trade.net_pnl

    overall_cum = 0.0
    overall_cum_map: dict[str, float] = {}
    for day in sorted(overall_daily_net.keys()):
        overall_cum += overall_daily_net[day]
        overall_cum_map[day] = overall_cum

    symbol_cum = 0.0
    symbol_cum_map: dict[str, float] = {}
    for day in sorted(symbol_daily_net.keys()):
        symbol_cum += symbol_daily_net[day]
        symbol_cum_map[day] = symbol_cum

    hover_rows: list[list[object]] = []
    trade_rows: list[dict] = []
    buy_markers = {"x": [], "y": [], "text": [], "custom": []}
    sell_markers = {"x": [], "y": [], "text": [], "custom": []}
    failed_markers = {"x": [], "y": [], "text": [], "custom": []}

    bar_dates = bars["trade_date"].dt.strftime("%Y-%m-%d")
    low_series = bars["low"].fillna(bars["close"])
    high_series = bars["high"].fillna(bars["close"])
    close_series = bars["close"].fillna(0.0)

    for idx, row in bars.iterrows():
        trade_date = row["trade_date"].strftime("%Y-%m-%d")
        day_trades = symbol_trade_map.get(trade_date, [])
        hover_rows.append([
            round(float(row["open"]), 4) if pd.notna(row["open"]) else None,
            round(float(row["high"]), 4) if pd.notna(row["high"]) else None,
            round(float(row["low"]), 4) if pd.notna(row["low"]) else None,
            round(float(row["close"]), 4) if pd.notna(row["close"]) else None,
            round(float(row["volume"]), 2) if pd.notna(row["volume"]) else 0.0,
            round(float(row["ma5"]), 4) if pd.notna(row["ma5"]) else None,
            round(float(row["ma10"]), 4) if pd.notna(row["ma10"]) else None,
            round(float(row["ma20"]), 4) if pd.notna(row["ma20"]) else None,
            round(float(row["ma30"]), 4) if pd.notna(row["ma30"]) else None,
            round(symbol_daily_realized.get(trade_date, 0.0), 2),
            round(symbol_daily_net.get(trade_date, 0.0), 2),
            round(symbol_cum_map.get(trade_date, 0.0), 2),
            round(overall_daily_net.get(trade_date, 0.0), 2),
            round(overall_cum_map.get(trade_date, 0.0), 2),
            format_trade_hover_lines(day_trades),
        ])

        for trade_idx, trade in enumerate(day_trades):
            marker_custom = [
                trade.trade_time,
                trade.side,
                trade.quantity,
                "--" if trade.price is None else f"{trade.price:.2f}",
                trade.status_text,
                trade.order_type,
                f"{trade.realized_pnl:.2f}",
                f"{trade.fee:.2f}",
                f"{trade.net_pnl:.2f}",
            ]
            trade_rows.append(
                {
                    "trade_date": trade.trade_date,
                    "trade_time": trade.trade_time,
                    "side": trade.side,
                    "quantity": trade.quantity,
                    "price": trade.price,
                    "status_text": trade.status_text,
                    "order_type": trade.order_type,
                    "gross_amount": round(trade.gross_amount, 2),
                    "realized_pnl": round(trade.realized_pnl, 2),
                    "fee": round(trade.fee, 2),
                    "net_pnl": round(trade.net_pnl, 2),
                }
            )

            if trade.executed and trade.side == "买":
                buy_markers["x"].append(trade.trade_date)
                buy_markers["y"].append(round(float(low_series.iloc[idx]) * (1 - 0.012 * (trade_idx + 1)), 4))
                buy_markers["text"].append(f"买 {trade.quantity}股")
                buy_markers["custom"].append(marker_custom)
            elif trade.executed and trade.side == "卖":
                sell_markers["x"].append(trade.trade_date)
                sell_markers["y"].append(round(float(high_series.iloc[idx]) * (1 + 0.012 * (trade_idx + 1)), 4))
                sell_markers["text"].append(f"卖 {abs(trade.quantity)}股")
                sell_markers["custom"].append(marker_custom)
            else:
                failed_markers["x"].append(trade.trade_date)
                failed_markers["y"].append(round(float(close_series.iloc[idx]), 4))
                failed_markers["text"].append(f"{trade.side} 未成交")
                failed_markers["custom"].append(marker_custom)

    name = trades[0].name
    symbol_summary = next(item for item in all_summary["symbols"] if item["code"] == code)
    payload = {
        "log_name": normalize_log_name(log_name),
        "code": code,
        "name": name,
        "symbol_text": trades[0].symbol_text,
        "window_start": start_date,
        "window_end": end_date,
        "dates": bar_dates.tolist(),
        "open": bars["open"].round(4).tolist(),
        "high": bars["high"].round(4).tolist(),
        "low": bars["low"].round(4).tolist(),
        "close": bars["close"].round(4).tolist(),
        "volume": bars["volume"].fillna(0.0).round(2).tolist(),
        "ma5": bars["ma5"].where(pd.notna(bars["ma5"]), None).round(4).tolist(),
        "ma10": bars["ma10"].where(pd.notna(bars["ma10"]), None).round(4).tolist(),
        "ma20": bars["ma20"].where(pd.notna(bars["ma20"]), None).round(4).tolist(),
        "ma30": bars["ma30"].where(pd.notna(bars["ma30"]), None).round(4).tolist(),
        "hover_rows": hover_rows,
        "buy_markers": buy_markers,
        "sell_markers": sell_markers,
        "failed_markers": failed_markers,
        "trades": trade_rows,
        "symbol_summary": {
            "trade_count": symbol_summary["trade_count"],
            "executed_count": symbol_summary["executed_count"],
            "failed_count": symbol_summary["failed_count"],
            "realized_pnl": round(symbol_summary["realized_pnl"], 2),
            "fee": round(symbol_summary["fee"], 2),
            "net_pnl": round(symbol_summary["net_pnl"], 2),
            "first_trade_date": symbol_summary["first_trade_date"],
            "last_trade_date": symbol_summary["last_trade_date"],
        },
        "portfolio_summary": {
            "trade_count": all_summary["trade_count"],
            "executed_count": all_summary["executed_count"],
            "failed_count": all_summary["failed_count"],
            "realized_pnl": all_summary["realized_pnl"],
            "fee": all_summary["fee"],
            "net_pnl": all_summary["net_pnl"],
            "symbol_count": all_summary["symbol_count"],
        },
    }
    return sanitize_json_value(payload)


@app.route("/")
def index():
    return render_template("trade_debug.html")


@app.route("/api/logs")
def api_logs():
    logs = available_logs()
    if not logs:
        return jsonify({"logs": [], "default_log": None})
    return jsonify({"logs": [path.name for path in logs], "default_log": logs[0].name})


@app.route("/api/log-summary")
def api_log_summary():
    log_name = request.args.get("log")
    try:
        summary = build_log_summary(log_name or "")
    except FileNotFoundError as exc:
        abort(404, description=str(exc))
    return jsonify(summary)


@app.route("/api/chart-data")
def api_chart_data():
    log_name = request.args.get("log")
    code = (request.args.get("code") or "").strip()
    if not code:
        abort(400, description="缺少 code 参数")
    try:
        payload = build_chart_payload(log_name or "", code)
    except FileNotFoundError as exc:
        abort(404, description=str(exc))
    except LookupError as exc:
        abort(404, description=str(exc))
    return jsonify(payload)


def run_cli_check(log_name: str | None, code: str | None) -> None:
    summary = build_log_summary(log_name or "")
    selected_code = code or summary["symbols"][0]["code"]
    payload = build_chart_payload(summary["log_name"], selected_code)
    check_result = {
        "log_name": payload["log_name"],
        "symbol": payload["symbol_text"],
        "bar_count": len(payload["dates"]),
        "trade_count": len(payload["trades"]),
        "portfolio_net_pnl": payload["portfolio_summary"]["net_pnl"],
        "symbol_net_pnl": payload["symbol_summary"]["net_pnl"],
        "db_path": str(DB_PATH),
    }
    print(json.dumps(check_result, ensure_ascii=False, indent=2))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="JoinQuant 策略成交调试看板")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--log", default=None, help="默认日志文件名，例如 1.log")
    parser.add_argument("--code", default=None, help="用于 --check 的股票代码，例如 000878")
    parser.add_argument("--check", action="store_true", help="只做日志和数据库联通性校验，不启动服务")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    if args.check:
        run_cli_check(args.log, args.code)
        return
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()