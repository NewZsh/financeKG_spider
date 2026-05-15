#!/usr/bin/env python3

"""基于历史日线回放选股信号，并执行事件回测。

参数：
    --start-date: 回测起始日期，格式 YYYYMMDD 或 YYYY-MM-DD
    --end-date: 回测结束日期，格式 YYYYMMDD 或 YYYY-MM-DD
    --top-n: 每天取前 N 个信号参与回测
    --limit: 仅分析前 N 只候选股票，便于调试
    --all-boards: 分析全部板块，不限制默认允许板块
    --max-hold-days: 最大持有天数，0 表示直到触发卖点或数据结束
    --scoring-mode: 评分模式，legacy 或 dedup
    --compare-modes: 同时回测 legacy 和 dedup 两种评分模式
    --initial-capital: 组合回测初始资金
    --position-size: 单笔固定仓位
    --max-positions: 最大同时持仓数

用法：
    - 回测指定时间段：
        python -m stock.backtest --start-date 2024-01-01 --end-date 2024-12-31

    - 每天只取前 20 个信号，并仅分析前 100 只候选股票：
        python -m stock.backtest --top-n 20 --limit 100

    - 同时对比 legacy 和 dedup 两种评分模式：
        python -m stock.backtest --compare-modes

注意：
    - 回测依赖本地 SQLite 中的日线、股票信息和最新流通市值数据
    - 默认使用 dedup 评分模式，以减少重复信号带来的权重膨胀
    - 回测会按历史日期逐日回放，避免使用未来数据
"""

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path
import sqlite3

import pandas as pd

try:
    from compute_indicators import (
        LOOKBACK_DAYS,
        STRATEGY_BREAKOUT_ACCEL,
        STRATEGY_LABELS,
        STRATEGY_TREND_INIT,
        analyze_stock,
        calc_rsi,
    )
    from review_common import (
        ALLOWED_BOARDS,
        MAX_FLOAT_MV,
        MIN_DAILY_AMOUNT,
        MIN_FLOAT_MV,
        REVIEW_DIR,
        get_db_connection,
        parse_review_date,
        to_csv_rows,
    )
except ImportError:
    from .compute_indicators import (
        LOOKBACK_DAYS,
        STRATEGY_BREAKOUT_ACCEL,
        STRATEGY_LABELS,
        STRATEGY_TREND_INIT,
        analyze_stock,
        calc_rsi,
    )
    from .review_common import (
        ALLOWED_BOARDS,
        MAX_FLOAT_MV,
        MIN_DAILY_AMOUNT,
        MIN_FLOAT_MV,
        REVIEW_DIR,
        get_db_connection,
        parse_review_date,
        to_csv_rows,
    )


BACKTEST_DIR = REVIEW_DIR / "backtests"
STOP_LOSS_RATIO = 0.10
TRAILING_PROFIT_ACTIVATION = 0.10
TRAILING_PROFIT_DRAWDOWN = 0.30
TAKE_PROFIT_COOLDOWN_DAYS = 30
DEFAULT_INITIAL_CAPITAL = 100000.0
DEFAULT_POSITION_SIZE = 10000.0
DEFAULT_MAX_POSITIONS = 10
MIN_BACKTEST_HISTORY_ROWS = 20
STRATEGY_FILTER_ALL = "all"
STRATEGY_FILTER_CHOICES = (STRATEGY_FILTER_ALL, STRATEGY_TREND_INIT, STRATEGY_BREAKOUT_ACCEL)
DEFAULT_STRATEGY_FILTER = STRATEGY_FILTER_ALL


class MarketDataCache:
    def __init__(self, conn: sqlite3.Connection, start_trade_date: str | None, end_trade_date: str | None):
        self.stock_info = {}
        print("=> Preloading stock metadata into memory...", flush=True)
        for row in conn.execute("SELECT s.code, s.name, s.board, s.is_st, lmv.float_mv_yi FROM stocks s LEFT JOIN latest_market_value lmv ON s.code = lmv.code").fetchall():
            self.stock_info[str(row["code"])] = {
                "name": row["name"],
                "board": row["board"],
                "is_st": row["is_st"],
                "float_mv_yi": row["float_mv_yi"]
            }
        
        params = []
        date_conditions = []
        if start_trade_date:
            start_dt = pd.Timestamp(start_trade_date) - pd.Timedelta(days=250)
            date_conditions.append("trade_date >= ?")
            params.append(start_dt.strftime("%Y-%m-%d"))
        if end_trade_date:
            date_conditions.append("trade_date <= ?")
            params.append(end_trade_date)
            
        # NOTE: daily_bars now stores volume only; amount is kept as a runtime alias for compatibility.
        query = "SELECT code, trade_date as date, open, high, low, close, volume, volume AS amount FROM daily_bars"
        if date_conditions:
            query += " WHERE " + " AND ".join(date_conditions)
        query += " ORDER BY code, trade_date"
        
        print(f"=> Preloading daily bars [{start_trade_date or 'ALL'} ~ {end_trade_date or 'ALL'}]...", flush=True)
        df = pd.read_sql_query(query, conn, params=params)
        
        print("=> Calculating global sequence indicators (MA5, MA10, MA20, MA60, RSI) via Pandas batch...", flush=True)
        df["ma5"] = df.groupby("code")["close"].transform(lambda x: x.rolling(5).mean())
        df["ma10"] = df.groupby("code")["close"].transform(lambda x: x.rolling(10).mean())
        df["ma20"] = df.groupby("code")["close"].transform(lambda x: x.rolling(20).mean())
        df["ma60"] = df.groupby("code")["close"].transform(lambda x: x.rolling(60).mean())
        df["rsi"] = df.groupby("code")["close"].transform(calc_rsi)

        print("=> Indexing memory slices...", flush=True)
        self.code_dfs = {code: group.reset_index(drop=True) for code, group in df.groupby("code")}
        
        all_dates = list(df['date'].unique())
        all_dates.sort()
        self.review_dates = []
        for d in all_dates:
            if start_trade_date and d < start_trade_date:
                continue
            if end_trade_date and d > end_trade_date:
                continue
            self.review_dates.append(str(d))
            
    def get_future_bars(self, code: str, review_date: str) -> list[dict]:
        df = self.code_dfs.get(code)
        if df is None: return []
        idx = df["date"].searchsorted(review_date, side="right")
        future_df = df.iloc[idx:].copy()
        future_df["trade_date"] = future_df["date"]
        return future_df[["trade_date", "open", "high", "low", "close", "ma20"]].to_dict('records')

    def get_close_lookup(self, codes: set[str], start_date: str, end_date: str) -> dict[tuple[str, str], float]:
        lookup = {}
        for code in codes:
            df = self.code_dfs.get(code)
            if df is None: continue
            subset = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
            for _, row in subset.iterrows():
                lookup[(code, str(row["date"]))] = float(row["close"])
        return lookup


def load_backtest_dates(conn: sqlite3.Connection, start_date: str | None, end_date: str | None) -> list[str]:
    query = """
        SELECT DISTINCT trade_date
        FROM daily_bars
        WHERE 1 = 1
    """
    params: list[object] = []
    conditions: list[str] = []
    if start_date:
        conditions.append("trade_date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("trade_date <= ?")
        params.append(end_date)
    if conditions:
        query += " AND " + " AND ".join(conditions)
    query += " ORDER BY trade_date"
    rows = conn.execute(query, params).fetchall()
    return [str(row[0]) for row in rows]


def build_daily_signals_cached(
    cache: MarketDataCache,
    review_date: str,
    top_n: int,
    limit: int,
    include_all_boards: bool,
    scoring_mode: str,
    is_backtest: bool = False,
    strategy_filter: str | None = None,
) -> list[dict]:
    candidates = []
    count = 0
    for code, df in cache.code_dfs.items():
        if limit > 0 and count >= limit:
            break
            
        info = cache.stock_info.get(code)
        if not info:
            continue
            
        if not is_backtest and info["is_st"]:
            continue
            
        if not include_all_boards and info["board"] not in ALLOWED_BOARDS:
            continue

        if info["is_st"]:
            continue

        idx = df["date"].searchsorted(review_date, side="right")
        if idx < MIN_BACKTEST_HISTORY_ROWS:
            continue
            
        current_bar = df.iloc[idx - 1]
        if current_bar["date"] != review_date:
            continue
            
        volume = float(current_bar["volume"])
        if volume < MIN_DAILY_AMOUNT:
            continue
            
        float_mv = info["float_mv_yi"]
        if not is_backtest:
            if float_mv is not None and not (MIN_FLOAT_MV <= float_mv <= MAX_FLOAT_MV):
                continue
            
        kline = df.iloc[max(0, idx - LOOKBACK_DAYS - 80):idx]
        result = analyze_stock(code, info["name"], kline, scoring_mode=scoring_mode)
        count += 1
        if result:
            result["board"] = info["board"]
            result["open"] = round(float(current_bar["open"]), 2)
            result["high"] = round(float(current_bar["high"]), 2)
            result["low"] = round(float(current_bar["low"]), 2)
            result["float_mv_yi"] = round(float(float_mv), 2) if float_mv is not None else None
            result["volume"] = round(float(volume), 2)
            result["has_pullback_shrink_twice"] = has_pullback_shrink_twice_signal(result)
            if not matches_strategy_filter(result, strategy_filter):
                continue
            candidates.append(result)

    candidates.sort(key=lambda item: item["score"], reverse=True)
    return candidates[:top_n]


def load_backtest_candidates(
    conn: sqlite3.Connection,
    review_date: str,
    include_all_boards: bool,
) -> list[sqlite3.Row]:
    board_condition = ""
    params: list[object] = [review_date, review_date]
    if not include_all_boards:
        placeholders = ",".join("?" for _ in ALLOWED_BOARDS)
        board_condition = f"AND s.board IN ({placeholders})"
        params.extend(ALLOWED_BOARDS)

    query = f"""
        WITH day_bars AS (
            SELECT
                code,
                trade_date,
                open,
                high,
                low,
                close,
                volume,
                LAG(close) OVER (PARTITION BY code ORDER BY trade_date) AS previous_close_calc
            FROM daily_bars
            WHERE trade_date <= ?
        ),
        latest_day_bars AS (
            SELECT *
            FROM day_bars
            WHERE trade_date = ?
        )
        SELECT
            s.code,
            s.name,
            s.board,
            ldb.close AS price,
            ldb.open,
            ldb.high,
            ldb.low,
            CASE
                WHEN ldb.previous_close_calc IS NOT NULL AND ldb.previous_close_calc > 0 THEN ((ldb.close / ldb.previous_close_calc) - 1) * 100
                ELSE NULL
            END AS pct_change,
            ldb.volume AS volume,
            lmv.float_mv_yi
        FROM stocks s
        INNER JOIN latest_day_bars ldb ON ldb.code = s.code
        LEFT JOIN latest_market_value lmv ON lmv.code = s.code
        WHERE s.is_st = 0
          {board_condition}
                    AND ldb.volume >= ?
          AND (lmv.float_mv_yi IS NULL OR lmv.float_mv_yi BETWEEN ? AND ?)
        ORDER BY s.code
    """
    params.extend([MIN_DAILY_AMOUNT, MIN_FLOAT_MV, MAX_FLOAT_MV])
    return conn.execute(query, params).fetchall()


def has_pullback_shrink_twice_signal(signal: dict) -> bool:
    for item in signal.get("signals", []):
        if not item:
            continue
        name = item[0]
        if isinstance(name, str) and name.startswith("60日内两次缩量下跌"):
            return True
    return False


def matches_strategy_filter(signal: dict, strategy_filter: str | None) -> bool:
    if strategy_filter in (None, STRATEGY_FILTER_ALL):
        return True
    return strategy_filter in (signal.get("strategy_setups") or [])


def get_strategy_filter_label(strategy_filter: str | None) -> str:
    if strategy_filter in (None, STRATEGY_FILTER_ALL):
        return "全部买入策略"
    return STRATEGY_LABELS.get(strategy_filter, str(strategy_filter))


def load_qfq_bars_as_of(
    conn: sqlite3.Connection,
    code: str,
    review_date: str,
    required_rows: int = LOOKBACK_DAYS + 80,
) -> pd.DataFrame:
    query = """
        SELECT trade_date AS date, open, high, low, close, volume, volume AS amount
        FROM daily_bars
        WHERE code = ? AND trade_date <= ?
        ORDER BY trade_date
    """
    df = pd.read_sql_query(query, conn, params=[code, review_date])
    if df.empty:
        return df
    return df.tail(required_rows)


def build_daily_signals(
    conn: sqlite3.Connection,
    review_date: str,
    top_n: int,
    limit: int,
    include_all_boards: bool,
    scoring_mode: str,
    strategy_filter: str | None = None,
) -> list[dict]:
    candidates = load_backtest_candidates(conn, review_date, include_all_boards)
    if limit > 0:
        candidates = candidates[:limit]

    results: list[dict] = []
    for row in candidates:
        kline = load_qfq_bars_as_of(conn, row["code"], review_date)
        if kline.empty or len(kline) < MIN_BACKTEST_HISTORY_ROWS:
            continue
        result = analyze_stock(row["code"], row["name"], kline, scoring_mode=scoring_mode)
        if result is None:
            continue
        result["board"] = row["board"]
        result["open"] = round(float(row["open"]), 2) if row["open"] is not None else None
        result["high"] = round(float(row["high"]), 2) if row["high"] is not None else None
        result["low"] = round(float(row["low"]), 2) if row["low"] is not None else None
        result["float_mv_yi"] = round(float(row["float_mv_yi"]), 2) if row["float_mv_yi"] is not None else None
        result["volume"] = round(float(row["volume"]), 2) if row["volume"] is not None else None
        result["has_pullback_shrink_twice"] = has_pullback_shrink_twice_signal(result)
        if not matches_strategy_filter(result, strategy_filter):
            continue
        results.append(result)

    results.sort(key=lambda item: item["score"], reverse=True)
    return results[:top_n]


def load_future_bars(conn: sqlite3.Connection, code: str, review_date: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT trade_date, open, high, low, close
        FROM daily_bars
        WHERE code = ? AND trade_date > ?
        ORDER BY trade_date
        """,
        (code, review_date),
    ).fetchall()


def build_entry_plan(review_date: str, signal: dict, future_bars: list[sqlite3.Row | dict]) -> dict | None:
    primary_strategy = signal.get("primary_strategy")
    if primary_strategy == STRATEGY_BREAKOUT_ACCEL:
        if not future_bars:
            return None
        confirm_bar = future_bars[0]
        confirm_close = float(confirm_bar["close"])
        signal_high = float(signal.get("high") or signal["close"])
        if confirm_close <= signal_high:
            return None
        return {
            "entry_date": str(confirm_bar["trade_date"]),
            "entry_price": round(confirm_close, 4),
            "bars": future_bars[1:],
            "entry_rule": "next_day_confirm_breakout",
        }

    return {
        "entry_date": review_date,
        "entry_price": round(float(signal["close"]), 4),
        "bars": future_bars,
        "entry_rule": "signal_close",
    }


def simulate_trade_from_bars(
    entry_date: str,
    entry_price: float,
    bars: list[sqlite3.Row | dict],
    max_hold_days: int = 0,
) -> dict:
    stop_loss_price = entry_price * (1 - STOP_LOSS_RATIO)
    max_profit = 0.0
    bars_to_process = bars if max_hold_days <= 0 else bars[:max_hold_days]

    for bar in bars_to_process:
        trade_date = str(bar["trade_date"])
        low_price = float(bar["low"])
        close_price = float(bar["close"])
        ma20_price = float(bar["ma20"]) if "ma20" in dict(bar) and not pd.isna(bar["ma20"]) else 0.0

        if low_price <= stop_loss_price:
            return {
                "entry_date": entry_date,
                "exit_date": trade_date,
                "exit_price": round(stop_loss_price, 4),
                "return_pct": round(-STOP_LOSS_RATIO * 100, 2),
                "holding_days": len(bars_to_process[: bars_to_process.index(bar) + 1]),
                "exit_reason": "stop_loss",
                "max_profit_pct": round(max_profit * 100, 2),
            }

        historical_max_profit = max_profit
        current_close_profit = close_price / entry_price - 1

        # 新策略：不再看 30% 回撤，只要收盘价跌破 MA20 就止盈/止损出局
        if ma20_price > 0 and close_price < ma20_price:
            return {
                "entry_date": entry_date,
                "exit_date": trade_date,
                "exit_price": round(close_price, 4),
                "return_pct": round(current_close_profit * 100, 2),
                "holding_days": len(bars_to_process[: bars_to_process.index(bar) + 1]),
                "exit_reason": "break_ma20",
                "max_profit_pct": round(historical_max_profit * 100, 2),
            }

        if current_close_profit > max_profit:
            max_profit = current_close_profit

    if not bars_to_process:
        return {
            "entry_date": entry_date,
            "exit_date": entry_date,
            "exit_price": round(entry_price, 4),
            "return_pct": 0.0,
            "holding_days": 0,
            "exit_reason": "no_future_bar",
            "max_profit_pct": 0.0,
        }

    last_bar = bars_to_process[-1]
    final_close = float(last_bar["close"])
    final_return = final_close / entry_price - 1
    return {
        "entry_date": entry_date,
        "exit_date": str(last_bar["trade_date"]),
        "exit_price": round(final_close, 4),
        "return_pct": round(final_return * 100, 2),
        "holding_days": len(bars_to_process),
        "exit_reason": "end_of_data" if max_hold_days <= 0 else "max_hold_days",
        "max_profit_pct": round(max_profit * 100, 2),
    }


def simulate_trade(
    cache: MarketDataCache,
    review_date: str,
    signal: dict,
    max_hold_days: int = 0,
) -> dict | None:
    future_bars = cache.get_future_bars(signal["code"], review_date)
    entry_plan = build_entry_plan(review_date, signal, future_bars)
    if entry_plan is None:
        return None
    entry_price = float(entry_plan["entry_price"])
    raw_result = simulate_trade_from_bars(
        entry_plan["entry_date"],
        entry_price,
        entry_plan["bars"],
        max_hold_days=max_hold_days,
    )
    signal_names = [item[0] for item in signal["signals"]]
    return {
        "entry_date": raw_result["entry_date"],
        "exit_date": raw_result["exit_date"],
        "entry_price": round(entry_price, 4),
        "exit_price": raw_result["exit_price"],
        "return_pct": raw_result["return_pct"],
        "holding_days": raw_result["holding_days"],
        "exit_reason": raw_result["exit_reason"],
        "max_profit_pct": raw_result["max_profit_pct"],
        "code": signal["code"],
        "name": signal["name"],
        "board": signal["board"],
        "score": signal["score"],
        "cross_type": signal["cross_type_cn"],
        "signals": "; ".join(signal_names),
        "has_pullback_shrink_twice": any(name.startswith("60日内两次缩量下跌") for name in signal_names),
        "strategy_setups": ", ".join(signal.get("strategy_labels", [])),
        "primary_strategy": signal.get("primary_strategy"),
        "primary_strategy_label": signal.get("primary_strategy_label"),
        "entry_rule": entry_plan["entry_rule"],
    }


def summarize_trades(trades: list[dict]) -> dict:
    if not trades:
        return {
            "trade_count": 0,
            "win_rate": 0.0,
            "avg_return_pct": 0.0,
            "median_return_pct": 0.0,
            "avg_holding_days": 0.0,
            "best_trade_pct": 0.0,
            "worst_trade_pct": 0.0,
            "take_profit_count": 0,
            "stop_loss_count": 0,
        }

    returns = pd.Series([item["return_pct"] for item in trades], dtype=float)
    holding_days = pd.Series([item["holding_days"] for item in trades], dtype=float)
    return {
        "trade_count": len(trades),
        "win_rate": round(float((returns > 0).mean() * 100), 2),
        "avg_return_pct": round(float(returns.mean()), 2),
        "median_return_pct": round(float(returns.median()), 2),
        "avg_holding_days": round(float(holding_days.mean()), 2),
        "best_trade_pct": round(float(returns.max()), 2),
        "worst_trade_pct": round(float(returns.min()), 2),
        "take_profit_count": sum(1 for item in trades if item["exit_reason"] == "break_ma20" and item["return_pct"] > 0),
        "stop_loss_count": sum(1 for item in trades if item["exit_reason"] in ("stop_loss", "break_ma20") and item["return_pct"] <= 0),
    }


def should_skip_signal_for_cooldown(review_date: str, signal: dict, cooldown_until_by_code: dict[str, str]) -> bool:
    cooldown_until = cooldown_until_by_code.get(str(signal["code"]))
    if not cooldown_until:
        return False
    return review_date <= cooldown_until


def build_close_lookup(conn: sqlite3.Connection, codes: list[str], start_date: str, end_date: str) -> dict[tuple[str, str], float]:
    if not codes:
        return {}

    placeholders = ",".join("?" for _ in codes)
    rows = conn.execute(
        f"""
        SELECT code, trade_date, close
        FROM daily_bars
                WHERE code IN ({placeholders})
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date, code
        """,
        [*codes, start_date, end_date],
    ).fetchall()
    return {(str(row["code"]), str(row["trade_date"])): float(row["close"]) for row in rows}


def summarize_equity_curve(curve_rows: list[dict], executed_trades: list[dict], skipped_trades: list[dict]) -> dict:
    if not curve_rows:
        return {
            "initial_capital": 0.0,
            "final_equity": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "executed_trade_count": 0,
            "skipped_trade_count": 0,
        }

    equity_series = pd.Series([row["equity"] for row in curve_rows], dtype=float)
    running_peak = equity_series.cummax()
    drawdown = (equity_series / running_peak - 1.0) * 100
    initial_capital = float(curve_rows[0]["initial_capital"])
    final_equity = float(curve_rows[-1]["equity"])
    total_return_pct = 0.0 if initial_capital == 0 else (final_equity / initial_capital - 1.0) * 100
    return {
        "initial_capital": round(initial_capital, 2),
        "final_equity": round(final_equity, 2),
        "total_return_pct": round(float(total_return_pct), 2),
        "max_drawdown_pct": round(abs(float(drawdown.min())), 2),
        "executed_trade_count": len(executed_trades),
        "skipped_trade_count": len(skipped_trades),
    }


def build_portfolio_equity_curve(
    trades: list[dict],
    trading_dates: list[str],
    close_lookup: dict[tuple[str, str], float],
    initial_capital: float = DEFAULT_INITIAL_CAPITAL,
    position_size: float = DEFAULT_POSITION_SIZE,
    max_positions: int = DEFAULT_MAX_POSITIONS,
) -> tuple[list[dict], list[dict], list[dict], dict]:
    entries_by_date: dict[str, list[dict]] = {}
    exits_by_date: dict[str, list[dict]] = {}
    for trade in sorted(trades, key=lambda item: (item["entry_date"], -float(item.get("score", 0)), str(item["code"]))):
        entries_by_date.setdefault(str(trade["entry_date"]), []).append(trade)
        exits_by_date.setdefault(str(trade["exit_date"]), []).append(trade)

    cash = float(initial_capital)
    open_positions: dict[tuple[str, str], dict] = {}
    executed_trades: list[dict] = []
    skipped_trades: list[dict] = []
    curve_rows: list[dict] = []
    realized_pnl = 0.0

    total_dates = len(trading_dates)
    for i, trade_date in enumerate(trading_dates):
        for trade in exits_by_date.get(trade_date, []):
            position_key = (str(trade["code"]), str(trade["entry_date"]))
            position = open_positions.pop(position_key, None)
            if position is None:
                continue
            exit_value = position["shares"] * float(trade["exit_price"])
            
            # 手续费和印花税计算
            sell_commission = max(5.0, exit_value * 0.0003) # 万3，最低5元
            stamp_tax = exit_value * 0.001 # 印花税千1
            net_exit_value = exit_value - sell_commission - stamp_tax
            
            pnl_amount = net_exit_value - position["cost"]
            cash += net_exit_value
            realized_pnl += pnl_amount
            position["executed_exit_price"] = float(trade["exit_price"])
            position["realized_pnl"] = round(pnl_amount, 2)
            position["sell_commission"] = round(sell_commission, 2)
            position["stamp_tax"] = round(stamp_tax, 2)

        for trade in entries_by_date.get(trade_date, []):
            if any(p["code"] == trade["code"] for p in open_positions.values()):
                skipped_trades.append({**trade, "skip_reason": "already_holding"})
                continue
            
            # 买入手续费万3，最低5元
            buy_commission = max(5.0, position_size * 0.0003)
            total_cost = position_size + buy_commission

            if len(open_positions) >= max_positions or cash < total_cost:
                skipped_trades.append({**trade, "skip_reason": "capital_or_position_limit"})
                continue
            
            entry_price = float(trade["entry_price"])
            shares = int(position_size / entry_price / 100) * 100 # 按手数（100股）取整向下
            if shares == 0:
                skipped_trades.append({**trade, "skip_reason": "insufficient_capital_for_100_shares"})
                continue
            
            actual_position_size = shares * entry_price
            buy_commission = max(5.0, actual_position_size * 0.0003)
            actual_total_cost = actual_position_size + buy_commission
            
            cash -= actual_total_cost
            position_key = (str(trade["code"]), str(trade["entry_date"]))
            open_positions[position_key] = {
                **trade,
                "shares": shares,
                "cost": actual_total_cost,
                "buy_commission": round(buy_commission, 2)
            }
            executed_trades.append({**trade, "allocated_capital": round(actual_total_cost, 2), "shares": shares})

        market_value = 0.0
        for position in open_positions.values():
            close_price = close_lookup.get((str(position["code"]), trade_date))
            if close_price is None:
                close_price = float(position["entry_price"])
            market_value += position["shares"] * close_price

        equity = cash + market_value
        pct = (equity / initial_capital) * 100
        print(f"\rAllocating Portfolio: {i + 1}/{total_dates} ({trade_date}) - Equity: {pct:.2f}%", end="", flush=True)

        curve_rows.append(
            {
                "date": trade_date,
                "initial_capital": round(initial_capital, 2),
                "cash": round(cash, 2),
                "market_value": round(market_value, 2),
                "equity": round(equity, 2),
                "open_positions": len(open_positions),
                "realized_pnl": round(realized_pnl, 2),
            }
        )
    print()

    portfolio_summary = summarize_equity_curve(curve_rows, executed_trades, skipped_trades)
    return executed_trades, skipped_trades, curve_rows, portfolio_summary


def write_backtest_markdown(output_path: Path, summary: dict, shrink_summary: dict, plain_summary: dict, notes: list[str]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Backtest {summary['start_date']} - {summary['end_date']}",
        "",
        f"- 回测交易数: {summary['overall']['trade_count']}",
        f"- 胜率: {summary['overall']['win_rate']}%",
        f"- 平均收益: {summary['overall']['avg_return_pct']}%",
        f"- 中位数收益: {summary['overall']['median_return_pct']}%",
        f"- 平均持有天数: {summary['overall']['avg_holding_days']}",
        f"- 止盈触发次数: {summary['overall']['take_profit_count']}",
        f"- 止损触发次数: {summary['overall']['stop_loss_count']}",
        f"- 评分模式: {summary['scoring_mode']}",
        f"- 买入策略: {get_strategy_filter_label(summary['strategy_filter'])}",
        "",
        "## 组合资金曲线",
        "",
        f"- 初始资金: {summary['portfolio']['initial_capital']}",
        f"- 最终权益: {summary['portfolio']['final_equity']}",
        f"- 组合收益率: {summary['portfolio']['total_return_pct']}%",
        f"- 最大回撤: {summary['portfolio']['max_drawdown_pct']}%",
        f"- 实际执行交易数: {summary['portfolio']['executed_trade_count']}",
        f"- 因资金或仓位限制跳过交易数: {summary['portfolio']['skipped_trade_count']}",
        "",
        "## 分组对比",
        "",
        "| 分组 | 交易数 | 胜率 | 平均收益 | 中位数收益 | 平均持有天数 |",
        "| --- | --- | --- | --- | --- | --- |",
        f"| 含两次缩量下跌 | {shrink_summary['trade_count']} | {shrink_summary['win_rate']}% | {shrink_summary['avg_return_pct']}% | {shrink_summary['median_return_pct']}% | {shrink_summary['avg_holding_days']} |",
        f"| 不含两次缩量下跌 | {plain_summary['trade_count']} | {plain_summary['win_rate']}% | {plain_summary['avg_return_pct']}% | {plain_summary['median_return_pct']}% | {plain_summary['avg_holding_days']} |",
        "",
        "## 回测假设",
        "",
    ]
    for note in notes:
        lines.append(f"- {note}")
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def write_compare_markdown(output_path: Path, compare_summary: dict) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    legacy = compare_summary["legacy"]
    dedup = compare_summary["dedup"]
    lines = [
        f"# Score Mode Compare {compare_summary['start_date']} - {compare_summary['end_date']}",
        "",
        "| 模式 | 信号交易数 | 胜率 | 平均收益 | 组合收益率 | 最大回撤 | 最终权益 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
        f"| legacy | {legacy['overall']['trade_count']} | {legacy['overall']['win_rate']}% | {legacy['overall']['avg_return_pct']}% | {legacy['portfolio']['total_return_pct']}% | {legacy['portfolio']['max_drawdown_pct']}% | {legacy['portfolio']['final_equity']} |",
        f"| dedup | {dedup['overall']['trade_count']} | {dedup['overall']['win_rate']}% | {dedup['overall']['avg_return_pct']}% | {dedup['portfolio']['total_return_pct']}% | {dedup['portfolio']['max_drawdown_pct']}% | {dedup['portfolio']['final_equity']} |",
        "",
        "- legacy: 原始累计打分。",
        "- dedup: 基础分 + 分组封顶修正分，压缩趋势确认类与位置质量类的重复加分。",
    ]
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def write_strategy_compare_markdown(output_path: Path, compare_summary: dict) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    trend_init = compare_summary[STRATEGY_TREND_INIT]
    breakout_accel = compare_summary[STRATEGY_BREAKOUT_ACCEL]
    lines = [
        f"# Strategy Compare {compare_summary['start_date']} - {compare_summary['end_date']}",
        "",
        f"- 评分模式: {compare_summary['scoring_mode']}",
        "",
        "| 策略 | 信号交易数 | 胜率 | 平均收益 | 组合收益率 | 最大回撤 | 最终权益 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
        f"| {get_strategy_filter_label(STRATEGY_TREND_INIT)} | {trend_init['overall']['trade_count']} | {trend_init['overall']['win_rate']}% | {trend_init['overall']['avg_return_pct']}% | {trend_init['portfolio']['total_return_pct']}% | {trend_init['portfolio']['max_drawdown_pct']}% | {trend_init['portfolio']['final_equity']} |",
        f"| {get_strategy_filter_label(STRATEGY_BREAKOUT_ACCEL)} | {breakout_accel['overall']['trade_count']} | {breakout_accel['overall']['win_rate']}% | {breakout_accel['overall']['avg_return_pct']}% | {breakout_accel['portfolio']['total_return_pct']}% | {breakout_accel['portfolio']['max_drawdown_pct']}% | {breakout_accel['portfolio']['final_equity']} |",
        "",
        f"- {get_strategy_filter_label(STRATEGY_TREND_INIT)}: 缩量回踩、低乖离、守 MA20、重新放量后按信号当日收盘介入。",
        f"- {get_strategy_filter_label(STRATEGY_BREAKOUT_ACCEL)}: 二次金叉且前高附近放量，需次日收盘确认站上信号日高点后介入。",
    ]
    output_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")


def run_backtest(
    start_date: str | None = None,
    end_date: str | None = None,
    top_n: int = 10,
    limit: int = 0,
    include_all_boards: bool = False,
    max_hold_days: int = 0,
    scoring_mode: str = "dedup",
    initial_capital: float = DEFAULT_INITIAL_CAPITAL,
    position_size: float = DEFAULT_POSITION_SIZE,
    max_positions: int = DEFAULT_MAX_POSITIONS,
    cache: MarketDataCache | None = None,
    strategy_filter: str = DEFAULT_STRATEGY_FILTER,
) -> dict:
    start_trade_date = parse_review_date(start_date)[0] if start_date else None
    end_trade_date = parse_review_date(end_date)[0] if end_date else None
    
    if cache is None:
        conn = get_db_connection()
        cache = MarketDataCache(conn, start_trade_date, end_trade_date)
        conn.close()
        
    review_dates = cache.review_dates

    trades: list[dict] = []
    signal_rows: list[dict] = []
    cooldown_until_by_code: dict[str, str] = {}
    holding_until_by_code: dict[str, str] = {}
    total_dates = len(review_dates)
    for i, review_date in enumerate(review_dates):
        print(f"\rScanning Signals [{scoring_mode}]: {i + 1}/{total_dates} {review_date} - Matched: {len(trades)} trades", end="", flush=True)
        signals = build_daily_signals_cached(
            cache,
            review_date,
            top_n=top_n,
            limit=limit,
            include_all_boards=include_all_boards,
            scoring_mode=scoring_mode,
            is_backtest=True,
            strategy_filter=strategy_filter,
        )
        for signal in signals:
            code_str = str(signal["code"])
            # 如果这只股票处于被持仓的状态，则在卖出之前不再响应它的新信号
            if holding_until_by_code.get(code_str) and review_date <= holding_until_by_code[code_str]:
                continue
            if should_skip_signal_for_cooldown(review_date, signal, cooldown_until_by_code):
                continue
            signal_rows.append({"run_date": review_date, **signal})
            trade = simulate_trade(cache, review_date, signal, max_hold_days=max_hold_days)
            if trade is None:
                continue
            trades.append(trade)
            
            # 记录这笔独立交易的下车时间，在它下车前不再买它
            holding_until_by_code[code_str] = str(trade["exit_date"])
            
            if trade["exit_reason"] == "break_ma20" and trade["return_pct"] > 0:
                cooldown_until = (pd.Timestamp(trade["exit_date"]) + timedelta(days=TAKE_PROFIT_COOLDOWN_DAYS)).strftime("%Y-%m-%d")
                cooldown_until_by_code[str(trade["code"])] = cooldown_until
    print()  # 换行结束进度条显示

    overall_summary = summarize_trades(trades)
    shrink_trades = [item for item in trades if item["has_pullback_shrink_twice"]]
    plain_trades = [item for item in trades if not item["has_pullback_shrink_twice"]]
    shrink_summary = summarize_trades(shrink_trades)
    plain_summary = summarize_trades(plain_trades)

    effective_start = review_dates[0] if review_dates else start_trade_date or "na"
    effective_end = review_dates[-1] if review_dates else end_trade_date or "na"
    strategy_suffix = strategy_filter or STRATEGY_FILTER_ALL
    file_suffix = f"{strategy_suffix}_{effective_start.replace('-', '')}_{effective_end.replace('-', '')}"
    portfolio_end_date = max((str(item["exit_date"]) for item in trades), default=effective_end)
    close_lookup = cache.get_close_lookup(set([str(item["code"]) for item in trades]), effective_start, portfolio_end_date)
    portfolio_trading_dates = [d for d in cache.review_dates if effective_start <= d <= portfolio_end_date]
    
    executed_trades, skipped_trades, equity_curve_rows, portfolio_summary = build_portfolio_equity_curve(
        trades,
        portfolio_trading_dates,
        close_lookup,
        initial_capital=initial_capital,
        position_size=position_size,
        max_positions=max_positions,
    )

    trade_csv_path = BACKTEST_DIR / f"backtest_trades_{scoring_mode}_{file_suffix}.csv"
    signal_csv_path = BACKTEST_DIR / f"backtest_signals_{scoring_mode}_{file_suffix}.csv"
    equity_csv_path = BACKTEST_DIR / f"backtest_equity_{scoring_mode}_{file_suffix}.csv"
    markdown_path = BACKTEST_DIR / f"backtest_{scoring_mode}_{file_suffix}.md"

    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(trades).to_csv(trade_csv_path, index=False, encoding="utf-8-sig")
    signal_export_rows = []
    for row in signal_rows:
        export_row = {"run_date": row["run_date"], **to_csv_rows([row])[0]}
        signal_export_rows.append(export_row)
    pd.DataFrame(signal_export_rows).to_csv(signal_csv_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(equity_curve_rows).to_csv(equity_csv_path, index=False, encoding="utf-8-sig")

    summary = {
        "start_date": effective_start,
        "end_date": effective_end,
        "scoring_mode": scoring_mode,
        "strategy_filter": strategy_filter,
        "overall": overall_summary,
        "portfolio": portfolio_summary,
        "with_pullback_shrink_twice": shrink_summary,
        "without_pullback_shrink_twice": plain_summary,
        "trade_csv_path": str(trade_csv_path),
        "signal_csv_path": str(signal_csv_path),
        "equity_csv_path": str(equity_csv_path),
        "markdown_path": str(markdown_path),
        "executed_trade_count": len(executed_trades),
        "skipped_trade_count": len(skipped_trades),
    }
    notes = [
        "趋势建立初期型：信号当日收盘价买入，只在尾盘买入。",
        "突破加速型：信号出现后需次日收盘确认站上信号日高点，再按次日收盘价买入。",
        "止损使用本金回撤 10%，一旦后续日线最低价触及止损价，按止损价卖出。",
        "出场规则改为趋势止盈/止损：只要后续任一交易日收盘价跌破 MA20，即按当日收盘价离场。",
        f"若股票因跌破 MA20 获利离场，则自卖出日开始 {TAKE_PROFIT_COOLDOWN_DAYS} 天内不再重复买入。",
        f"本次回测买入策略过滤：{get_strategy_filter_label(strategy_filter)}。",
        "同一只股票在前一笔交易尚未出场前，不会响应新的重复信号。",
        "回测候选直接使用 stocks + daily_bars 还原历史当日样本；成交量过滤保持一致，流通市值过滤仅在 float_mv_yi 可用时生效。",
        "若直到数据末尾仍未触发卖出，则按最后一个可用收盘价平仓。",
        f"组合资金曲线使用初始资金 {initial_capital}、单笔仓位 {position_size}、最大持仓数 {max_positions}。",
    ]
    write_backtest_markdown(markdown_path, summary, shrink_summary, plain_summary, notes)
    return summary


def compare_scoring_modes(
    start_date: str | None = None,
    end_date: str | None = None,
    top_n: int = 10,
    limit: int = 0,
    include_all_boards: bool = False,
    max_hold_days: int = 0,
    initial_capital: float = DEFAULT_INITIAL_CAPITAL,
    position_size: float = DEFAULT_POSITION_SIZE,
    max_positions: int = DEFAULT_MAX_POSITIONS,
    strategy_filter: str = DEFAULT_STRATEGY_FILTER,
) -> dict:
    start_trade_date = parse_review_date(start_date)[0] if start_date else None
    end_trade_date = parse_review_date(end_date)[0] if end_date else None
    
    conn = get_db_connection()
    cache = MarketDataCache(conn, start_trade_date, end_trade_date)
    conn.close()
    
    legacy_summary = run_backtest(
        start_date=start_date,
        end_date=end_date,
        top_n=top_n,
        limit=limit,
        include_all_boards=include_all_boards,
        max_hold_days=max_hold_days,
        scoring_mode="legacy",
        initial_capital=initial_capital,
        position_size=position_size,
        max_positions=max_positions,
        cache=cache,
        strategy_filter=strategy_filter,
    )
    dedup_summary = run_backtest(
        start_date=start_date,
        end_date=end_date,
        top_n=top_n,
        limit=limit,
        include_all_boards=include_all_boards,
        max_hold_days=max_hold_days,
        scoring_mode="dedup",
        initial_capital=initial_capital,
        position_size=position_size,
        max_positions=max_positions,
        cache=cache,
        strategy_filter=strategy_filter,
    )
    compare_summary = {
        "start_date": legacy_summary["start_date"],
        "end_date": legacy_summary["end_date"],
        "legacy": legacy_summary,
        "dedup": dedup_summary,
    }
    compare_path = BACKTEST_DIR / (
        f"backtest_compare_{legacy_summary['start_date'].replace('-', '')}_{legacy_summary['end_date'].replace('-', '')}.md"
    )
    write_compare_markdown(compare_path, compare_summary)
    compare_summary["compare_markdown_path"] = str(compare_path)
    return compare_summary


def compare_strategies(
    start_date: str | None = None,
    end_date: str | None = None,
    top_n: int = 10,
    limit: int = 0,
    include_all_boards: bool = False,
    max_hold_days: int = 0,
    scoring_mode: str = "dedup",
    initial_capital: float = DEFAULT_INITIAL_CAPITAL,
    position_size: float = DEFAULT_POSITION_SIZE,
    max_positions: int = DEFAULT_MAX_POSITIONS,
) -> dict:
    start_trade_date = parse_review_date(start_date)[0] if start_date else None
    end_trade_date = parse_review_date(end_date)[0] if end_date else None

    conn = get_db_connection()
    cache = MarketDataCache(conn, start_trade_date, end_trade_date)
    conn.close()

    trend_init_summary = run_backtest(
        start_date=start_date,
        end_date=end_date,
        top_n=top_n,
        limit=limit,
        include_all_boards=include_all_boards,
        max_hold_days=max_hold_days,
        scoring_mode=scoring_mode,
        initial_capital=initial_capital,
        position_size=position_size,
        max_positions=max_positions,
        cache=cache,
        strategy_filter=STRATEGY_TREND_INIT,
    )
    breakout_accel_summary = run_backtest(
        start_date=start_date,
        end_date=end_date,
        top_n=top_n,
        limit=limit,
        include_all_boards=include_all_boards,
        max_hold_days=max_hold_days,
        scoring_mode=scoring_mode,
        initial_capital=initial_capital,
        position_size=position_size,
        max_positions=max_positions,
        cache=cache,
        strategy_filter=STRATEGY_BREAKOUT_ACCEL,
    )
    compare_summary = {
        "start_date": trend_init_summary["start_date"],
        "end_date": trend_init_summary["end_date"],
        "scoring_mode": scoring_mode,
        STRATEGY_TREND_INIT: trend_init_summary,
        STRATEGY_BREAKOUT_ACCEL: breakout_accel_summary,
    }
    compare_path = BACKTEST_DIR / (
        f"backtest_strategy_compare_{scoring_mode}_{trend_init_summary['start_date'].replace('-', '')}_{trend_init_summary['end_date'].replace('-', '')}.md"
    )
    write_strategy_compare_markdown(compare_path, compare_summary)
    compare_summary["compare_markdown_path"] = str(compare_path)
    return compare_summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="基于历史日线回放选股信号并执行事件回测")
    parser.add_argument("--start-date", type=str, default=None, help="起始日期，格式 YYYYMMDD 或 YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=None, help="结束日期，格式 YYYYMMDD 或 YYYY-MM-DD")
    parser.add_argument("--top-n", type=int, default=10, help="每天取前 N 个信号做回测")
    parser.add_argument("--limit", type=int, default=0, help="仅分析前 N 只候选股票")
    parser.add_argument("--all-boards", action="store_true", help="分析全部板块")
    parser.add_argument("--max-hold-days", type=int, default=0, help="最大持有天数，0 表示直到触发卖点或数据结束")
    parser.add_argument("--scoring-mode", choices=["legacy", "dedup"], default="dedup", help="评分模式")
    parser.add_argument("--compare-modes", action="store_true", help="同时回测 legacy 和 dedup 两种评分模式")
    parser.add_argument("--strategy-filter", choices=list(STRATEGY_FILTER_CHOICES), default=DEFAULT_STRATEGY_FILTER, help="买入策略过滤：all、trend_init、breakout_accel")
    parser.add_argument("--compare-strategies", action="store_true", help="同时回测趋势建立初期型与突破加速型")
    parser.add_argument("--initial-capital", type=float, default=DEFAULT_INITIAL_CAPITAL, help="组合回测初始资金")
    parser.add_argument("--position-size", type=float, default=DEFAULT_POSITION_SIZE, help="单笔固定仓位")
    parser.add_argument("--max-positions", type=int, default=DEFAULT_MAX_POSITIONS, help="最大同时持仓数")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    if args.compare_strategies:
        summary = compare_strategies(
            start_date=args.start_date,
            end_date=args.end_date,
            top_n=args.top_n,
            limit=args.limit,
            include_all_boards=args.all_boards,
            max_hold_days=args.max_hold_days,
            scoring_mode=args.scoring_mode,
            initial_capital=args.initial_capital,
            position_size=args.position_size,
            max_positions=args.max_positions,
        )
        print(
            f"策略对比回测完成: {summary['start_date']} -> {summary['end_date']}, "
            f"{get_strategy_filter_label(STRATEGY_TREND_INIT)} 组合收益 {summary[STRATEGY_TREND_INIT]['portfolio']['total_return_pct']}%, "
            f"{get_strategy_filter_label(STRATEGY_BREAKOUT_ACCEL)} 组合收益 {summary[STRATEGY_BREAKOUT_ACCEL]['portfolio']['total_return_pct']}%"
        )
        print(f"对比摘要: {summary['compare_markdown_path']}")
        return

    if args.compare_modes:
        summary = compare_scoring_modes(
            start_date=args.start_date,
            end_date=args.end_date,
            top_n=args.top_n,
            limit=args.limit,
            include_all_boards=args.all_boards,
            max_hold_days=args.max_hold_days,
            initial_capital=args.initial_capital,
            position_size=args.position_size,
            max_positions=args.max_positions,
            strategy_filter=args.strategy_filter,
        )
        print(
            f"对比回测完成: {summary['start_date']} -> {summary['end_date']}, "
            f"legacy 组合收益 {summary['legacy']['portfolio']['total_return_pct']}%, "
            f"dedup 组合收益 {summary['dedup']['portfolio']['total_return_pct']}%"
        )
        print(f"对比摘要: {summary['compare_markdown_path']}")
        return

    summary = run_backtest(
        start_date=args.start_date,
        end_date=args.end_date,
        top_n=args.top_n,
        limit=args.limit,
        include_all_boards=args.all_boards,
        max_hold_days=args.max_hold_days,
        scoring_mode=args.scoring_mode,
        initial_capital=args.initial_capital,
        position_size=args.position_size,
        max_positions=args.max_positions,
        strategy_filter=args.strategy_filter,
    )
    print(
        f"回测完成: {summary['start_date']} -> {summary['end_date']}, "
        f"模式 {summary['scoring_mode']}, 策略 {get_strategy_filter_label(summary['strategy_filter'])}, 交易 {summary['overall']['trade_count']} 笔, "
        f"组合收益 {summary['portfolio']['total_return_pct']}%"
    )
    print(f"交易明细: {summary['trade_csv_path']}")
    print(f"信号明细: {summary['signal_csv_path']}")
    print(f"资金曲线: {summary['equity_csv_path']}")
    print(f"摘要: {summary['markdown_path']}")


if __name__ == "__main__":
    main()