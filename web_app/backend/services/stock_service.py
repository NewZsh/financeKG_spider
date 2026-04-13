import copy
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .graph_service import find_company_by_stock, get_company_graph
from .stock_directory import ak, stock_directory_service

_MARKET_CACHE_TTL_SECONDS = 10 * 60
_market_cache_lock = threading.Lock()
_market_cache: Dict[str, Dict[str, object]] = {}


def lookup_stock_info(query_type: str, keyword: str):
    return stock_directory_service.lookup(query_type=query_type, keyword=keyword)


def build_stock_graph_payload(stock_info):
    company = find_company_by_stock(stock_info)
    if not company:
        return {
            "matched": True,
            "has_data": False,
            "message": "还没有数据",
            "stock": stock_info,
            "company": None,
            "graph": {"nodes": [], "edges": []},
        }

    graph_data = get_company_graph(company_id=company["id"], hops=2)
    has_data = bool(graph_data["nodes"])
    return {
        "matched": True,
        "has_data": has_data,
        "message": None if has_data else "还没有数据",
        "stock": stock_info,
        "company": company,
        "graph": graph_data,
    }


def get_stock_lookup_payload(query_type: str, keyword: str):
    stock_info = lookup_stock_info(query_type=query_type, keyword=keyword)
    return {
        "matched": bool(stock_info),
        "message": None if stock_info else "未找到匹配的A股公司",
        "stock": stock_info,
    }


def get_stock_graph_payload(query_type: str, keyword: str):
    stock_info = lookup_stock_info(query_type=query_type, keyword=keyword)
    if not stock_info:
        return {
            "matched": False,
            "has_data": False,
            "message": "未找到匹配的A股公司",
            "stock": None,
            "company": None,
            "graph": {"nodes": [], "edges": []},
        }

    return build_stock_graph_payload(stock_info)


def _safe_float(value):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number:
        return None
    return round(number, 4)


def _get_cached_market_payload(symbol: str, allow_stale: bool = False):
    with _market_cache_lock:
        entry = _market_cache.get(symbol)
        if not entry:
            return None

        age = time.time() - float(entry["timestamp"])
        if not allow_stale and age > _MARKET_CACHE_TTL_SECONDS:
            return None

        payload = copy.deepcopy(entry["payload"])
        if allow_stale and age > _MARKET_CACHE_TTL_SECONDS:
            payload["warnings"] = [
                *payload.get("warnings", []),
                f"当前展示的是 {int(age // 60)} 分钟前的缓存行情数据",
            ]
        return payload


def _set_cached_market_payload(symbol: str, payload):
    with _market_cache_lock:
        _market_cache[symbol] = {
            "timestamp": time.time(),
            "payload": copy.deepcopy(payload),
        }


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


def _serialize_daily_rows(dataframe) -> List[Dict[str, Optional[float]]]:
    rows: List[Dict[str, Optional[float]]] = []
    for _, row in dataframe.iterrows():
        date_value = row.get("日期")
        rows.append(
            {
                "date": date_value.strftime("%Y-%m-%d") if hasattr(date_value, "strftime") else str(date_value),
                "open": _safe_float(row.get("开盘")),
                "close": _safe_float(row.get("收盘")),
                "high": _safe_float(row.get("最高")),
                "low": _safe_float(row.get("最低")),
                "volume": _safe_float(row.get("成交量")),
                "amount": _safe_float(row.get("成交额")),
                "amplitude": _safe_float(row.get("振幅")),
                "change_pct": _safe_float(row.get("涨跌幅")),
                "change_amount": _safe_float(row.get("涨跌额")),
                "turnover_rate": _safe_float(row.get("换手率")),
            }
        )
    return rows


def _serialize_intraday_rows(dataframe) -> List[Dict[str, Optional[float]]]:
    rows: List[Dict[str, Optional[float]]] = []
    for _, row in dataframe.iterrows():
        raw_time = row.get("时间")
        timestamp = None
        if raw_time is not None:
            try:
                timestamp = datetime.fromisoformat(str(raw_time))
            except ValueError:
                timestamp = None

        rows.append(
            {
                "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S") if timestamp else str(raw_time),
                "date": timestamp.strftime("%Y-%m-%d") if timestamp else str(raw_time)[:10],
                "time": timestamp.strftime("%H:%M") if timestamp else str(raw_time)[11:16],
                "open": _safe_float(row.get("开盘")),
                "close": _safe_float(row.get("收盘")),
                "high": _safe_float(row.get("最高")),
                "low": _safe_float(row.get("最低")),
                "avg_price": _safe_float(row.get("均价")),
                "volume": _safe_float(row.get("成交量")),
                "amount": _safe_float(row.get("成交额")),
                "change_pct": _safe_float(row.get("涨跌幅")),
                "change_amount": _safe_float(row.get("涨跌额")),
            }
        )
    return rows


def _build_day_distribution(trade_date: str, rows: List[Dict[str, Optional[float]]], bin_count: int = 8):
    valid_rows = [row for row in rows if row.get("close") is not None and row.get("volume") is not None]
    if not valid_rows:
        return None

    prices = [float(row["close"]) for row in valid_rows if row.get("close") is not None]
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

    for row in valid_rows:
        price = float(row["close"])
        volume = float(row["volume"] or 0)
        if effective_bin_count == 1:
            bucket_index = 0
        else:
            bucket_index = min(int((price - low_price) / step), effective_bin_count - 1)

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

    total_volume = sum(float(row.get("volume") or 0) for row in valid_rows)
    total_amount = sum(float(row.get("amount") or 0) for row in valid_rows)
    day_prices = [float(row["close"]) for row in valid_rows]

    return {
        "date": trade_date,
        "summary": {
            "open": valid_rows[0].get("open"),
            "close": valid_rows[-1].get("close"),
            "high": round(max(day_prices), 4),
            "low": round(min(day_prices), 4),
            "total_volume": round(total_volume, 4),
            "total_amount": round(total_amount, 4),
        },
        "buy_sell_bins": buy_sell_bins,
        "price_histogram": price_histogram,
    }


def build_stock_market_payload(stock_info):
    if ak is None:
        raise RuntimeError("akshare is not installed")

    symbol = str(stock_info["code"])
    today = datetime.now()
    cached_payload = _get_cached_market_payload(symbol=symbol)
    if cached_payload:
        return cached_payload

    warnings: List[str] = []
    daily_series: List[Dict[str, Optional[float]]] = []
    candle_windows = {
        "day": [],
        "five_day": [],
        "twenty_day": [],
    }
    intraday_series: List[Dict[str, Optional[float]]] = []
    daily_distributions: List[Dict[str, object]] = []

    try:
        daily_df = _retry_akshare_call(
            lambda: ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date="20210101",
                end_date=(today + timedelta(days=1)).strftime("%Y%m%d"),
                adjust="",
                timeout=15,
            ),
            label="日线行情获取",
        )
        if daily_df is not None and not daily_df.empty:
            daily_df = daily_df.sort_values("日期")
            daily_series = _serialize_daily_rows(daily_df)
            candle_windows["day"] = daily_series[-1:]
            candle_windows["five_day"] = daily_series[-5:]
            candle_windows["twenty_day"] = daily_series[-20:]
        else:
            warnings.append("未获取到日线行情")
    except Exception as exc:
        warnings.append(f"日线行情获取失败: {exc}")

    try:
        intraday_df = _retry_akshare_call(
            lambda: ak.stock_zh_a_hist_min_em(
                symbol=symbol,
                start_date=(today - timedelta(days=14)).strftime("%Y-%m-%d 09:30:00"),
                end_date=(today + timedelta(days=1)).strftime("%Y-%m-%d 15:00:00"),
                period="1",
                adjust="",
            ),
            label="5 日分时行情获取",
        )
        if intraday_df is not None and not intraday_df.empty:
            intraday_df = intraday_df.sort_values("时间")
            intraday_series = _serialize_intraday_rows(intraday_df)

            rows_by_date: Dict[str, List[Dict[str, Optional[float]]]] = defaultdict(list)
            for row in intraday_series:
                trade_date = str(row.get("date") or "").strip()
                if trade_date:
                    rows_by_date[trade_date].append(row)

            for trade_date in sorted(rows_by_date.keys())[-5:]:
                distribution = _build_day_distribution(trade_date=trade_date, rows=rows_by_date[trade_date])
                if distribution:
                    daily_distributions.append(distribution)
        else:
            warnings.append("未获取到 5 日分时行情")
    except Exception as exc:
        warnings.append(f"5 日分时行情获取失败: {exc}")

    payload = {
        "daily_series": daily_series,
        "candle_windows": candle_windows,
        "intraday_series": intraday_series,
        "daily_distributions": daily_distributions,
        "warnings": warnings,
    }

    if daily_series or intraday_series or daily_distributions:
        _set_cached_market_payload(symbol=symbol, payload=payload)
        return payload

    stale_payload = _get_cached_market_payload(symbol=symbol, allow_stale=True)
    if stale_payload:
        stale_payload["warnings"] = [*warnings, *stale_payload.get("warnings", [])]
        return stale_payload

    return payload


def get_stock_detail_payload(query_type: str, keyword: str):
    stock_info = lookup_stock_info(query_type=query_type, keyword=keyword)
    if not stock_info:
        return {
            "matched": False,
            "message": "未找到匹配的A股公司",
            "stock": None,
            "company": None,
            "has_data": False,
            "graph": {"nodes": [], "edges": []},
            "market": {
                "daily_series": [],
                "candle_windows": {"day": [], "five_day": [], "twenty_day": []},
                "intraday_series": [],
                "daily_distributions": [],
                "warnings": [],
            },
        }

    graph_payload = build_stock_graph_payload(stock_info)
    market_payload = build_stock_market_payload(stock_info)
    return {
        **graph_payload,
        "market": market_payload,
    }