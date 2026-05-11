"""同步 A 股行情到 SQLite，并从 DB 读取前端所需的市场数据载荷。

参数：
    --start-date: 同步起始日期，格式 YYYY-MM-DD；不指定时默认使用 FULL_REFRESH_START
    --end-date: 同步结束日期，格式 YYYY-MM-DD；不指定时默认使用当天
    --limit: 仅同步前 N 只股票，便于调试，默认为 0（不限制）

用法：
    - 同步某一天的数据：
        python -m stock.sync_market_data --start-date 2026-05-01 --end-date 2026-05-01

    - 从指定起点同步到当天：
        python -m stock.sync_market_data --start-date 2026-05-01

    - 不指定日期时，从 FULL_REFRESH_START 同步到当天：
        python -m stock.sync_market_data

注意：
    - 市值只同步本日最新的，每次运行都会刷新
    - 日线会检查是否价格一致，如果不一致，应当是触发了复权，就会重新拉取该股票的全部日线数据（不区分前后复权），以保证数据一致性
    - 日线的时间支持用户自己指定，通过 --start-date 和 --end-date 参数控制，默认会从 FULL_REFRESH_START 同步到当天
    - 分时数据拉当天向前 INTRADAY_FETCH_WINDOW_DAYS 天的，前端展示时会根据日期过滤到 INTRADAY_LOOKBACK_DAYS 天内的，尽量覆盖节假日和周末带来的日期间隔问题
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import pickle
import random
import sqlite3
import sys
import threading
import time
import socket
import mplfinance as mpf
from datetime import datetime, timedelta
from pathlib import Path

# 设置全局默认的 socket 超时时间，防止网络请求无限期挂起
socket.setdefaulttimeout(60.0)

from typing import Dict, List, Optional, Any

import akshare
import pandas as pd
import requests
from requests.adapters import HTTPAdapter

# 进程内前端载荷缓存的有效期。
PAYLOAD_CACHE_TTL_SECONDS = 10 * 60
FULL_REFRESH_START = "2020-01-01"  # 从历史上改日开始拉取数据，建立前复权日线
# 腾讯快照接口支持批量代码，单次请求尽量按批处理，避免过细碎请求。
TENCENT_BATCH_SIZE = 50
# 读取时保留最近多少个交易日的分时和价格分布数据。
INTRADAY_LOOKBACK_DAYS = 5
# 分时同步会多取一段时间窗口，尽量覆盖节假日和周末带来的日期间隔。
INTRADAY_FETCH_WINDOW_DAYS = 14
STOCK_LIST_CACHE_TTL_SECONDS = 12 * 60 * 60
DAILY_BARS_CACHE_TTL_SECONDS = 12 * 60 * 60
INTRADAY_CACHE_TTL_SECONDS = 60 * 60
RATE_LIMIT_INTERVALS = {
    "stock_list": 1.0,
    "daily_bars": 0.5,
    "intraday": 0.5,
    "tencent": 0.15,
}

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SCHEMA_PATH = DATA_DIR / "schema" / "stock.sql"
DB_PATH = DATA_DIR / "stock.db"
MARKET_CACHE_DIR = DATA_DIR / "cache" / "market"

BOARD_CATEGORIES = {
    "主板-沪（60）": lambda code: code.startswith("60"),
    "主板-深（00）": lambda code: code.startswith("000") or code.startswith("001"),
    "中小板（002/003）": lambda code: code.startswith("002") or code.startswith("003"),
    "创业板（300）": lambda code: code.startswith("30"),
    "科创板（688）": lambda code: code.startswith("688"),
    "北交所（8/4/9）": lambda code: code.startswith(("8", "4", "9")),
}

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_cli_date(value: Optional[str], *, fallback: Optional[datetime] = None) -> str:
    if value is None:
        if fallback is None:
            raise ValueError("date value is required")
        return fallback.strftime("%Y-%m-%d")
    return datetime.strptime(value.strip(), "%Y-%m-%d").strftime("%Y-%m-%d")


def normalize_trade_date(value: Any) -> str:
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def classify_board(code: str) -> str:
    for name, check in BOARD_CATEGORIES.items():
        if check(code):
            return name
    return "其他"


def is_st(name: str) -> bool:
    if not isinstance(name, str):
        return True
    return "ST" in name.upper() or "*ST" in name


def to_symbol(code: str) -> str:
    return f"sh{code}" if code.startswith(("6", "688")) else f"sz{code}"


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


def fill_daily_amount(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "volume" not in df.columns:
        return df
    data = df.copy()
    data["volume"] = pd.to_numeric(data["volume"], errors="coerce")
    data["amount"] = data["volume"]
    return data


def retry_call(fetcher, *, label: str, attempts: int = 3, delay_seconds: float = 1.2) -> Any:
    """具备重试机制的数据抓取包装器"""
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return fetcher()
        except Exception as exc:
            last_error = exc
            sys.stdout.write(f"\n[网络提醒] {label} 请求异常 (第{attempt}/{attempts}次重试) - {exc}\n")
            sys.stdout.flush()
            if attempt == attempts:
                break
            if not is_retryable_fetch_error(exc):
                break
            jitter = random.uniform(0.0, delay_seconds * 0.35)
            time.sleep(delay_seconds * (2 ** (attempt - 1)) + jitter)
    raise RuntimeError(f"{label}在重试 {attempts} 次后仍失败: {last_error}") from last_error


def is_retryable_fetch_error(exc: Exception) -> bool:
    if isinstance(exc, (requests.RequestException, TimeoutError, ConnectionError)):
        return True
    message = str(exc).lower()
    retryable_fragments = (
        "timed out",
        "timeout",
        "temporarily unavailable",
        "remote end closed connection",
        "connection aborted",
        "connection reset",
        "429",
        "502",
        "503",
        "504",
        "proxyerror",
        "ssleoferror",
    )
    return any(fragment in message for fragment in retryable_fragments)


def load_pickle_cache(cache_path: Path, ttl_seconds: int) -> Any:
    if not cache_path.exists():
        return None
    if time.time() - cache_path.stat().st_mtime > ttl_seconds:
        return None
    try:
        with cache_path.open("rb") as cache_file:
            return pickle.load(cache_file)
    except Exception:
        return None


def load_pickle_cache_any_age(cache_path: Path) -> Any:
    if not cache_path.exists():
        return None
    try:
        with cache_path.open("rb") as cache_file:
            return pickle.load(cache_file)
    except Exception:
        return None


def save_pickle_cache(cache_path: Path, data: Any) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with cache_path.open("wb") as cache_file:
            pickle.dump(data, cache_file)
    except Exception:
        return


class ApiRateLimiter:
    _lock = threading.Lock()
    _last_called_at: Dict[str, float] = {}

    @classmethod
    def wait(cls, key: str) -> None:
        interval = RATE_LIMIT_INTERVALS.get(key, 0.0)
        if interval <= 0:
            return
        with cls._lock:
            now = time.monotonic()
            last_called_at = cls._last_called_at.get(key, 0.0)
            wait_seconds = interval - (now - last_called_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)
                now = time.monotonic()
            cls._last_called_at[key] = now


class SharedHttpClient:
    _lock = threading.RLock()
    _session: Optional[requests.Session] = None

    @classmethod
    def get_session(cls) -> requests.Session:
        with cls._lock:
            if cls._session is None:
                session = requests.Session()
                adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=0)
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                session.headers.update(
                    {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                        "Connection": "keep-alive",
                    }
                )
                cls._session = session
            return cls._session

    @classmethod
    def get(cls, url: str, **kwargs) -> requests.Response:
        return cls.get_session().get(url, **kwargs)


class DbManager:
    """管理单库 stock.db 的连接与初始化。"""

    @staticmethod
    def ensure_legacy_tables_dropped(conn: sqlite3.Connection) -> None:
        conn.execute("DROP INDEX IF EXISTS idx_corporate_actions_code_date")
        conn.execute("DROP TABLE IF EXISTS corporate_actions")

    @staticmethod
    def ensure_intraday_schema(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS intraday_bars (
                code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                trade_time TEXT NOT NULL,
                trade_timestamp TEXT NOT NULL,
                open REAL,
                close REAL,
                high REAL,
                low REAL,
                avg_price REAL,
                volume REAL,
                amount REAL,
                change_pct REAL,
                change_amount REAL,
                PRIMARY KEY (code, trade_timestamp)
            );

            CREATE INDEX IF NOT EXISTS idx_intraday_bars_code_date_time
                ON intraday_bars(code, trade_date DESC, trade_timestamp DESC);

            CREATE TABLE IF NOT EXISTS daily_price_distributions (
                code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                buy_sell_bins_json TEXT NOT NULL,
                price_histogram_json TEXT NOT NULL,
                PRIMARY KEY (code, trade_date)
            );

            CREATE INDEX IF NOT EXISTS idx_daily_price_distributions_code_date
                ON daily_price_distributions(code, trade_date DESC);
            """
        )
        intraday_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(intraday_bars)").fetchall()}
        if "fetched_at" in intraday_columns:
            conn.execute("ALTER TABLE intraday_bars RENAME TO intraday_bars_legacy_migrating")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS intraday_bars (
                    code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    trade_time TEXT NOT NULL,
                    trade_timestamp TEXT NOT NULL,
                    open REAL,
                    close REAL,
                    high REAL,
                    low REAL,
                    avg_price REAL,
                    volume REAL,
                    amount REAL,
                    change_pct REAL,
                    change_amount REAL,
                    PRIMARY KEY (code, trade_timestamp),
                    FOREIGN KEY (code) REFERENCES stocks(code)
                );

                CREATE INDEX IF NOT EXISTS idx_intraday_bars_code_date_time
                    ON intraday_bars(code, trade_date DESC, trade_timestamp DESC);
                """
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO intraday_bars(
                    code, trade_date, trade_time, trade_timestamp, open, close, high, low,
                    avg_price, volume, amount, change_pct, change_amount
                )
                SELECT
                    code, trade_date, trade_time, trade_timestamp, open, close, high, low,
                    avg_price, volume, amount, change_pct, change_amount
                FROM intraday_bars_legacy_migrating
                """
            )
            conn.execute("DROP TABLE intraday_bars_legacy_migrating")

    @staticmethod
    def ensure_latest_market_value_schema(conn: sqlite3.Connection) -> None:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(latest_market_value)").fetchall()}
        if "amount_wan" not in columns:
            if "fetched_at" in columns:
                conn.execute("ALTER TABLE latest_market_value RENAME TO latest_market_value_legacy_migrating")
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS latest_market_value (
                        code TEXT PRIMARY KEY,
                        trade_date TEXT NOT NULL,
                        amount_wan REAL,
                        float_mv_yi REAL NOT NULL,
                        FOREIGN KEY (code) REFERENCES stocks(code)
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO latest_market_value(code, trade_date, amount_wan, float_mv_yi)
                    SELECT code, trade_date, NULL AS amount_wan, float_mv_yi
                    FROM latest_market_value_legacy_migrating
                    """
                )
                conn.execute("DROP TABLE latest_market_value_legacy_migrating")
            else:
                conn.execute("ALTER TABLE latest_market_value ADD COLUMN amount_wan REAL")
        elif "fetched_at" in columns:
            conn.execute("ALTER TABLE latest_market_value RENAME TO latest_market_value_legacy_migrating")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS latest_market_value (
                    code TEXT PRIMARY KEY,
                    trade_date TEXT NOT NULL,
                    amount_wan REAL,
                    float_mv_yi REAL NOT NULL,
                    FOREIGN KEY (code) REFERENCES stocks(code)
                );
                """
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO latest_market_value(code, trade_date, amount_wan, float_mv_yi)
                SELECT code, trade_date, NULL AS amount_wan, float_mv_yi
                FROM latest_market_value_legacy_migrating
                """
            )
            conn.execute("DROP TABLE latest_market_value_legacy_migrating")

    @staticmethod
    def ensure_daily_bar_schema(conn: sqlite3.Connection) -> None:
        table_row = conn.execute(
            "SELECT type FROM sqlite_master WHERE name = 'daily_bars'"
        ).fetchone()
        if table_row and str(table_row["type"]) == "view":
            conn.execute("DROP VIEW daily_bars")
            conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

        legacy_year_tables = [
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name GLOB 'daily_bars_[0-9][0-9][0-9][0-9]'"
            ).fetchall()
        ]
        if legacy_year_tables:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_bars_tmp_migrating (
                    code TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    PRIMARY KEY (code, trade_date)
                )
                """
            )
            for table_name in legacy_year_tables:
                columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
                if "adjust_type" in columns:
                    conn.execute(
                        f"""
                        INSERT OR REPLACE INTO daily_bars_tmp_migrating(code, trade_date, open, high, low, close, volume)
                        SELECT code, trade_date, open, high, low, close, volume
                        FROM {table_name}
                        WHERE adjust_type = 'qfq'
                        """
                    )
                else:
                    conn.execute(
                        f"""
                        INSERT OR REPLACE INTO daily_bars_tmp_migrating(code, trade_date, open, high, low, close, volume)
                        SELECT code, trade_date, open, high, low, close, volume
                        FROM {table_name}
                        """
                    )
                conn.execute(f"DROP TABLE {table_name}")
            conn.execute("DELETE FROM daily_bars")
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_bars(code, trade_date, open, high, low, close, volume)
                SELECT code, trade_date, open, high, low, close, volume
                FROM daily_bars_tmp_migrating
                """
            )
            conn.execute("DROP TABLE daily_bars_tmp_migrating")

        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(daily_bars)").fetchall()}
        if "adjust_type" in columns:
            conn.execute("ALTER TABLE daily_bars RENAME TO daily_bars_legacy_migrating")
            conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_bars(code, trade_date, open, high, low, close, volume)
                SELECT code, trade_date, open, high, low, close, volume
                FROM daily_bars_legacy_migrating
                WHERE adjust_type = 'qfq'
                """
            )
            conn.execute("DROP TABLE daily_bars_legacy_migrating")
        elif "amount" in columns:
            conn.execute("ALTER TABLE daily_bars RENAME TO daily_bars_legacy_migrating")
            conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_bars(code, trade_date, open, high, low, close, volume)
                SELECT code, trade_date, open, high, low, close, volume
                FROM daily_bars_legacy_migrating
                """
            )
            conn.execute("DROP TABLE daily_bars_legacy_migrating")

    @staticmethod
    def ensure_daily_distribution_schema(conn: sqlite3.Connection) -> None:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(daily_price_distributions)").fetchall()}
        if "summary_json" not in columns and "fetched_at" not in columns:
            return
        conn.execute("ALTER TABLE daily_price_distributions RENAME TO daily_price_distributions_legacy_migrating")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS daily_price_distributions (
                code TEXT NOT NULL,
                trade_date TEXT NOT NULL,
                buy_sell_bins_json TEXT NOT NULL,
                price_histogram_json TEXT NOT NULL,
                PRIMARY KEY (code, trade_date),
                FOREIGN KEY (code) REFERENCES stocks(code)
            );

            CREATE INDEX IF NOT EXISTS idx_daily_price_distributions_code_date
                ON daily_price_distributions(code, trade_date DESC);
            """
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO daily_price_distributions(code, trade_date, buy_sell_bins_json, price_histogram_json)
            SELECT code, trade_date, buy_sell_bins_json, price_histogram_json
            FROM daily_price_distributions_legacy_migrating
            """
        )
        conn.execute("DROP TABLE daily_price_distributions_legacy_migrating")

    @staticmethod
    def get_connection() -> sqlite3.Connection:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
        conn.row_factory = sqlite3.Row
        daily_bars_kind = conn.execute("SELECT type FROM sqlite_master WHERE name = 'daily_bars'").fetchone()
        if daily_bars_kind and str(daily_bars_kind[0]) == "view":
            conn.execute("DROP VIEW daily_bars")
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        DbManager.ensure_legacy_tables_dropped(conn)
        DbManager.ensure_latest_market_value_schema(conn)
        DbManager.ensure_intraday_schema(conn)
        DbManager.ensure_daily_bar_schema(conn)
        DbManager.ensure_daily_distribution_schema(conn)
        return conn

class MarketDataFetcher:
    """封装同步链路所需的市场数据抓取逻辑。"""

    @staticmethod
    def _cache_path(name: str) -> Path:
        safe_name = hashlib.md5(name.encode("utf-8")).hexdigest()
        return MARKET_CACHE_DIR / f"{safe_name}.pkl"

    @staticmethod
    def _load_stale_cache(cache_path: Path, label: str) -> Any:
        cached = load_pickle_cache_any_age(cache_path)
        if cached is None:
            return None
        sys.stdout.write(f"\n[缓存回退] {label} 使用最近一次成功缓存\n")
        sys.stdout.flush()
        return cached

    @staticmethod
    def _load_stock_codes_from_db() -> pd.DataFrame:
        if not DB_PATH.exists():
            return pd.DataFrame(columns=["code", "name", "board", "is_st"])
        conn = sqlite3.connect(DB_PATH)
        try:
            return pd.read_sql_query("SELECT code, name, board, is_st FROM stocks ORDER BY code", conn)
        except Exception:
            return pd.DataFrame(columns=["code", "name", "board", "is_st"])
        finally:
            conn.close()
    
    @staticmethod
    def get_all_stock_codes() -> pd.DataFrame:
        cache_path = MarketDataFetcher._cache_path("stock_info_a_code_name")
        cached = load_pickle_cache(cache_path, STOCK_LIST_CACHE_TTL_SECONDS)
        if isinstance(cached, pd.DataFrame) and not cached.empty:
            return cached.copy()

        ApiRateLimiter.wait("stock_list")
        try:
            df = retry_call(lambda: akshare.stock_info_a_code_name(), label="A股股票列表获取", attempts=4, delay_seconds=1.0)
        except Exception:
            stale_cached = MarketDataFetcher._load_stale_cache(cache_path, "A股股票列表获取")
            if isinstance(stale_cached, pd.DataFrame) and not stale_cached.empty:
                return stale_cached.copy()
            db_cached = MarketDataFetcher._load_stock_codes_from_db()
            if not db_cached.empty:
                sys.stdout.write("\n[缓存回退] A股股票列表获取 使用数据库中的现有股票表\n")
                sys.stdout.flush()
                return db_cached.copy()
            raise

        df["board"] = df["code"].apply(classify_board)
        df["is_st"] = df["name"].apply(is_st).astype(int)
        result = df[["code", "name", "board", "is_st"]].copy()
        save_pickle_cache(cache_path, result)
        return result

    @staticmethod
    def fetch_tencent_realtime(codes: List[str]) -> Dict[str, dict]:
        result: Dict[str, dict] = {}
        tencent_codes = [to_symbol(code) for code in codes]
        for index in range(0, len(tencent_codes), TENCENT_BATCH_SIZE):
            batch = tencent_codes[index:index + TENCENT_BATCH_SIZE]
            try:
                ApiRateLimiter.wait("tencent")
                response = retry_call(
                    lambda: SharedHttpClient.get(f"https://qt.gtimg.cn/q={','.join(batch)}", timeout=15),
                    label=f"腾讯快照获取[{batch[0]}..]",
                    attempts=3,
                    delay_seconds=0.8,
                )
                if response.status_code != 200:
                    continue
                for line in [item for item in response.text.strip().split(";") if "~" in item]:
                    parts = line.split("~")
                    if len(parts) < 50:
                        continue
                    code_str = parts[2]
                    try:
                        amount_wan = float(parts[37]) if parts[37] else 0
                        float_mv_yi = float(parts[44]) if parts[44] else 0
                    except (TypeError, ValueError):
                        continue
                    result[code_str] = {
                        "name": parts[1],
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
        cache_path = MarketDataFetcher._cache_path(f"daily_bars:{code}:{start_date}:{end_date}:{adjust}")
        cached = load_pickle_cache(cache_path, DAILY_BARS_CACHE_TTL_SECONDS)
        if isinstance(cached, pd.DataFrame) and not cached.empty:
            return fill_daily_amount(cached.copy())
            
        symbol = to_symbol(code)
        adjust_type = "qfq" if adjust == "qfq" else ""
        limit = 640
        all_data = []
        current_end = end_date
        
        while True:
            ApiRateLimiter.wait("daily_bars")
            url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_dayqfq&param={symbol},day,{start_date},{current_end},{limit},{adjust_type}"
            def do_fetch():
                resp = SharedHttpClient.get(url, timeout=15)
                if resp.status_code != 200:
                    raise RuntimeError("HTTP failed")
                return resp.text
            
            try:
                text = retry_call(do_fetch, label=f"{code} 腾讯日线({current_end})", attempts=4, delay_seconds=1.0)
                json_str = text.split("=", 1)[1]
                data = json.loads(json_str)
                k_data = data.get("data", {}).get(symbol, {})
                day_key = "qfqday" if adjust_type == "qfq" and "qfqday" in k_data else "day"
                days = k_data.get(day_key, [])
            except Exception as e:
                sys.stdout.write(f"\n[获取日线拉取错误] {e}\n")
                break
                
            if not days:
                break
                
            all_data.extend(reversed(days))  # Collect in reverse to keep track
            first_day = days[0][0]
            if first_day <= start_date or len(days) < limit:
                break
            
            # Request next page
            try:
                dt = datetime.strptime(first_day, "%Y-%m-%d") - timedelta(days=1)
                current_end = dt.strftime("%Y-%m-%d")
            except ValueError:
                break
                
        if not all_data:
            cached = MarketDataFetcher._load_stale_cache(cache_path, f"{code} 日线行情获取({adjust})")
            if isinstance(cached, pd.DataFrame) and not cached.empty:
                return fill_daily_amount(cached.copy())
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
            
        # Re-reverse to chronological order
        all_data = list(reversed(all_data))
        
        records = []
        for row in all_data:
            if len(row) >= 6:
                records.append({
                    "date": row[0],
                    "open": float(row[1]),
                    "close": float(row[2]),
                    "high": float(row[3]),
                    "low": float(row[4]),
                    "volume": float(row[5]),
                })
        
        df = pd.DataFrame(records)
        df = fill_daily_amount(df)
        normalized = MarketDataFetcher._normalize_daily_bars_df(df)
        if not normalized.empty:
            save_pickle_cache(cache_path, normalized)
        return normalized

    @staticmethod
    def fetch_intraday_bars(code: str, start_datetime: str, end_datetime: str) -> pd.DataFrame:
        cache_path = MarketDataFetcher._cache_path(f"intraday:{code}:{start_datetime}:{end_datetime}")
        cached = load_pickle_cache(cache_path, INTRADAY_CACHE_TTL_SECONDS)
        if isinstance(cached, pd.DataFrame) and not cached.empty:
            return cached.copy()
            
        ApiRateLimiter.wait("intraday")
        symbol = to_symbol(code)
        url = f"https://web.ifzq.gtimg.cn/appstock/app/minute/query?code={symbol}"
        def do_fetch():
            resp = SharedHttpClient.get(url, timeout=15)
            if resp.status_code != 200:
                raise RuntimeError("HTTP failed")
            return resp.json()
        
        try:
            data = retry_call(do_fetch, label=f"{code} 腾讯分时", attempts=4, delay_seconds=1.2)
            k_data = data.get("data", {}).get(symbol, {})
            qt_data = k_data.get("qt", {}).get(symbol, [])
            if len(qt_data) > 4:
                y_close = float(qt_data[4])
            else:
                y_close = 0.0
            raw_minutes = k_data.get("data", {}).get("data", [])
        except Exception:
            raw_minutes = []
            y_close = 0.0
            
        if not raw_minutes:
            cached = MarketDataFetcher._load_stale_cache(cache_path, f"{code} 分时行情获取")
            if isinstance(cached, pd.DataFrame) and not cached.empty:
                return cached.copy()
            return MarketDataFetcher._normalize_intraday_df(pd.DataFrame())
            
        records = []
        prev_vol = 0
        prev_amt = 0.0
        
        # Parse date from qt_data if possible (qt_data[30] usually holds datetime)
        date_str = None
        if len(qt_data) > 30:
            date_val = qt_data[30][:8]
            if len(date_val) == 8:
                date_str = f"{date_val[:4]}-{date_val[4:6]}-{date_val[6:8]}"
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")
            
        for item in raw_minutes:
            parts = item.split(" ")
            if len(parts) < 3: continue
            try:
                hhmm = parts[0]
                price = float(parts[1])
                cum_vol = int(parts[2])
                cum_amt = float(parts[3]) if len(parts) > 3 else 0.0
            except (ValueError, TypeError):
                continue
                
            vol = cum_vol - prev_vol
            amt = cum_amt - prev_amt
            prev_vol = cum_vol
            prev_amt = cum_amt
            
            records.append({
                "trade_timestamp": f"{date_str} {hhmm[:2]}:{hhmm[2:]}:00",
                "open": price,
                "close": price,
                "high": price,
                "low": price,
                "volume": vol,
                "amount": amt,
                "change_pct": round((price - y_close) / y_close * 100, 4) if y_close else 0.0,
                "change_amount": round(price - y_close, 4) if y_close else 0.0,
                "avg_price": price
            })
            
        df = pd.DataFrame(records)
        normalized = MarketDataFetcher._normalize_intraday_df(df)
        if not normalized.empty:
            save_pickle_cache(cache_path, normalized)
        return normalized

    @staticmethod
    def fetch_recent_intraday_bars(code: str, lookback_days: int = INTRADAY_LOOKBACK_DAYS) -> pd.DataFrame:
        cache_path = MarketDataFetcher._cache_path(f"intraday_recent:{code}:{lookback_days}")
        cached = load_pickle_cache(cache_path, INTRADAY_CACHE_TTL_SECONDS)
        if isinstance(cached, pd.DataFrame) and not cached.empty:
            return cached.copy()

        ApiRateLimiter.wait("intraday")
        symbol = to_symbol(code)
        url = f"https://web.ifzq.gtimg.cn/appstock/app/day/query?code={symbol}"

        def do_fetch():
            resp = SharedHttpClient.get(url, timeout=15)
            if resp.status_code != 200:
                raise RuntimeError("HTTP failed")
            return resp.json()

        try:
            data = retry_call(do_fetch, label=f"{code} 腾讯五日分时", attempts=4, delay_seconds=1.2)
            day_entries = data.get("data", {}).get(symbol, {}).get("data", [])
        except Exception:
            day_entries = []

        records = []
        if day_entries:
            recent_entries = sorted(
                [entry for entry in day_entries if entry.get("date")],
                key=lambda entry: str(entry.get("date")),
            )[-lookback_days:]

            for entry in recent_entries:
                date_text = str(entry.get("date") or "")
                if len(date_text) != 8:
                    continue
                date_str = f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:8]}"
                try:
                    y_close = float(entry.get("prec") or 0.0)
                except (TypeError, ValueError):
                    y_close = 0.0

                prev_vol = 0
                prev_amt = 0.0
                for item in entry.get("data", []):
                    parts = str(item).split()
                    if len(parts) < 3:
                        continue
                    try:
                        hhmm = parts[0]
                        price = float(parts[1])
                        cum_vol = int(parts[2])
                        cum_amt = float(parts[3]) if len(parts) > 3 else 0.0
                    except (TypeError, ValueError):
                        continue

                    vol = cum_vol - prev_vol
                    amt = cum_amt - prev_amt
                    prev_vol = cum_vol
                    prev_amt = cum_amt

                    records.append({
                        "trade_timestamp": f"{date_str} {hhmm[:2]}:{hhmm[2:]}:00",
                        "open": price,
                        "close": price,
                        "high": price,
                        "low": price,
                        "volume": vol,
                        "amount": amt,
                        "change_pct": round((price - y_close) / y_close * 100, 4) if y_close else 0.0,
                        "change_amount": round(price - y_close, 4) if y_close else 0.0,
                        "avg_price": price,
                    })

        df = pd.DataFrame(records)
        normalized = MarketDataFetcher._normalize_intraday_df(df)
        if not normalized.empty:
            save_pickle_cache(cache_path, normalized)
            return normalized

        today = datetime.now().strftime("%Y-%m-%d")
        fallback = MarketDataFetcher.fetch_intraday_bars(code, f"{today} 09:30:00", f"{today} 15:00:00")
        if not fallback.empty:
            save_pickle_cache(cache_path, fallback)
        return fallback

    @staticmethod
    def _normalize_daily_bars_df(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
        if "date" not in df.columns and "日期" not in df.columns:
            df = df.reset_index()
            if "date" not in df.columns and "index" in df.columns:
                df = df.rename(columns={"index": "date"})
        rename_map = {"vol": "volume", "成交量": "volume", "成交额": "amount", "日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low"}
        df = df.rename(columns=rename_map)
        needed_columns = ["date", "open", "high", "low", "close", "volume", "amount"]
        missing = [col for col in needed_columns if col not in df.columns]
        if missing:
            raise ValueError(f"K line data missing columns: {missing}")
        data = df[needed_columns].copy()
        data["date"] = data["date"].apply(normalize_trade_date)
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
    行情同步引擎，按串行方式同步全市场股票数据。
    """
    def __init__(
        self,
        limit: int = 0,
        request_pause: float = 0.15,
    ):
        self.limit = limit
        self.request_pause = request_pause

    def _resolve_first_trade_day_on_or_after(self, value: str) -> str:
        current = datetime.strptime(value, "%Y-%m-%d")
        while current.weekday() >= 5:
            current += timedelta(days=1)
        return current.strftime("%Y-%m-%d")

    def run_sync(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> dict:
        """主同步逻辑"""
        end_trade_date = parse_cli_date(end_date, fallback=datetime.now())
        start_trade_date = parse_cli_date(start_date, fallback=datetime.strptime(FULL_REFRESH_START, "%Y-%m-%d"))

        intraday_anchor_dt = datetime.now()
        intra_start_date = (intraday_anchor_dt - timedelta(days=INTRADAY_FETCH_WINDOW_DAYS)).strftime("%Y-%m-%d")
        intra_start = f"{intra_start_date} 09:30:00"
        intra_end = (intraday_anchor_dt + timedelta(days=1)).strftime("%Y-%m-%d 15:00:00")

        self.summary = {
            "review_date": f"{start_trade_date} ~ {end_trade_date}",
            "db_path": str(DB_PATH),
            "universe_count": 0, 
            "target_universe_count": 0,
            "synced_codes": 0, 
            "daily_bar_rows": 0,
            "intraday_bar_rows": 0, 
            "distribution_rows": 0,
            "full_refresh_codes": [], 
            "coverage_backfill_codes": [],
            "adjustment_event_updates": 0, 
            "skipped_daily_codes": 0,
            "retry_attempted_codes": 0,
            "retry_recovered_codes": 0,
            "errors": []
        }

        conn = DbManager.get_connection()
        try:
            stocks_df = MarketDataFetcher.get_all_stock_codes()
            self.summary["universe_count"] = self._upsert_stocks(conn, stocks_df)

            sync_df = stocks_df.head(self.limit).copy() if self.limit > 0 else stocks_df.copy()
            self.summary["target_universe_count"] = len(sync_df)

            # 盘中图固定取最近 5 个交易日，腾讯快照仍用于刷新最新流通市值。
            tencent_spot = MarketDataFetcher.fetch_tencent_realtime(sync_df["code"].tolist())
            total_tasks = len(sync_df)
            completed_tasks = 0
            failed_rows: List[tuple[Any, str]] = []

            for _, row in sync_df.iterrows():
                StockMarketDataReader.clear_cache(str(row["code"]))

            for row in sync_df.itertuples(index=False):
                code_str = str(row.code)

                error_message = self._process_single_stock(
                    conn=conn, code=code_str, start_trade_date=start_trade_date, end_trade_date=end_trade_date,
                    intra_start=intra_start, intra_end=intra_end,
                    tencent_spot=tencent_spot,
                    enable_spot_enrichment=True,
                    enable_intraday=True,
                )
                if error_message:
                    failed_rows.append((row, error_message))
                completed_tasks += 1
                sys.stdout.write(f"\r同步进度: [{completed_tasks}/{total_tasks}] {completed_tasks/total_tasks*100:.1f}%")
                sys.stdout.flush()

            if failed_rows:
                self.summary["retry_attempted_codes"] = len(failed_rows)
                sys.stdout.write(f"\n开始重试失败股票: {len(failed_rows)} 只\n")
                sys.stdout.flush()
                time.sleep(1.5)
                for row, first_error in failed_rows:
                    retry_error = self._process_single_stock(
                        conn=conn, code=str(row.code), start_trade_date=start_trade_date, end_trade_date=end_trade_date,
                        intra_start=intra_start, intra_end=intra_end,
                        tencent_spot=tencent_spot,
                        enable_spot_enrichment=True,
                        enable_intraday=True,
                    )
                    if retry_error:
                        self.summary["errors"].append(retry_error)
                    else:
                        self.summary["retry_recovered_codes"] += 1

            sys.stdout.write("\n")
            return self.summary
        finally:
            conn.close()

    def _process_single_stock(self, conn: sqlite3.Connection, code: str, start_trade_date: str, end_trade_date: str,
                              intra_start: str, intra_end: str, tencent_spot: dict,
                              enable_spot_enrichment: bool, enable_intraday: bool):
        error_message = None
        try:
            spot = tencent_spot.get(code) if enable_spot_enrichment else None
            current_market_value_date = datetime.now().strftime("%Y-%m-%d")
            if spot and spot.get("float_mv_yi"):
                self._upsert_latest_market_value(
                    conn,
                    code,
                    current_market_value_date,
                    spot.get("amount_wan"),
                    spot["float_mv_yi"],
                )

            coverage_row = self._get_daily_bar_coverage(conn, code)
            oldest_db_record = conn.execute(
                "SELECT trade_date, close FROM daily_bars WHERE code = ? ORDER BY trade_date ASC LIMIT 1",
                (code,)
            ).fetchone()

            signature_changed = False
            effective_start_trade_date = self._resolve_first_trade_day_on_or_after(start_trade_date)

            if oldest_db_record:
                anchor_date = str(oldest_db_record["trade_date"])
                db_anchor_close = float(oldest_db_record["close"])
                
                anchor_df = MarketDataFetcher.fetch_daily_bars(code, start_date=anchor_date, end_date=anchor_date, adjust="qfq")
                if not anchor_df.empty:
                    api_anchor_close = float(anchor_df.iloc[0]["close"])
                    if abs(api_anchor_close - db_anchor_close) > 0.01:
                        signature_changed = True

            coverage_matches = bool(
                coverage_row
                and not signature_changed
                and str(coverage_row["coverage_start_date"]) <= effective_start_trade_date
                and str(coverage_row["coverage_end_date"]) >= end_trade_date
            )

            if signature_changed:
                self.summary["adjustment_event_updates"] += 1

            if coverage_matches:
                self.summary["skipped_daily_codes"] += 1
            else:
                needs_full = signature_changed or (
                    start_trade_date == FULL_REFRESH_START
                    and (not coverage_row or str(coverage_row["coverage_start_date"]) > effective_start_trade_date)
                )
                fetch_start_date = FULL_REFRESH_START if needs_full else start_trade_date
                if not needs_full and coverage_row:
                    coverage_end_date = str(coverage_row["coverage_end_date"])
                    if coverage_end_date >= start_trade_date:
                        fetch_start_date = (
                            datetime.strptime(coverage_end_date, "%Y-%m-%d") + timedelta(days=1)
                        ).strftime("%Y-%m-%d")

                if fetch_start_date <= end_trade_date:
                    qfq_bars = MarketDataFetcher.fetch_daily_bars(code, start_date=fetch_start_date, end_date=end_trade_date, adjust="qfq")
                    if needs_full:
                        qfq_rows = self._replace_qfq_history(conn, code, qfq_bars)
                        if signature_changed:
                            self.summary["full_refresh_codes"].append(code)
                        else:
                            self.summary["coverage_backfill_codes"].append(code)
                    else:
                        qfq_rows = self._upsert_daily_bars(conn, code, "qfq", qfq_bars)
                    self.summary["daily_bar_rows"] += qfq_rows

            if enable_intraday:
                intra_df = MarketDataFetcher.fetch_recent_intraday_bars(code)
                if not intra_df.empty:
                    intra_rows = self._replace_intraday_bars(conn, code, intra_df)
                    recent_intra_df = intra_df[intra_df["trade_date"].isin(sorted(intra_df["trade_date"].unique())[-INTRADAY_LOOKBACK_DAYS:])]
                    distributions = self._build_daily_distributions(recent_intra_df)
                    dist_rows = self._replace_daily_distributions(conn, code, distributions)
                    self.summary["intraday_bar_rows"] += intra_rows
                    self.summary["distribution_rows"] += dist_rows

            self.summary["synced_codes"] += 1
            
        except Exception as exc:
            error_message = f"{code}: {exc}"
        finally:
            if self.request_pause > 0:
                time.sleep(self.request_pause)
        return error_message

    # 数据库写入相关逻辑
    def _upsert_stocks(self, conn: sqlite3.Connection, stocks_df: pd.DataFrame) -> int:
        rows = [(r.code, r.name, r.board, int(r.is_st), now_ts()) for r in stocks_df.itertuples(index=False)]
        conn.executemany("""
            INSERT INTO stocks(code, name, board, is_st, updated_at) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET name=excluded.name, board=excluded.board, is_st=excluded.is_st, updated_at=excluded.updated_at
        """, rows)
        conn.commit()
        return len(rows)

    def _upsert_latest_market_value(
        self,
        conn: sqlite3.Connection,
        code: str,
        trade_date: str,
        amount_wan: float | None,
        float_mv_yi: float,
    ) -> None:
        conn.execute("""
            INSERT INTO latest_market_value(code, trade_date, amount_wan, float_mv_yi) VALUES (?, ?, ?, ?)
            ON CONFLICT(code) DO UPDATE SET
                trade_date=excluded.trade_date,
                amount_wan=excluded.amount_wan,
                float_mv_yi=excluded.float_mv_yi
        """, (code, trade_date, amount_wan, float_mv_yi))

    def _upsert_daily_bars(self, conn: sqlite3.Connection, code: str, adjust_type: str, bars_df: pd.DataFrame) -> int:
        if bars_df.empty:
            return 0
        if adjust_type != "qfq":
            raise ValueError(f"daily bars only support qfq, got: {adjust_type}")
        rows = [
            (code, r.date, r.open, r.high, r.low, r.close, r.volume)
            for r in bars_df.itertuples(index=False)
        ]
        conn.executemany(
            """
            INSERT INTO daily_bars(code, trade_date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code, trade_date) DO UPDATE SET
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume
            """,
            rows,
        )
        conn.commit()
        return len(rows)

    def _replace_qfq_history(self, conn: sqlite3.Connection, code: str, bars_df: pd.DataFrame) -> int:
        with conn:
            conn.execute("DELETE FROM daily_bars WHERE code = ?", (code,))
        return self._upsert_daily_bars(conn, code, "qfq", bars_df)

    def _get_daily_bar_coverage(self, conn: sqlite3.Connection, code: str) -> Optional[sqlite3.Row]:
        row = conn.execute(
            "SELECT MIN(trade_date) AS coverage_start_date, MAX(trade_date) AS coverage_end_date FROM daily_bars WHERE code = ?",
            (code,),
        ).fetchone()
        if not row or row["coverage_start_date"] is None or row["coverage_end_date"] is None:
            return None
        return row

    def _replace_intraday_bars(self, conn: sqlite3.Connection, code: str, intra_df: pd.DataFrame) -> int:
        if intra_df.empty: return 0
        rows = [
            (code, r.trade_date, r.trade_time, r.trade_timestamp, r.open, r.close, r.high, r.low, r.avg_price, r.volume, r.amount, r.change_pct, r.change_amount)
            for r in intra_df.itertuples(index=False)
        ]
        conn.executemany("""
            INSERT INTO intraday_bars(code, trade_date, trade_time, trade_timestamp, open, close, high, low, avg_price, volume, amount, change_pct, change_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(code, trade_timestamp) DO UPDATE SET 
                trade_date=excluded.trade_date, trade_time=excluded.trade_time,
                open=excluded.open, close=excluded.close, high=excluded.high, low=excluded.low,
                avg_price=excluded.avg_price, volume=excluded.volume, amount=excluded.amount,
                change_pct=excluded.change_pct, change_amount=excluded.change_amount
        """, rows)
        conn.commit()
        return len(rows)

    def _replace_daily_distributions(self, conn: sqlite3.Connection, code: str, distributions: List[dict]) -> int:
        if not distributions: return 0
        rows = [
            (code, item["date"], json.dumps(item["buy_sell_bins"], ensure_ascii=False), json.dumps(item["price_histogram"], ensure_ascii=False))
            for item in distributions
        ]
        conn.executemany("""
            INSERT INTO daily_price_distributions(code, trade_date, buy_sell_bins_json, price_histogram_json)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(code, trade_date) DO UPDATE SET 
                buy_sell_bins_json=excluded.buy_sell_bins_json,
                price_histogram_json=excluded.price_histogram_json
        """, rows)
        conn.commit()
        return len(rows)

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
                "buy_sell_bins": buy_sell_bins, "price_histogram": price_hist
            })
        dists.sort(key=lambda i: i["date"])
        return dists


class StockMarketDataReader:
    """提供给下游查询和组装结构化前端市场数据载荷的读取器"""
    
    _payload_cache_lock = threading.Lock()
    _payload_cache: Dict[str, dict] = {}

    @classmethod
    def get_empty_payload(cls) -> Dict[str, object]:
        return {"daily_series": [], "candle_windows": {"day": [], "five_day": [], "twenty_day": []}, "intraday_series": [], "daily_distributions": [], "warnings": []}

    @classmethod
    def clear_cache(cls, code: str):
        with cls._payload_cache_lock:
            cls._payload_cache.pop(code, None)

    @classmethod
    def is_trade_day(cls, value: Optional[str] = None) -> bool:
        trade_date = parse_cli_date(value, fallback=datetime.now())
        return datetime.strptime(trade_date, "%Y-%m-%d").weekday() < 5

    @staticmethod
    def has_market_data_for_date(value: Optional[str] = None) -> dict:
        trade_date = parse_cli_date(value, fallback=datetime.now())
        conn = DbManager.get_connection()
        try:
            db_vars = {
                "daily_bar_count": int(conn.execute("SELECT COUNT(*) FROM daily_bars WHERE trade_date = ?", (trade_date,)).fetchone()[0]),
                "intraday_bar_count": int(conn.execute("SELECT COUNT(*) FROM intraday_bars WHERE trade_date = ?", (trade_date,)).fetchone()[0]),
                "distribution_count": int(conn.execute("SELECT COUNT(*) FROM daily_price_distributions WHERE trade_date = ?", (trade_date,)).fetchone()[0]),
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
        
        with cls._payload_cache_lock:
            entry = cls._payload_cache.get(code)
            if entry and time.time() - entry["timestamp"] <= PAYLOAD_CACHE_TTL_SECONDS:
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

        with cls._payload_cache_lock:
            cls._payload_cache[code] = {"timestamp": time.time(), "payload": copy.deepcopy(payload)}
        return payload

    @staticmethod
    def _load_daily_series(conn: sqlite3.Connection, code: str, limit: int = 240) -> List[dict]:
        rows = conn.execute("SELECT trade_date AS date, open, close, high, low, volume FROM daily_bars WHERE code = ? ORDER BY trade_date DESC LIMIT ?", (code, limit)).fetchall()
        return [{"date": str(r["date"]), "open": safe_float(r["open"]), "close": safe_float(r["close"]), "high": safe_float(r["high"]), "low": safe_float(r["low"]), "volume": safe_float(r["volume"]), "amount": safe_float(r["volume"]), "amplitude": None, "change_pct": None, "change_amount": None, "turnover_rate": None} for r in reversed(rows)]

    @staticmethod
    def _load_recent_intraday_series(conn: sqlite3.Connection, code: str, lookback_days: int = INTRADAY_LOOKBACK_DAYS) -> List[dict]:
        trade_dates_desc = [
            str(row["trade_date"])
            for row in conn.execute(
                "SELECT DISTINCT trade_date FROM intraday_bars WHERE code = ? ORDER BY trade_date DESC LIMIT ?",
                (code, lookback_days),
            ).fetchall()
        ]
        if not trade_dates_desc:
            return []

        trade_dates = sorted(trade_dates_desc)
        rows = conn.execute(
            f"SELECT trade_timestamp, trade_date, trade_time, open, close, high, low, avg_price, volume, amount, change_pct, change_amount FROM intraday_bars WHERE code = ? AND trade_date IN ({','.join('?' * len(trade_dates))}) ORDER BY trade_timestamp",
            [code, *trade_dates],
        ).fetchall()

        return [{"timestamp": str(r["trade_timestamp"]), "date": str(r["trade_date"]), "time": str(r["trade_time"])[:5], "open": safe_float(r["open"]), "close": safe_float(r["close"]), "high": safe_float(r["high"]), "low": safe_float(r["low"]), "avg_price": safe_float(r["avg_price"]), "volume": safe_float(r["volume"]), "amount": safe_float(r["amount"]), "change_pct": safe_float(r["change_pct"]), "change_amount": safe_float(r["change_amount"])} for r in rows]

    @staticmethod
    def _load_daily_distributions(conn: sqlite3.Connection, code: str, lookback_days: int = INTRADAY_LOOKBACK_DAYS) -> List[dict]:
        rows = conn.execute(
            "SELECT trade_date, buy_sell_bins_json, price_histogram_json FROM daily_price_distributions WHERE code = ? ORDER BY trade_date DESC LIMIT ?",
            (code, lookback_days),
        ).fetchall()
        rows = sorted(rows, key=lambda row: str(row["trade_date"]))[-lookback_days:]
        return [{"date": str(r["trade_date"]), "buy_sell_bins": json.loads(r["buy_sell_bins_json"]), "price_histogram": json.loads(r["price_histogram_json"])} for r in rows]

    @staticmethod
    def plot_kline(code: str, limit: int = 120) -> None:
        """展示给定股票的前复权 K 线图及移动平均线（MA5, MA10, MA20）
        limit 参数控制展示的日线数量，默认120条（约半年交易日）
        """
        conn = DbManager.get_connection()
        try:
            query = "SELECT trade_date, open, high, low, close, volume FROM daily_bars WHERE code = ? ORDER BY trade_date DESC LIMIT ?"
            df = pd.read_sql(query, conn, params=(code, limit))
        finally:
            conn.close()

        if df.empty:
            print(f"数据不足: 数据库中没有找到 {code} 的前复权数据")
            return

        df = df.sort_values("trade_date")
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df.set_index("trade_date", inplace=True)
        # mplfinance 要求列名为大写首字母
        df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"}, inplace=True)

        # 计算均线以便点击时能够在控制台中打印
        df["MA5"] = df["Close"].rolling(window=5).mean()
        df["MA10"] = df["Close"].rolling(window=10).mean()
        df["MA20"] = df["Close"].rolling(window=20).mean()

        # K 线配色：红涨绿跌
        fig, axes = mpf.plot(df, type='candle', mav=(5, 10, 20), volume=True,
                             title=f"Stock K-Line: {code}",
                             style=mpf.make_mpf_style(base_mpf_style='yahoo', marketcolors=mpf.make_marketcolors(up='r', down='g', inherit=True)),
                     update_width_config=dict(candle_width=0.55, candle_linewidth=0.8, volume_width=0.55),
                             show_nontrading=False,
                             returnfig=True)

        def on_click(event):
            # 判断是否点击在坐标轴内，并且能获取到 x 坐标 (通过 show_nontrading=False 绘制时，x 坐标对应的是 DataFrame 的整数索引)
            if event.inaxes and event.xdata is not None:
                idx = int(round(event.xdata))
                if 0 <= idx < len(df):
                    row = df.iloc[idx]
                    date_str = df.index[idx].strftime('%Y-%m-%d')
                    ma5 = f"{row['MA5']:.2f}" if not pd.isna(row['MA5']) else "N/A"
                    ma10 = f"{row['MA10']:.2f}" if not pd.isna(row['MA10']) else "N/A"
                    ma20 = f"{row['MA20']:.2f}" if not pd.isna(row['MA20']) else "N/A"
                    sys.stdout.write(f"\n[{date_str}] 开盘: {row['Open']:.2f}, 收盘: {row['Close']:.2f}, "
                                     f"最高: {row['High']:.2f}, 最低: {row['Low']:.2f}, 成交量: {row['Volume']}, "
                                     f"MA5: {ma5}, MA10: {ma10}, MA20: {ma20}")
                    sys.stdout.flush()

        fig.canvas.mpl_connect('button_press_event', on_click)
        mpf.show()
        
def main() -> None:
    parser = argparse.ArgumentParser(description="同步 A 股行情到 SQLite")
    parser.add_argument("--start-date", type=str, default=None, help="起始同步日期。格式 YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=None, help="结束同步日期。格式 YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=0, help="仅同步前 N 只股票，便于调试")
    parser.add_argument("--plot", type=str, default=None, help="输入股票代码（如 000001），直接拉取库中数据并展示K线图（不会进行同步操作）")
    args = parser.parse_args()

    if args.plot:
        StockMarketDataReader.plot_kline(args.plot, limit=300)
        return

    engine = StockMarketSyncEngine(
        limit=args.limit,
        request_pause=0.15,
    )
    summary = engine.run_sync(start_date=args.start_date, end_date=args.end_date)

    print(
        f"同步完成: 目标股票 {summary['target_universe_count']} 只, "
        f"实际同步 {summary['synced_codes']} 只, 日线 {summary['daily_bar_rows']} 行, "
        f"最近5日分时 {summary['intraday_bar_rows']} 行, 分布 {summary['distribution_rows']} 条"
    )
    print(f"核心库: {summary['db_path']}")
    if summary["full_refresh_codes"]:
        print(f"触发复权重刷: {', '.join(summary['full_refresh_codes'][:20])}")
    if summary["errors"]:
        print("异常:")
        for item in summary["errors"][:20]:
            print(f"  - {item}")


if __name__ == "__main__":
    main()
