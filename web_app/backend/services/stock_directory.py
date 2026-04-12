"""股票目录服务。

负责从 akshare 拉取沪深北股票基础信息，构建代码和名称索引，
并提供带缓存的股票查询能力。
"""

import threading
import time
import unicodedata
from typing import Dict, List, Optional

try:
    import akshare as ak
except ImportError:
    ak = None


def normalize_company_name(value: Optional[str]) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKC", str(value)).strip()
    return "".join(normalized.split()).upper()


class StockDirectoryService:
    def __init__(self, refresh_interval_seconds: int = 6 * 60 * 60):
        self.refresh_interval_seconds = refresh_interval_seconds
        self._lock = threading.Lock()
        self._last_loaded_at = 0.0
        self._records_by_code: Dict[str, Dict[str, Optional[str]]] = {}
        self._records_by_name: Dict[str, List[Dict[str, Optional[str]]]] = {}

    def _build_record(self, code: str, name: str, exchange: str, full_name: Optional[str] = None):
        normalized_code = str(code).strip().zfill(6)
        normalized_name = str(name).strip()
        normalized_full_name = str(full_name).strip() if full_name else None
        return {
            "code": normalized_code,
            "name": normalized_name,
            "full_name": normalized_full_name,
            "exchange": exchange,
        }

    def _index_record(self, record: Dict[str, Optional[str]]):
        self._records_by_code[record["code"]] = record
        for raw_name in (record.get("name"), record.get("full_name")):
            key = normalize_company_name(raw_name)
            if not key:
                continue
            bucket = self._records_by_name.setdefault(key, [])
            if all(existing["code"] != record["code"] for existing in bucket):
                bucket.append(record)

    def _reload_directory(self):
        if ak is None:
            raise RuntimeError("akshare is not installed")

        records: List[Dict[str, Optional[str]]] = []

        sh_df = ak.stock_info_sh_name_code()
        for _, row in sh_df.iterrows():
            records.append(
                self._build_record(
                    code=row["证券代码"],
                    name=row["证券简称"],
                    full_name=row.get("公司全称"),
                    exchange="SH",
                )
            )

        sz_df = ak.stock_info_sz_name_code()
        for _, row in sz_df.iterrows():
            records.append(
                self._build_record(
                    code=row["A股代码"],
                    name=row["A股简称"],
                    exchange="SZ",
                )
            )

        bj_df = ak.stock_info_bj_name_code()
        for _, row in bj_df.iterrows():
            records.append(
                self._build_record(
                    code=row["证券代码"],
                    name=row["证券简称"],
                    exchange="BJ",
                )
            )

        records_by_code: Dict[str, Dict[str, Optional[str]]] = {}
        records_by_name: Dict[str, List[Dict[str, Optional[str]]]] = {}
        for record in records:
            records_by_code[record["code"]] = record
            for raw_name in (record.get("name"), record.get("full_name")):
                key = normalize_company_name(raw_name)
                if not key:
                    continue
                records_by_name.setdefault(key, []).append(record)

        self._records_by_code = records_by_code
        self._records_by_name = records_by_name
        self._last_loaded_at = time.time()

    def _ensure_loaded(self):
        now = time.time()
        if self._records_by_code and now - self._last_loaded_at < self.refresh_interval_seconds:
            return

        with self._lock:
            now = time.time()
            if self._records_by_code and now - self._last_loaded_at < self.refresh_interval_seconds:
                return
            self._reload_directory()

    def _enrich_full_name(self, record: Dict[str, Optional[str]]):
        if record.get("full_name"):
            return record

        if ak is None:
            return record

        with self._lock:
            current = self._records_by_code.get(record["code"], record)
            if current.get("full_name"):
                return current

            profile_df = ak.stock_profile_cninfo(symbol=current["code"])
            if not profile_df.empty:
                full_name = profile_df.iloc[0].get("公司名称")
                if full_name:
                    current["full_name"] = str(full_name).strip()
                    self._index_record(current)
            return current

    def lookup(self, query_type: str, keyword: str):
        self._ensure_loaded()

        raw_keyword = str(keyword or "").strip()
        if not raw_keyword:
            return None

        if query_type == "code":
            code = raw_keyword.zfill(6)
            record = self._records_by_code.get(code)
        elif query_type == "name":
            matched_records = self._records_by_name.get(normalize_company_name(raw_keyword), [])
            record = matched_records[0] if matched_records else None
        else:
            raise ValueError("query_type must be 'code' or 'name'")

        if not record:
            return None

        return self._enrich_full_name(dict(record))


stock_directory_service = StockDirectoryService()