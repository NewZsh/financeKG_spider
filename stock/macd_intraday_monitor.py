#!/usr/bin/env python3
"""日内分钟级 MACD 实时监控程序。

从腾讯财经 API 拉取实时行情，计算分钟级 MACD 指标（动态 + 静态），
支持 ANSI 彩色终端输出、盘中热恢复、柱体阶段极值价格追踪。

工作逻辑：
  1. 晨间预热 — 从数据库回放昨日分钟数据，恢复 MACD 状态
  2. 盘中重启 — 从今日 CSV 热恢复 MACD 状态，跳过预热
  3. 监控循环 — 每秒拉取实时 tick，更新动态 MACD（每 tick 价格递推）
  4. 分钟固化 — 跨分钟时用 VWAP 反算分钟均价，更新静态 MACD，写入 CSV
  5. ANSI 颜色 — 当前 BAR 与上一静态 BAR 比较：
     红柱：增强(红) / 衰减(粉) ； 绿柱：伸长(深绿) / 缩短(浅绿)

用法:
    python stock/macd_intraday_monitor.py --stocks 601689,002841
    python stock/macd_intraday_monitor.py --stocks 601689,002841 --config macd_monitor.json

数据落盘: data/macd_monitor/{code}_{date}.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

API_TEMPLATE = "http://qt.gtimg.cn/q={}"

# stock_monitor.py 原始字段
TICK_CSV_FIELDS = [
    "时间戳", "股票名称", "股票代码", "当前价格", "昨日收盘", "今日开盘",
    "最高价", "最低价", "涨跌额", "涨跌幅(%)",
    "总成交量(手)", "成交额(万元)", "外盘(手)", "内盘(手)",
    "买1价", "买1量", "买2价", "买2量", "买3价", "买3量", "买4价", "买4量", "买5价", "买5量",
    "卖1价", "卖1量", "卖2价", "卖2量", "卖3价", "卖3量", "卖4价", "卖4量", "卖5价", "卖5量",
    "换手率(%)", "动态市盈率", "总市值(亿)", "流通市值(亿)", "加权均价", "交易币种",
]

# 追加的 MACD 列
MACD_CSV_FIELDS = [
    "dyn_DIF", "dyn_DEA", "dyn_BAR",
    "sta_DIF", "sta_DEA", "sta_BAR",
]

CSV_FIELDS = TICK_CSV_FIELDS + MACD_CSV_FIELDS

# MACD 参数
ALPHA_FAST = 2.0 / 13.0   # EMA12: 2/(12+1)
ALPHA_SLOW = 2.0 / 27.0   # EMA26: 2/(26+1)
ALPHA_SIGNAL = 0.2         # DEA9:  2/(9+1)

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "stock.db"
MONITOR_DIR = BASE_DIR / "data" / "macd_monitor"

# ANSI 终端颜色
RED         = '\033[91m'   # 红柱增强 (current_bar > prev_sta_bar)
PINK        = '\033[95m'   # 红柱衰减 (current_bar < prev_sta_bar)
DEEP_GREEN  = '\033[32m'   # 绿柱伸长 (abs(current_bar) > abs(prev_sta_bar))
LIGHT_GREEN = '\033[92m'   # 绿柱缩短 (abs(current_bar) < abs(prev_sta_bar))
RESET       = '\033[0m'

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def to_symbol(code: str) -> str:
    """纯数字代码 → 带 sh/sz/bj 前缀的 symbol。"""
    if code.startswith(("8", "4", "9")):
        return f"bj{code}"
    return f"sh{code}" if code.startswith(("6", "688")) else f"sz{code}"


def normalize_code(code: str) -> str:
    """如果已带前缀则直接返回，否则补全。"""
    if re.match(r'^(sh|sz|bj)\d', code):
        return code
    return to_symbol(code)


def csv_path(code: str) -> str:
    """返回今日 CSV 路径。"""
    today = datetime.now().strftime("%Y%m%d")
    os.makedirs(str(MONITOR_DIR), exist_ok=True)
    return str(MONITOR_DIR / f"{code}_{today}.csv")


# ---------------------------------------------------------------------------
# 数据获取（复用 stock_monitor.py 逻辑）
# ---------------------------------------------------------------------------

def fetch_raw(stock_code: str) -> str:
    """从腾讯财经 API 拉取原始行情数据。"""
    req = urllib.request.Request(API_TEMPLATE.format(stock_code))
    req.add_header("User-Agent", "Mozilla/5.0")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.read().decode("gbk")


def parse(raw: str) -> Optional[dict]:
    """解析 tilde 分隔的行情字符串为 dict。"""
    m = re.search(r'="(.+)"', raw)
    if not m:
        return None

    fields = m.group(1).split("~")

    ts_str = fields[30]
    ts = datetime.strptime(ts_str, "%Y%m%d%H%M%S") if ts_str else None

    return {
        "股票名称":     fields[1],
        "股票代码":     fields[2],
        "当前价格":     float(fields[3])   if fields[3]  else None,
        "昨日收盘":     float(fields[4])   if fields[4]  else None,
        "今日开盘":     float(fields[5])   if fields[5]  else None,
        "总成交量(手)":  int(fields[6])     if fields[6]  else None,
        "外盘(手)":     int(fields[7])     if fields[7]  else None,
        "内盘(手)":     int(fields[8])     if fields[8]  else None,
        "买1价": float(fields[9])  if fields[9]  else None,
        "买1量": int(fields[10])   if fields[10] else None,
        "买2价": float(fields[11]) if fields[11] else None,
        "买2量": int(fields[12])   if fields[12] else None,
        "买3价": float(fields[13]) if fields[13] else None,
        "买3量": int(fields[14])   if fields[14] else None,
        "买4价": float(fields[15]) if fields[15] else None,
        "买4量": int(fields[16])   if fields[16] else None,
        "买5价": float(fields[17]) if fields[17] else None,
        "买5量": int(fields[18])   if fields[18] else None,
        "卖1价": float(fields[19]) if fields[19] else None,
        "卖1量": int(fields[20])   if fields[20] else None,
        "卖2价": float(fields[21]) if fields[21] else None,
        "卖2量": int(fields[22])   if fields[22] else None,
        "卖3价": float(fields[23]) if fields[23] else None,
        "卖3量": int(fields[24])   if fields[24] else None,
        "卖4价": float(fields[25]) if fields[25] else None,
        "卖4量": int(fields[26])   if fields[26] else None,
        "卖5价": float(fields[27]) if fields[27] else None,
        "卖5量": int(fields[28])   if fields[28] else None,
        "时间戳":     ts,
        "涨跌额":     float(fields[31]) if fields[31] else None,
        "涨跌幅(%)":  float(fields[32]) if fields[32] else None,
        "最高价":     float(fields[33]) if fields[33] else None,
        "最低价":     float(fields[34]) if fields[34] else None,
        "成交额(万元)":   float(fields[37]) if fields[37] else None,
        "换手率(%)":     float(fields[38]) if fields[38] else None,
        "动态市盈率":     float(fields[39]) if fields[39] else None,
        "总市值(亿)":     float(fields[44]) if fields[44] else None,
        "流通市值(亿)":   float(fields[45]) if fields[45] else None,
        "交易币种":       fields[82],
        "加权均价":       float(fields[85]) if fields[85] else None,
    }


# ---------------------------------------------------------------------------
# 核心 MACD 算法
# ---------------------------------------------------------------------------

@dataclass
class MACDState:
    ema12: float
    ema26: float
    diff: float
    dea: float
    bar: float


def init_macd(first_price: float) -> MACDState:
    """首个价格初始化：EMA12=EMA26=price，DIFF=DEA=BAR=0。"""
    return MACDState(
        ema12=first_price,
        ema26=first_price,
        diff=0.0,
        dea=0.0,
        bar=0.0,
    )


def step_macd(state: MACDState, price: float) -> MACDState:
    """一步递推，返回新状态。纯函数。"""
    ema12 = price * ALPHA_FAST + state.ema12 * (1.0 - ALPHA_FAST)
    ema26 = price * ALPHA_SLOW + state.ema26 * (1.0 - ALPHA_SLOW)
    diff = ema12 - ema26
    dea = diff * ALPHA_SIGNAL + state.dea * (1.0 - ALPHA_SIGNAL)
    bar = 2.0 * (diff - dea)
    return MACDState(ema12=ema12, ema26=ema26, diff=diff, dea=dea, bar=bar)


def replay_day(prices: list[float]) -> list[MACDState]:
    """全量回放一天的分钟 VWAP 序列。

    首分钟自动 init，后续逐分钟 step。
    返回与输入等长的 MACDState 列表，最后一个即 15:00 收盘状态。
    """
    if not prices:
        return []
    states = [init_macd(prices[0])]
    for p in prices[1:]:
        states.append(step_macd(states[-1], p))
    return states


# ---------------------------------------------------------------------------
# 分钟 VWAP 反算
# ---------------------------------------------------------------------------

def calc_minute_vwap(
    vol_start: int, amount_start: float,
    vol_end: int, amount_end: float,
) -> Optional[float]:
    """从累计成交量/成交额反算单分钟 VWAP。

    amount 单位为万元，volume 单位为手（1 手 = 100 股）。
    返回 None 表示该分钟无成交。
    """
    vol_delta = vol_end - vol_start
    amount_delta = amount_end - amount_start
    if vol_delta <= 0 or amount_delta <= 0:
        return None
    # VWAP = 成交额 / 成交量
    # 成交额(元) = amount_delta(万元) * 10000
    # 成交量(股) = vol_delta(手) * 100
    # VWAP(元/股) = amount_delta * 10000 / (vol_delta * 100)
    #             = amount_delta * 100 / vol_delta
    return amount_delta * 100.0 / vol_delta


# ---------------------------------------------------------------------------
# MACD 监控器
# ---------------------------------------------------------------------------

class MACDMonitor:
    """多股票 MACD 实时监控器。"""

    def __init__(self, codes: list[str]):
        self.codes = [normalize_code(c) for c in codes]
        # 动态 MACD 状态（每 tick 用实时价更新）
        self.dyn_states: dict[str, MACDState] = {}
        # 静态 MACD 状态（分钟 VWAP 固化时才更新）
        self.sta_states: dict[str, MACDState] = {}
        # 上一 tick 时间戳（去重用）
        self._prev_ts: dict[str, Optional[datetime]] = {c: None for c in self.codes}
        # 当前分钟（用于跨分钟检测）
        self._current_minute: dict[str, Optional[str]] = {c: None for c in self.codes}
        # 当前分钟累计起始值（用于 VWAP 反算，上一分钟末的累计值）
        self._minute_start_vol: dict[str, int] = {c: 0 for c in self.codes}
        self._minute_start_amount: dict[str, float] = {c: 0.0 for c in self.codes}
        # 当前分钟内首/末 tick 价格（用于记录 OHLC）
        self._minute_open: dict[str, Optional[float]] = {c: None for c in self.codes}
        self._minute_high: dict[str, float] = {c: 0.0 for c in self.codes}
        self._minute_low: dict[str, float] = {c: 1e9 for c in self.codes}
        self._minute_close: dict[str, Optional[float]] = {c: None for c in self.codes}
        # 股票名称缓存
        self._names: dict[str, str] = {}
        # CSV 文件路径
        self._csv_files: dict[str, str] = {}
        # 连续网络错误计数
        self._net_errors: dict[str, int] = {c: 0 for c in self.codes}
        # 最后已知的累计值（用于收盘时 VWAP 计算）
        self._last_vol: dict[str, int] = {c: 0 for c in self.codes}
        self._last_amount: dict[str, float] = {c: 0.0 for c in self.codes}
        # ---- 颜色/极值追踪字段 ----
        # 上一根静态 BAR（颜色比较基准）
        self._prev_sta_bar: dict[str, float] = {c: 0.0 for c in self.codes}
        # 当前 BAR 阶段的局部最高价和最低价（红柱/绿柱均追踪）
        self._phase_high: dict[str, Optional[float]] = {c: None for c in self.codes}
        self._phase_low: dict[str, Optional[float]] = {c: None for c in self.codes}
        # 当前 BAR 阶段（基于静态 BAR 符号）
        self._bar_phase: dict[str, Optional[str]] = {c: None for c in self.codes}
        # 动态行覆写追踪
        self._dyn_lines_printed: int = 0
        self._max_dyn_len: dict[str, int] = {}

    # ---- 数据库 ----

    def _get_db_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        return conn

    def _lookup_names(self) -> None:
        """预加载所有股票名称。"""
        conn = self._get_db_connection()
        try:
            for code in self.codes:
                row = conn.execute(
                    "SELECT code, name FROM stocks WHERE code = ?",
                    (code.removeprefix("sh").removeprefix("sz").removeprefix("bj"),),
                ).fetchone()
                if row:
                    self._names[code] = row["name"]
                else:
                    self._names[code] = code
        finally:
            conn.close()

    # ---- 颜色决策 ----

    @staticmethod
    def _get_color(current_bar: float, prev_sta_bar: float) -> str:
        """根据当前 BAR 与上一静态 BAR 比较，返回 ANSI 颜色码。

        红柱 (current_bar > 0): 原值比较
          current_bar > prev_sta_bar → RED,  current_bar < prev_sta_bar → PINK
        绿柱 (current_bar < 0): 绝对值比较
          abs(cur) > abs(prev) → DEEP_GREEN,  abs(cur) < abs(prev) → LIGHT_GREEN
        """
        if current_bar > 0:
            if prev_sta_bar == 0:
                return RED
            return RED if current_bar > prev_sta_bar else PINK
        elif current_bar < 0:
            if prev_sta_bar == 0:
                return DEEP_GREEN
            return DEEP_GREEN if abs(current_bar) > abs(prev_sta_bar) else LIGHT_GREEN
        return ""

    # ---- CSV 热恢复 ----

    def _load_state_from_csv(self, code: str) -> Optional[MACDState]:
        """从今日 CSV 恢复 MACD 状态（盘中重启用）。

        读取最后一行：优先 sta_DIF/DEA/BAR，若全零则取 dyn_DIF/DEA/BAR。
        用最后价格近似重建 EMA12/EMA26，同时恢复 prev_sta_bar 和 bar_phase。
        返回 None 表示 CSV 不存在或不可用。
        """
        filepath = csv_path(code)
        if not os.path.exists(filepath):
            return None

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except Exception:
            return None

        if not rows:
            return None

        # 最后一行
        last = rows[-1]
        try:
            price = float(last.get("当前价格", 0))
            sta_dif = float(last.get("sta_DIF", 0))
            sta_dea = float(last.get("sta_DEA", 0))
            sta_bar = float(last.get("sta_BAR", 0))
            dyn_dif = float(last.get("dyn_DIF", 0))
            dyn_dea = float(last.get("dyn_DEA", 0))
            dyn_bar = float(last.get("dyn_BAR", 0))
        except (ValueError, TypeError):
            return None

        if price <= 0:
            return None

        # 优先取静态值，全零则取动态值
        if sta_dif == 0 and sta_dea == 0:
            dif, dea, bar = dyn_dif, dyn_dea, dyn_bar
        else:
            dif, dea, bar = sta_dif, sta_dea, sta_bar

        # EMA 近似重建：ema12 - ema26 = DIF, 设 ema12 = price + DIF/2, ema26 = price - DIF/2
        ema12 = price + dif / 2.0
        ema26 = price - dif / 2.0

        # 恢复 prev_sta_bar：取倒数第二行的 sta_BAR
        prev_bar = 0.0
        if len(rows) >= 2:
            try:
                prev = rows[-2]
                p_sb = float(prev.get("sta_BAR", 0))
                p_sd = float(prev.get("sta_DIF", 0))
                p_se = float(prev.get("sta_DEA", 0))
                if p_sd == 0 and p_se == 0:
                    prev_bar = float(prev.get("dyn_BAR", 0))
                else:
                    prev_bar = p_sb
            except (ValueError, TypeError):
                prev_bar = 0.0

        self._prev_sta_bar[code] = prev_bar

        # 恢复 bar_phase 和极值
        if bar > 0:
            self._bar_phase[code] = "red"
            self._phase_high[code] = price
            self._phase_low[code] = price
        elif bar < 0:
            self._bar_phase[code] = "green"
            self._phase_high[code] = price
            self._phase_low[code] = price
        else:
            self._bar_phase[code] = None
            self._phase_high[code] = None
            self._phase_low[code] = None

        state = MACDState(ema12=ema12, ema26=ema26, diff=dif, dea=dea, bar=bar)
        print(f"  {code}  从今日 CSV 热恢复 → "
              f"价格={price:.2f} DIF={dif:+.4f} DEA={dea:+.4f} BAR={bar:+.4f}  "
              f"prev_sta_bar={prev_bar:+.4f} phase={self._bar_phase[code]}")
        return state

    # ---- 晨间预热 ----

    def warmup(self, csv_recovered: set | None = None) -> None:
        """晨间预热：拉昨日 intraday_bars，回放得到 15:00 MACD 状态。

        csv_recovered: 已从 CSV 热恢复的 code 集合，跳过这些 code 的 intraday 回放。
        """
        csv_recovered = csv_recovered or set()
        print("=" * 60)
        print("  晨间预热：从昨日分钟数据回放 MACD ...")
        print("=" * 60)

        conn = self._get_db_connection()
        try:
            for code in self.codes:
                if code in csv_recovered:
                    print(f"  {code}  已从 CSV 热恢复，跳过 intraday 预热")
                    continue

                raw_code = code.removeprefix("sh").removeprefix("sz").removeprefix("bj")
                # 取最近一个交易日
                row = conn.execute(
                    "SELECT DISTINCT trade_date FROM intraday_bars WHERE code = ? "
                    "ORDER BY trade_date DESC LIMIT 1",
                    (raw_code,),
                ).fetchone()

                if not row:
                    print(f"  {code}  无历史 intraday 数据，将以当日成交均价冷启动")
                    continue

                trade_date = row["trade_date"]
                rows = conn.execute(
                    "SELECT trade_time, avg_price FROM intraday_bars "
                    "WHERE code = ? AND trade_date = ? "
                    "ORDER BY trade_time",
                    (raw_code, trade_date),
                ).fetchall()

                prices = [r["avg_price"] for r in rows if r["avg_price"] and r["avg_price"] > 0]
                if not prices:
                    print(f"  {code}  {trade_date}  无有效价格，将以当日成交均价冷启动")
                    continue

                states = replay_day(prices)
                last_state = states[-1]
                self.dyn_states[code] = last_state
                self.sta_states[code] = MACDState(
                    ema12=last_state.ema12,
                    ema26=last_state.ema26,
                    diff=last_state.diff,
                    dea=last_state.dea,
                    bar=last_state.bar,
                )
                # 首个 prev_sta_bar 即为昨日收盘的 sta_BAR
                self._prev_sta_bar[code] = last_state.bar
                # 阶段和极值初始化（基于昨日收盘 BAR）
                if last_state.bar > 0:
                    self._bar_phase[code] = "red"
                    self._phase_high[code] = prices[-1]
                    self._phase_low[code] = prices[-1]
                elif last_state.bar < 0:
                    self._bar_phase[code] = "green"
                    self._phase_high[code] = prices[-1]
                    self._phase_low[code] = prices[-1]

                print(f"  {code}  {self._names.get(code, '')}  "
                      f"昨日({trade_date}) {len(prices)} 分钟 → "
                      f"15:00 EMA12={last_state.ema12:.2f} DIFF={last_state.diff:+.4f}")

        finally:
            conn.close()

        # 对于冷启动的股票，标记为待初始化（首个 tick 到达时用当日成交均价 init）
        for code in self.codes:
            if code not in self.dyn_states:
                self.dyn_states[code] = MACDState(0, 0, 0, 0, 0)  # placeholder
                self.sta_states[code] = MACDState(0, 0, 0, 0, 0)
            # 初始化 CSV 文件路径
            self._csv_files[code] = csv_path(code)

        print()

    # ---- 等待开盘 ----

    @staticmethod
    def _wait_until(target_hour: int, target_minute: int, label: str) -> None:
        """自适应等待至目标时间，在当前行刷新倒计时。

        策略：距目标 > 10 秒则 sleep 剩余时间的一半；≤ 10 秒则每秒检查一次。
        到达目标时间后换行继续。
        """
        while True:
            now = datetime.now()
            current = now.hour * 60 + now.minute
            target = target_hour * 60 + target_minute

            if current >= target:
                print(f"\r[{now.strftime('%H:%M:%S')}] {label} 时间到，开始交易\033[K")
                break

            # 计算剩余秒数
            target_dt = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            remaining = max((target_dt - now).total_seconds(), 0)

            # 在当前行刷新倒计时
            print(
                f"\r[{now.strftime('%H:%M:%S')}] 等待 {label} 开盘，"
                f"剩余 {int(remaining)} 秒 ... \033[K",
                end="", flush=True,
            )

            if remaining > 10:
                time.sleep(remaining / 2.0)
            else:
                time.sleep(1)

        print()  # 换行

    def wait_for_open(self) -> None:
        """等待至 09:30 早盘开盘。"""
        now = datetime.now()
        hm = now.hour * 60 + now.minute

        if hm >= 15 * 60:
            print("已过 15:00，今日已收盘")
            sys.exit(0)

        if hm >= 9 * 60 + 30:
            print(f"[延迟启动] 当前已过 09:30，直接进入监控循环\n")
            return

        self._wait_until(9, 30, "早盘")

    # ---- 主循环 ----

    def run(self) -> None:
        """主入口：CSV 热恢复 → 预热 → 等待 → 监控循环 → 收盘退出。"""
        self._lookup_names()

        # 先尝试 CSV 热恢复（盘中重启 / 延迟启动场景）
        csv_recovered: set[str] = set()
        now = datetime.now()
        hm = now.hour * 60 + now.minute
        if hm >= 9 * 60 + 30:  # 已过 09:30，尝试从今日 CSV 恢复
            print("=" * 60)
            print("  尝试从今日 CSV 热恢复 MACD 状态 ...")
            print("=" * 60)
            for code in self.codes:
                state = self._load_state_from_csv(code)
                if state is not None:
                    self.dyn_states[code] = state
                    self.sta_states[code] = MACDState(
                        ema12=state.ema12, ema26=state.ema26,
                        diff=state.diff, dea=state.dea, bar=state.bar,
                    )
                    csv_recovered.add(code)
            print()

        self.warmup(csv_recovered)
        self.wait_for_open()

        # 冷启动 / 延迟启动标记：ema12==0 表示 placeholder，待首个 tick 初始化
        pending_init = {c: (self.dyn_states[c].ema12 == 0) for c in self.codes}

        print("=" * 60)
        print("  开始盘中监控 ...")
        print("=" * 60)
        print()

        while True:
            now = datetime.now()
            hm = now.hour * 60 + now.minute

            # 收盘退出
            if hm >= 15 * 60:
                print(f"\n[{now.strftime('%H:%M:%S')}] 收盘 15:00，固化最后一分钟，退出")
                for code in self.codes:
                    self._finalize_current_minute(
                        code,
                        end_vol=self._last_vol.get(code, 0),
                        end_amount=self._last_amount.get(code, 0.0),
                    )
                break

            # 午休等待至 13:00
            if 11 * 60 + 30 <= hm < 13 * 60:
                self._wait_until(13, 0, "午盘")
                continue

            self._dyn_printed_this_cycle = 0
            for code in self.codes:
                try:
                    symbol = normalize_code(code)
                    raw = fetch_raw(symbol)
                    tick = parse(raw)
                    if tick is None:
                        self._net_errors[code] += 1
                        if self._net_errors[code] > 10:
                            print(f"[!] {code} 连续解析失败 > 10 次，请检查网络")
                        continue
                    self._net_errors[code] = 0

                    # 冷启动/延迟启动：首个 tick 用当日成交均价初始化
                    # 优先用加权均价，不可用时 fallback 到当前价格
                    if pending_init.get(code):
                        init_price = (
                            tick["加权均价"] if tick["加权均价"] and tick["加权均价"] > 0
                            else tick["当前价格"]
                        )
                        if init_price and init_price > 0:
                            st = init_macd(init_price)
                            self.dyn_states[code] = st
                            self.sta_states[code] = MACDState(
                                ema12=st.ema12, ema26=st.ema26,
                                diff=st.diff, dea=st.dea, bar=st.bar,
                            )
                            pending_init[code] = False
                            print(f"[{now.strftime('%H:%M:%S')}] {code} 冷启动，"
                                  f"以价格 {init_price:.2f} 初始化 MACD，直接进入递推")

                    # Tick 去重（必须在 init 之后，确保首 tick 不会被错误跳过）
                    current_ts = tick["时间戳"]
                    prev_ts = self._prev_ts.get(code)
                    if prev_ts and current_ts and current_ts == prev_ts:
                        continue  # 无新成交
                    self._prev_ts[code] = current_ts

                    if tick["当前价格"] is None or tick["当前价格"] <= 0:
                        continue

                    # 处理这一个 tick
                    self._on_tick(code, tick)

                except urllib.error.URLError as e:
                    self._net_errors[code] += 1
                    if self._net_errors[code] > 10:
                        print(f"[!] {code} 连续网络失败 > 10 次: {e}")
                except Exception as e:
                    print(f"[!] {code} 错误: {e}")

            if len(self.codes) > 1:
                self._dyn_lines_printed = self._dyn_printed_this_cycle

            # 随机抖动 1~3 秒
            time.sleep(random.uniform(1, 3))

    # ---- Tick 处理 ----

    def _on_tick(self, code: str, tick: dict) -> None:
        """处理一只股票的一个新 tick。"""
        now = datetime.now()
        price = tick["当前价格"]
        ts = tick["时间戳"]
        tick_minute = ts.strftime("%H:%M") if ts else now.strftime("%H:%M")
        vol = tick["总成交量(手)"] or 0
        amount = tick["成交额(万元)"] or 0.0

        prev_minute = self._current_minute.get(code)

        # ---- 分钟切换检测 ----
        # 当前 tick 属于新分钟 → 上一分钟结束
        if prev_minute is not None and tick_minute != prev_minute:
            # 此时: _minute_start_vol/amount = 上一分钟的起始累计值
            #       vol/amount = 当前 tick 累计值 = 上一分钟的结束累计值
            self._finalize_current_minute(code, end_vol=vol, end_amount=amount)

        # 如果是新的一分钟（或首次），初始化分钟跟踪
        if self._current_minute.get(code) != tick_minute:
            self._current_minute[code] = tick_minute
            self._minute_start_vol[code] = vol
            self._minute_start_amount[code] = amount
            self._minute_open[code] = price
            self._minute_high[code] = price
            self._minute_low[code] = price
            self._minute_close[code] = price

        # 记录最近一次的累计值（收盘时用）
        self._last_vol[code] = vol
        self._last_amount[code] = amount

        # 更新当前分钟 OHLC
        self._minute_high[code] = max(self._minute_high[code], price)
        self._minute_low[code] = min(self._minute_low[code], price)
        self._minute_close[code] = price

        # ---- 更新动态 MACD（每 tick 用实时价） ----
        self.dyn_states[code] = step_macd(self.dyn_states[code], price)
        dyn = self.dyn_states[code]

        # ---- 静态 MACD（当前分钟末才更新，此处保持上分钟值） ----
        sta = self.sta_states[code]

        # ---- 写 CSV ----
        self._write_csv(code, tick, dyn, sta)

        # ---- 控制台打印 ----
        name = self._names.get(code, code)
        change_pct = tick["涨跌幅(%)"] or 0
        change_amt = tick["涨跌额"] or 0
        day_high = tick["最高价"] or 0
        day_low = tick["最低价"] or 0

        # 颜色决策：分别计算 sta 和 dyn 的颜色（各自 vs 上一静态 BAR）
        prev_bar = self._prev_sta_bar.get(code, 0.0)
        sta_color = self._get_color(sta.bar, prev_bar)
        dyn_color = self._get_color(dyn.bar, prev_bar)
        sta_rst = RESET if sta_color else ""
        dyn_rst = RESET if dyn_color else ""

        # 极值价格（当前 phase 的最高/最低）
        extreme_str = ""
        phase = self._bar_phase.get(code)
        if phase and self._phase_high.get(code) is not None:
            label = "红柱" if phase == "red" else "绿柱"
            extreme_str = f"{label} H:{self._phase_high[code]:.2f} L:{self._phase_low[code]:.2f}"

        content = (
            f"[{now.strftime('%H:%M:%S')}] {code}  {name:8s}  │  "
            f"{sta_color}sta DIF:{sta.diff:+.4f}  DEA:{sta.dea:+.4f}  BAR:{sta.bar:+.4f}{sta_rst}  │  "
            f"H:{day_high:.2f}  L:{day_low:.2f}  │  "
            f"{price:7.2f}  {change_amt:+.2f}  {change_pct:+.2f}%"
            + (f"  │  {extreme_str}" if extreme_str else "")
            + f"  │  {dyn_color}dyn DIF:{dyn.diff:+.4f}  DEA:{dyn.dea:+.4f}  BAR:{dyn.bar:+.4f}{dyn_rst}"
        )

        # 计算可见长度（去除 ANSI 转义码）
        visible_len = len(re.sub(r'\033\[[0-9;]*m', '', content))
        max_len = self._max_dyn_len.get(code, 0)
        if visible_len > max_len:
            max_len = visible_len
            self._max_dyn_len[code] = max_len
        padding = max_len - visible_len

        n_codes = len(self.codes)

        if n_codes == 1:
            # 单股票：纯 \r 覆写，不换行，光标始终在同一行
            print(f"\r{content}{' ' * padding}\033[K", end="", flush=True)
            self._dyn_lines_printed = 1
        else:
            # 多股票：回退到第一行动态行位置，逐行覆写
            if self._dyn_printed_this_cycle == 0 and self._dyn_lines_printed > 0:
                print(f"\033[{self._dyn_lines_printed}A", end="", flush=True)

            self._dyn_printed_this_cycle += 1

            is_last = (self._dyn_printed_this_cycle == n_codes)
            if is_last:
                print(f"\r{content}{' ' * padding}\033[K", end="", flush=True)
                self._dyn_lines_printed = self._dyn_printed_this_cycle
            else:
                print(f"\r{content}{' ' * padding}\033[K")

    def _finalize_current_minute(
        self, code: str, end_vol: int, end_amount: float,
    ) -> None:
        """当前分钟结束，用 VWAP 固化静态 MACD。

        Args:
            end_vol: 新分钟首 tick 的累计成交量（= 上一分钟的结束值）
            end_amount: 新分钟首 tick 的累计成交额（= 上一分钟的结束值）
        """
        minute = self._current_minute.get(code)
        if minute is None:
            return

        start_vol = self._minute_start_vol.get(code, 0)
        start_amount = self._minute_start_amount.get(code, 0.0)

        # 用累计值反算分钟 VWAP
        vwap = calc_minute_vwap(start_vol, start_amount, end_vol, end_amount)

        # 无成交 → 用上一 tick 价格兜底
        if vwap is None:
            vwap = self._minute_close.get(code)
        if vwap is None or vwap <= 0:
            return

        # 保存旧值（颜色比较基准 + 极值更新用）
        old_sta_bar = self.sta_states[code].bar
        old_phase = self._bar_phase.get(code)

        # 更新静态 MACD
        self.sta_states[code] = step_macd(self.sta_states[code], vwap)
        sta = self.sta_states[code]
        new_bar = sta.bar

        # ---- 阶段判定与极值更新 ----
        if new_bar > 0:
            new_phase = "red"
        elif new_bar < 0:
            new_phase = "green"
        else:
            new_phase = old_phase

        # 阶段切换 → 重置极值
        if new_phase != old_phase:
            if new_phase is not None:
                self._phase_high[code] = vwap
                self._phase_low[code] = vwap
            else:
                self._phase_high[code] = None
                self._phase_low[code] = None
        else:
            # 同阶段延续 → 更新极值
            if new_phase is not None and vwap is not None:
                cur_h = self._phase_high.get(code)
                cur_l = self._phase_low.get(code)
                if cur_h is None:
                    self._phase_high[code] = vwap
                else:
                    self._phase_high[code] = max(cur_h, vwap)
                if cur_l is None:
                    self._phase_low[code] = vwap
                else:
                    self._phase_low[code] = min(cur_l, vwap)

        self._bar_phase[code] = new_phase

        # 更新 prev_sta_bar（新 sta_BAR 成为下一分钟的参考基准）
        self._prev_sta_bar[code] = new_bar

        # ---- 打印分钟固化线（覆写当前动态行，\n 落盘） ----
        name = self._names.get(code, code)
        color = self._get_color(new_bar, old_sta_bar)
        reset = RESET if color else ""

        extreme_str = ""
        if new_phase and self._phase_high.get(code) is not None:
            label = "红柱" if new_phase == "red" else "绿柱"
            extreme_str = f"{label} H:{self._phase_high[code]:.2f} L:{self._phase_low[code]:.2f}"

        print(f"\r── {minute} 固化 {code} {name}  "
              f"VWAP:{vwap:.2f}  "
              f"{color}sta DIF:{sta.diff:+.4f}  DEA:{sta.dea:+.4f}  BAR:{sta.bar:+.4f}{reset}"
              + (f"  {extreme_str}" if extreme_str else "")
              + f" ──\033[K")
        self._dyn_lines_printed = 0

    # ---- CSV 写入 ----

    def _write_csv(self, code: str, tick: dict, dyn: MACDState, sta: MACDState) -> None:
        """将一行 tick + MACD 数据追加写入 CSV。"""
        filepath = self._csv_files.get(code, csv_path(code))
        file_exists = os.path.exists(filepath)

        row = {f: tick.get(f) for f in TICK_CSV_FIELDS}
        row["dyn_DIF"] = round(dyn.diff, 6)
        row["dyn_DEA"] = round(dyn.dea, 6)
        row["dyn_BAR"] = round(dyn.bar, 6)
        row["sta_DIF"] = round(sta.diff, 6)
        row["sta_DEA"] = round(sta.dea, 6)
        row["sta_BAR"] = round(sta.bar, 6)

        with open(filepath, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
            f.flush()
            os.fsync(f.fileno())


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="A-share 日内分钟级 MACD 实时监控（带 ANSI 颜色 + 极值追踪）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python stock/macd_intraday_monitor.py --stocks 601689,002841
  python stock/macd_intraday_monitor.py --config macd_monitor.json
        """,
    )
    parser.add_argument(
        "--stocks", type=str, default=None,
        help="监控股票代码列表，逗号分隔，如 601689,002841,000001",
    )
    parser.add_argument(
        "--config", type=str, default=None,
        help="JSON 配置文件路径，格式: {\"stocks\": [\"601689\", \"002841\"]}",
    )
    args = parser.parse_args()

    # 解析股票列表
    codes: list[str] = []
    if args.config:
        config_path = Path(args.config)
        if not config_path.exists():
            print(f"配置文件不存在: {args.config}")
            sys.exit(1)
        with open(config_path) as f:
            config = json.load(f)
        codes = config.get("stocks", [])
    elif args.stocks:
        codes = [c.strip() for c in args.stocks.split(",") if c.strip()]
    else:
        print("请指定 --stocks 或 --config")
        sys.exit(1)

    if not codes:
        print("监控股票列表为空")
        sys.exit(1)

    print(f"监控股票: {', '.join(codes)}")
    print(f"数据目录: {MONITOR_DIR}")
    print()

    monitor = MACDMonitor(codes)
    try:
        monitor.run()
    except KeyboardInterrupt:
        print("\n用户中断，程序退出")


if __name__ == "__main__":
    main()
