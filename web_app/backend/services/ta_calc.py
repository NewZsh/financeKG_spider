#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ta_calc.py - 技术分析计算工具（道氏 / 艾略特波浪 / 缠论 / 通用指标）
自包含可离线重现。数据获取优先 mootdx(通达信TCP)，降级腾讯HTTP(ifzq.gtimg.cn)。

依赖: pandas numpy mootdx requests   (绘图另需 matplotlib mplfinance)
用法见文件末尾示例。

核心模块:
  get_prefix(code)            股票/指数前缀路由
  fetch_kline(code, period)   取K线 (period: mon/week/day/60/30/15/5/1)
  fractal(df)                 分型识别(顶/底分型)
  bi_list(df)                 缠论"笔"
  duan_list(bi)               缠论"线段"
  zhongshu_list(duan_or_bi)  缠论"中枢"
  bei_chi(df, bi)             缠论"背驰"（MACD面积辅助）
  buy_sell_points(...)        缠论一二三类买卖点
  dow_trend(df)               道氏N字趋势 (上升/下降/盘整)
  elliott_hint(bi, duan)      艾略特波浪辅助标注
  ma/macd/boll/rsi/kdj        通用指标
"""
import json, math, datetime, urllib.request, ssl, sys
import pandas as pd, numpy as np

# ---------------- 数据获取 ----------------

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"

def get_prefix(code: str) -> str:
    """股票/指数前缀路由: 沪深 ETF/指数/股票 → sh/sz。支持显式前缀透传。"""
    c = str(code).strip().lower()
    if c.startswith(("sh", "sz")):
        return c
    if c.startswith("hk"):
        # 港股: hk01378 / hkHSI(恒指) 透传, 仅腾讯源支持
        return "hk" + code.strip()[2:]  # 保留原大小写(hkHSI)
    pure = c.lstrip("shsz")
    if pure.startswith("6") or pure.startswith("5") or pure.startswith("9"):
        return "sh" + pure
    if pure.startswith(("0", "3", "2")):
        return "sz" + pure
    # 沪指数白名单 000xxx/999xxx 视为沪
    if pure.startswith(("0000", "9999")):
        return "sh" + pure
    return "sz" + pure

_PERIOD_TENCENT = {
    "mon": "month", "week": "week", "day": "day",
    "60": "m60", "30": "m30", "15": "m15", "5": "m5", "1": "m1",
}

def _is_index(sym: str) -> bool:
    """判断是否为指数(需带前缀)。上证系: sh000xxx/sh999xxx；深证系: sz399xxx。
    沪市个股为 600/601/603/605/688，深市个股为 000/001/002/003/300，故此判断安全。"""
    pure = sym[2:]
    if sym.startswith("sh") and pure.startswith(("000", "999")):
        return True
    if sym.startswith("sz") and pure.startswith("399"):
        return True
    return False

def _tencent_kline(code: str, period: str, count: int = 800) -> pd.DataFrame:
    """腾讯 ifzq.gtimg.cn K线 (零鉴权, 不封IP). period 见 _PERIOD_TENCENT."""
    sym = get_prefix(code)
    p = _PERIOD_TENCENT.get(period, "d")
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sym},{p},,,{count},qfq"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=15) as r:
        data = json.loads(r.read().decode("utf-8"))
    arr = data.get("data", {}).get(sym, {})
    # 腾讯返回 qfqday/qfqmonth/qfqweek/qfqminute 等不同 key
    rows = None
    period_full = {"mon": "month", "week": "week", "day": "day"}.get(period, p)
    for k in ("qfq" + period_full, "qfq" + p, period_full, p):
        if k in arr and arr[k]:
            rows = arr[k]
            break
    if not rows:
        for k, v in arr.items():
            if isinstance(v, list) and v and isinstance(v[0], list):
                rows = v
                break
    if not rows:
        return pd.DataFrame()
    # 腾讯不同周期/最新未收盘K线的行列数可能不一致(日线6列, 周/月线部分行7列),
    # 统一只取标准6列(datetime/open/close/high/low/volume), 过滤列数不足的行。
    base = ["datetime", "open", "close", "high", "low", "volume"]
    rows = [r[:6] for r in rows if len(r) >= 6]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=base)
    for c in ("open", "close", "high", "low", "volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    return df

def _mootdx_kline(code: str, period: str, count: int = 800) -> pd.DataFrame:
    """通达信 TCP 取K线 (mootdx). 失败抛异常."""
    try:
        from mootdx.quotes import Quotes
    except Exception as e:
        raise RuntimeError("mootdx 未安装或导入失败: pip install mootdx") from e
    q = Quotes.factory(market="std")
    cat = {"mon": 6, "week": 5, "day": 9, "60": 7, "30": 8, "15": 2, "5": 0, "1": 1}.get(period, 9)
    sym = get_prefix(code)
    market = 1 if sym.startswith("sh") else 0
    code_pure = sym[2:]
    # 注意: mootdx bars() 的 category 周期参数对 week/mon 不生效(实测均返回日线),
    # 故 fetch_kline 已将 week/mon 优先路由到腾讯; 此处 mootdx 仅可靠处理 day/分钟线。
    if _is_index(sym):
        # 指数必须用 index() 方法, bars() 会取成同代码个股(如 sh000001 误取平安银行)
        res = q.index(symbol=code_pure, market=market, category=cat, count=count)
    else:
        res = q.bars(symbol=code_pure, category=cat, market=market, count=count)
    if res is None or (hasattr(res, "__len__") and len(res) == 0):
        raise RuntimeError("mootdx 返回空")
    df = res.copy()
    # mootdx 已含 datetime/volume 列, 仅选需要的列
    keep = [c for c in ["datetime", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].copy()
    for c in ("open", "close", "high", "low", "volume"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    return df

def fetch_kline(code: str, period: str = "day", count: int = 800) -> pd.DataFrame:
    """取K线并按周期路由数据源。返回标准列: datetime/open/high/low/close/volume

    路由策略(基于实测):
    - week/mon: mootdx 的 category 周期参数不生效(均返回日线), 故优先腾讯(qfqweek/qfqmonth 正确)
    - day/分钟线: 优先 mootdx(快、稳), 降级腾讯
    - 指数: 两源均已正确处理(mootdx用index(), 腾讯直接支持)
    """
    errors = []
    if str(code).strip().lower().startswith("hk"):
        # 港股仅腾讯源支持(mootdx std 不含港股)
        order = (_tencent_kline,)
    elif period in ("week", "mon"):
        order = (_tencent_kline, _mootdx_kline)
    else:
        order = (_mootdx_kline, _tencent_kline)
    for fn in order:
        try:
            df = fn(code, period, count)
            if df is not None and len(df) > 0:
                return df
        except Exception as e:
            errors.append(f"{fn.__name__}: {e}")
    raise RuntimeError(f"所有数据源失败: {errors}")

# ---------------- 缠论核心 ----------------

def fractal(df: pd.DataFrame) -> pd.DataFrame:
    """顶分型/底分型识别。新增列: fractal = 1顶 / -1底 / 0。"""
    df = df.copy().reset_index(drop=True)
    n = len(df)
    f = [0] * n
    for i in range(1, n - 1):
        hi, lo = df["high"].iloc[i], df["low"].iloc[i]
        if hi > df["high"].iloc[i - 1] and hi > df["high"].iloc[i + 1] and lo > df["low"].iloc[i - 1] and lo > df["low"].iloc[i + 1]:
            f[i] = 1
        elif lo < df["low"].iloc[i - 1] and lo < df["low"].iloc[i + 1] and hi < df["high"].iloc[i - 1] and hi < df["high"].iloc[i + 1]:
            f[i] = -1
    df["fractal"] = f
    return df

def bi_list(df: pd.DataFrame) -> list:
    """缠论'笔'。相邻不同性质分型连线为一笔。返回 [(start_idx, end_idx, direction, start_price, end_price)] direction=1上升笔/-1下降笔。"""
    df = fractal(df)
    pts = [(i, v) for i, v in enumerate(df["fractal"]) if v != 0]
    bi = []
    last = None
    for idx, v in pts:
        if last is None:
            last = (idx, v)
            continue
        if v == last[1]:
            # 同性质: 取更极端者合并
            if v == 1:
                if df["high"].iloc[idx] > df["high"].iloc[last[0]]:
                    last = (idx, v)
            else:
                if df["low"].iloc[idx] < df["low"].iloc[last[0]]:
                    last = (idx, v)
            continue
        # 异性质: 成笔(满足最少5根合并后)
        if abs(idx - last[0]) >= 4:
            sp = df["low"].iloc[last[0]] if last[1] == -1 else df["high"].iloc[last[0]]
            ep = df["high"].iloc[idx] if v == 1 else df["low"].iloc[idx]
            direction = 1 if last[1] == -1 else -1
            bi.append((last[0], idx, direction, sp, ep))
            last = (idx, v)
        else:
            last = (idx, v)
    return bi

def duan_list(bi: list) -> list:
    """缠论'线段'。简化版: 连续3笔方向一致且无破坏视为一段(标准线段判断较复杂，此处给实用近似)。
    返回 [(start_idx, end_idx, direction)]"""
    if len(bi) < 3:
        return []
    duan = []
    s = 0
    while s < len(bi) - 2:
        b0, b1, b2 = bi[s], bi[s + 1], bi[s + 2]
        # 上升线段: b0↑ b1↓ b2↑ 且 b2顶>b0顶
        if b0[2] == 1 and b1[2] == -1 and b2[2] == 1 and b2[4] > b0[4]:
            end = s + 2
            # 向后延伸
            j = s + 3
            while j + 1 < len(bi) and not (bi[j][2] == -1 and bi[j + 1][2] == 1 and bi[j + 1][4] > bi[s + 2][4]):
                j += 1
            duan.append((b0[0], bi[j][1] if j < len(bi) else b2[1], 1))
            s = j + 1
            continue
        # 下降线段
        if b0[2] == -1 and b1[2] == 1 and b2[2] == -1 and b2[4] < b0[4]:
            end = s + 2
            j = s + 3
            while j + 1 < len(bi) and not (bi[j][2] == 1 and bi[j + 1][2] == -1 and bi[j + 1][4] < bi[s + 2][4]):
                j += 1
            duan.append((b0[0], bi[j][1] if j < len(bi) else b2[1], -1))
            s = j + 1
            continue
        s += 1
    return duan

def zhongshu_list(seg: list) -> list:
    """缠论'中枢'。连续3段(方向交替)构成中枢: 取2-3段的重叠区间[ZG,ZD]。
    返回 [(start_idx, end_idx, ZG, ZD, ZG_is_high)]"""
    if len(seg) < 3:
        return []
    zsl = []
    i = 0
    while i + 2 < len(seg):
        a, b, c = seg[i], seg[i + 1], seg[i + 2]
        # 取 a/c 同向(都是上升或下降线段)，b为反向
        segs = [a, c]
        highs = [max(s[3] if hasattr(s, "__len__") and len(s) > 3 else 0, s[1]) for s in segs]
        # 简化: 用每段的起止价格区间
        def seg_range(s):
            return (min(s[3], s[4]) if len(s) > 4 else s[1], max(s[3], s[4]) if len(s) > 4 else s[1])
        r1 = seg_range(a)
        r3 = seg_range(c)
        zg = min(max(r1[1], r3[1]), max(r1[0], r3[0]))  # 上沿
        zd = max(min(r1[0], r3[0]), min(r1[1], r3[1]))  # 下沿
        # 重叠区间存在
        overlap_low = max(r1[0], r3[0])
        overlap_high = min(r1[1], r3[1])
        if overlap_high > overlap_low:
            zsl.append((a[0], c[1], overlap_high, overlap_low, True))
            i += 2
        else:
            i += 1
    return zsl

def macd(df: pd.DataFrame, fast=12, slow=26, signal=9) -> pd.DataFrame:
    close = df["close"]
    ema_f = close.ewm(span=fast, adjust=False).mean()
    ema_s = close.ewm(span=slow, adjust=False).mean()
    dif = ema_f - ema_s
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = (dif - dea) * 2
    df = df.copy()
    df["dif"], df["dea"], df["macd_hist"] = dif, dea, hist
    return df

def bei_chi(df: pd.DataFrame, bi: list) -> list:
    """缠论背驰判定(简化): 同方向相邻两笔, 后者创新高/低但MACD红绿柱面积缩小→背驰。
    返回背驰笔索引列表."""
    df = macd(df)
    flags = []
    for i in range(2, len(bi)):
        cur, prev = bi[i], bi[i - 2]
        if cur[2] != prev[2]:
            continue
        s, e = cur[0], cur[1]
        ps, pe = prev[0], prev[1]
        area_cur = abs(df["macd_hist"].iloc[s:e + 1].sum())
        area_prev = abs(df["macd_hist"].iloc[ps:pe + 1].sum())
        if cur[2] == 1 and cur[4] > prev[4] and area_cur < area_prev:
            flags.append(i)
        elif cur[2] == -1 and cur[4] < prev[4] and area_cur < area_prev:
            flags.append(i)
    return flags

def buy_sell_points(df, bi, duan, zs):
    """缠论一二三类买卖点(启发式):
      一买: 下跌趋势末段背驰+跌破中枢后回拉
      二买: 一买后第一次回踩不创新低
      三买: 离开中枢后回踩不进中枢
    返回 [(idx, type, 'buy'/'sell')]"""
    pts = []
    bc = bei_chi(df, bi)
    for i in bc:
        b = bi[i]
        if b[2] == -1:
            pts.append((b[1], 1, "buy"))
        else:
            pts.append((b[1], 1, "sell"))
    # 三买: 中枢上沿之上的回踩
    for z in zs:
        zg, zd = z[2], z[3]
        for b in bi:
            if b[2] == -1 and b[3] > zg and b[4] > zg:
                pts.append((b[1], 3, "buy"))
    return pts

# ---------------- 道氏趋势 ----------------

def dow_trend(df: pd.DataFrame, lookback: int = 0) -> dict:
    """道氏N字趋势: 上升=高高点+高低点; 下降=低低点+低高点; 否则盘整。
    返回 {trend, last_high, last_low, hh, hl, lh, ll}"""
    f = fractal(df)
    pts = [(i, v, df["high"].iloc[i] if v == 1 else df["low"].iloc[i]) for i, v in enumerate(f["fractal"]) if v != 0]
    if len(pts) < 4:
        return {"trend": "unknown", "points": pts}
    rec = pts[-4:]
    highs = [(i, p) for i, v, p in rec if v == 1]
    lows = [(i, p) for i, v, p in rec if v == -1]
    if len(highs) >= 2 and len(lows) >= 2:
        hh = highs[-1][1] > highs[-2][1]
        hl = lows[-1][1] > lows[-2][1]
        lh = highs[-1][1] < highs[-2][1]
        ll = lows[-1][1] < lows[-2][1]
        if hh and hl:
            t = "up"
        elif lh and ll:
            t = "down"
        else:
            t = "range"
    else:
        t = "range"
    return {"trend": t, "points": rec, "last_high": highs[-1][1] if highs else None,
            "last_low": lows[-1][1] if lows else None}

# ---------------- 艾略特波浪(辅助标注) ----------------

def elliott_hint(bi: list) -> list:
    """艾略特波浪辅助标注: 在一个完整推动(5波)或调整(3波)序列上标1-5/A-B-C。
    规则: 5个同向驱动波 + 3个反向调整波。返回 [(bi_idx, label)]"""
    out = []
    if len(bi) < 5:
        return out
    # 找连续5个上升笔(简化)
    i = 0
    while i + 4 < len(bi):
        window = bi[i:i + 5]
        if all(w[2] == 1 for w in window):
            for k, w in enumerate(window):
                out.append((i + k, str(k + 1)))
            # 紧接3个调整
            if i + 7 < len(bi) and all(bi[i + 5 + k][2] == -1 for k in range(3)):
                for k in range(3):
                    out.append((i + 5 + k, "ABC"[k]))
            i += 8
            continue
        i += 1
    return out

# ---------------- 通用指标 ----------------

def ma(df, n=20):
    return df["close"].rolling(n).mean()

def boll(df, n=20, k=2):
    mid = df["close"].rolling(n).mean()
    std = df["close"].rolling(n).std()
    return mid, mid + k * std, mid - k * std

def rsi(df, n=14):
    diff = df["close"].diff()
    up = diff.clip(lower=0).rolling(n).mean()
    dn = (-diff.clip(upper=0)).rolling(n).mean()
    rs = up / dn
    return 100 - 100 / (1 + rs)

def kdj(df, n=9, m1=3, m2=3):
    lowv = df["low"].rolling(n).min()
    highv = df["high"].rolling(n).max()
    rsv = (df["close"] - lowv) / (highv - lowv) * 100
    k = rsv.ewm(alpha=1 / m1, adjust=False).mean()
    d = k.ewm(alpha=1 / m2, adjust=False).mean()
    j = 3 * k - 2 * d
    return k, d, j

# ---------------- 汇总: 一次出报告 ----------------

def analyze(code: str, period: str = "day", count: int = 500) -> dict:
    """对单只标的一次性跑完整套计算，返回结构化结果(供绘图/报告)."""
    df = fetch_kline(code, period, count)
    if len(df) < 30:
        return {"error": "数据不足30根", "rows": len(df)}
    df = macd(df)
    df["ma5"], df["ma20"], df["ma60"] = ma(df, 5), ma(df, 20), ma(df, 60)
    df["rsi14"] = rsi(df, 14)
    df["k"], df["d"], df["j"] = kdj(df)
    df["boll_mid"], df["boll_up"], df["boll_dn"] = boll(df)
    bi = bi_list(df)
    du = duan_list(bi)
    zs = zhongshu_list(du if du else bi)
    bc = bei_chi(df, bi)
    pts = buy_sell_points(df, bi, du, zs)
    dow = dow_trend(df)
    elli = elliott_hint(bi)
    return {
        "code": code, "period": period, "rows": len(df),
        "last": {"close": float(df["close"].iloc[-1]), "date": str(df["datetime"].iloc[-1])},
        "dow_trend": dow["trend"], "dow_points": [(int(i), int(v), float(p)) for i, v, p in dow["points"]],
        "bi_count": len(bi), "bi": [(int(a), int(b), int(c), float(d), float(e)) for a, b, c, d, e in bi],
        "duan_count": len(du), "duan": [(int(a), int(b), int(c)) for a, b, c in du],
        "zhongshu": [(int(z[0]), int(z[1]), float(z[2]), float(z[3])) for z in zs],
        "beichi": [int(i) for i in bc],
        "points": [(int(i), int(t), s) for i, t, s in pts],
        "elliott": [(int(i), s) for i, s in elli],
        "macd_last": {"dif": float(df["dif"].iloc[-1]), "dea": float(df["dea"].iloc[-1]), "hist": float(df["macd_hist"].iloc[-1])},
        "ma_last": {"ma5": float(df["ma5"].iloc[-1]), "ma20": float(df["ma20"].iloc[-1]), "ma60": float(df["ma60"].iloc[-1])},
        "rsi_last": float(df["rsi14"].iloc[-1]),
        "kdj_last": {"k": float(df["k"].iloc[-1]), "d": float(df["d"].iloc[-1]), "j": float(df["j"].iloc[-1])},
        "_df": df,
    }

# ---------------- CLI ----------------

def _cli():
    import argparse
    ap = argparse.ArgumentParser(description="技术分析计算工具 (道氏/波浪/缠论/指标)")
    ap.add_argument("code", help="股票/指数代码, 如 600519 / 000001(上证指数请用 sh000001)")
    ap.add_argument("-p", "--period", default="day", help="周期: mon/week/day/60/30/15/5/1")
    ap.add_argument("-n", "--count", type=int, default=500, help="K线根数")
    ap.add_argument("--json", action="store_true", help="输出JSON(去掉_df)")
    args = ap.parse_args()
    res = analyze(args.code, args.period, args.count)
    if "error" in res:
        print(res); return
    res.pop("_df", None)
    print(json.dumps(res, ensure_ascii=False, indent=2, default=str))

if __name__ == "__main__":
    _cli()

# 示例:
#   python ta_calc.py 600519 -p day -n 500 --json
#   python ta_calc.py sh000001 -p week -n 200
