#!/usr/bin/env python3

from __future__ import annotations

import math

import numpy as np
import pandas as pd


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
    "liquidity_low": -20,
    "liquidity_moderate": 8,
    "liquidity_good": 20,
    "liquidity_hot": -10,
}

GROUP_SCORE_CAPS = {
    "trend_confirmation": 50, # 提升上限以容纳爆发倍量加分
    "position_quality": 10,
    "market_cap": 30, # 新增的大市值专项加分上限（最大容许加30分）
    "liquidity": 20,
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
TREND_INIT_MAX_BIAS_RATIO = 0.05
TREND_INIT_MIN_VOLUME_RATIO = 1.0
TREND_INIT_MAX_CROSS_AGE = 10
TREND_INIT_MA20_HOLD_RATIO = 0.995

STRATEGY_TREND_INIT = "trend_init"
STRATEGY_BREAKOUT_ACCEL = "breakout_accel"
STRATEGY_LABELS = {
    STRATEGY_TREND_INIT: "趋势建立初期型",
    STRATEGY_BREAKOUT_ACCEL: "突破加速型",
}

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
GOLDEN_CROSS_DECAY_DAYS = 20
GOLDEN_CROSS_DECAY_FLOOR = 0.1
MARKET_CAP_PENALTY_THRESHOLD_YI = 2000
LIQUIDITY_RATIO_MIN_PCT = 2.0
LIQUIDITY_RATIO_GOOD_LOW_PCT = 5.0
LIQUIDITY_RATIO_GOOD_HIGH_PCT = 15.0
LIQUIDITY_RATIO_RISK_PCT = 30.0


def calc_ma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window).mean()


def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def calc_ma_angle(ma_short_val: float, ma_long_val: float, ma_short_prev: float, ma_long_prev: float) -> float:
    try:
        slope_short = (ma_short_val - ma_short_prev) / ma_short_prev * 100
        slope_long = (ma_long_val - ma_long_prev) / ma_long_prev * 100
        angle = abs(np.degrees(np.arctan(slope_short - slope_long)))
        return round(angle, 1)
    except (ZeroDivisionError, TypeError, ValueError):
        return 0.0


def calc_golden_cross_decay_factor(cross_age: int, decay_days: int = GOLDEN_CROSS_DECAY_DAYS) -> float:
    if cross_age <= 0:
        return 1.0
    if cross_age >= decay_days:
        return GOLDEN_CROSS_DECAY_FLOOR
    return GOLDEN_CROSS_DECAY_FLOOR ** (cross_age / decay_days)


def score_liquidity_ratio(liquidity_ratio_pct: float | None) -> tuple[str | None, int]:
    if liquidity_ratio_pct is None:
        return None, 0

    ratio = float(liquidity_ratio_pct)
    if ratio < LIQUIDITY_RATIO_MIN_PCT:
        penalty = -max(1, int(round((LIQUIDITY_RATIO_MIN_PCT - ratio) / LIQUIDITY_RATIO_MIN_PCT * 20)))
        return f"流动性不足({ratio:.1f}%)", penalty
    if ratio < LIQUIDITY_RATIO_GOOD_LOW_PCT:
        score = int(round((ratio - LIQUIDITY_RATIO_MIN_PCT) / (LIQUIDITY_RATIO_GOOD_LOW_PCT - LIQUIDITY_RATIO_MIN_PCT) * 8))
        return f"流动性起步({ratio:.1f}%)", score
    if ratio <= LIQUIDITY_RATIO_GOOD_HIGH_PCT:
        score = 8 + int(round((ratio - LIQUIDITY_RATIO_GOOD_LOW_PCT) / (LIQUIDITY_RATIO_GOOD_HIGH_PCT - LIQUIDITY_RATIO_GOOD_LOW_PCT) * 12))
        return f"流动性良好({ratio:.1f}%)", score
    if ratio <= LIQUIDITY_RATIO_RISK_PCT:
        score = 20 - int(round((ratio - LIQUIDITY_RATIO_GOOD_HIGH_PCT) / (LIQUIDITY_RATIO_RISK_PCT - LIQUIDITY_RATIO_GOOD_HIGH_PCT) * 30))
        return f"流动性偏热({ratio:.1f}%)", score

    penalty = -10 - int(round(min((ratio - LIQUIDITY_RATIO_RISK_PCT) / 10 * 10, 10)))
    return f"流动性过热({ratio:.1f}%)", penalty


def detect_golden_cross_type_and_age(ma5: pd.Series, ma20: pd.Series, close: pd.Series) -> tuple[str, int | None]:
    n = len(ma5)
    if n < MA_LONG + 5:
        return "none", None
    if pd.isna(ma5.iloc[-1]) or pd.isna(ma20.iloc[-1]) or ma5.iloc[-1] <= ma20.iloc[-1]:
        return "none", None

    cross_idx = None
    for index in range(n - 1, max(0, n - GOLDEN_CROSS_DECAY_DAYS - 1), -1):
        if index - 1 < 0:
            break
        if any(pd.isna(series.iloc[index]) or pd.isna(series.iloc[index - 1]) for series in (ma5, ma20)):
            continue
        if ma5.iloc[index] > ma20.iloc[index] and ma5.iloc[index - 1] <= ma20.iloc[index - 1]:
            cross_idx = index
            break

    if cross_idx is None:
        for index in range(max(0, n - GOLDEN_CROSS_DECAY_DAYS - 1), max(0, n - 60), -1):
            if index - 1 < 0:
                break
            if any(pd.isna(series.iloc[index]) or pd.isna(series.iloc[index - 1]) for series in (ma5, ma20)):
                continue
            if ma5.iloc[index] > ma20.iloc[index] and ma5.iloc[index - 1] <= ma20.iloc[index - 1]:
                cross_idx = index
                break

    if cross_idx is None:
        return "none", None

    cross_age = n - 1 - cross_idx
    if cross_age > GOLDEN_CROSS_DECAY_DAYS:
        return "none", None

    lookback = min(60, len(close))
    low_60 = close.iloc[-lookback:].min()
    if low_60 > 0 and (close.iloc[-1] / low_60 - 1) > 0.30:
        return "high", cross_age

    start = max(0, cross_idx - min(30, n - 1))
    for index in range(cross_idx - 1, start, -1):
        if index - 1 < 0:
            break
        if any(pd.isna(series.iloc[index]) or pd.isna(series.iloc[index - 1]) for series in (ma5, ma20)):
            continue
        if ma5.iloc[index] < ma20.iloc[index] and ma5.iloc[index - 1] >= ma20.iloc[index - 1]:
            return "second", cross_age

    return "first", cross_age


def detect_golden_cross_type(ma5: pd.Series, ma20: pd.Series, close: pd.Series) -> str:
    cross_type, _ = detect_golden_cross_type_and_age(ma5, ma20, close)
    return cross_type


def get_shrinking_down_day_volumes(
    close: pd.Series,
    volume: pd.Series,
    lookback: int = SHRINKING_DOWN_LOOKBACK,
) -> list[float]:
    if len(close) < 2 or len(volume) < lookback:
        return []

    recent_close = close.iloc[-lookback:]
    recent_volume = volume.iloc[-lookback:]
    
    # 寻找这 60 天内的“前高”（收盘价最高点）所在索引
    # 这样回踩的时间段就被严格限制在 MIN(60, 距离前高的天数)
    pre_high_idx = int(recent_close.argmax())
    
    volume_mean = recent_volume.mean()
    if volume_mean <= 0 or math.isnan(volume_mean):
        return []

    shrinking_down_volumes: list[float] = []
    # 只从“前高”那一天之后开始往后统计缩量回踩，之前的缩量将不纳入本次统计
    for index in range(pre_high_idx + 1, len(recent_close)):
        is_down_day = recent_close.iloc[index] < recent_close.iloc[index - 1]
        volume_value = recent_volume.iloc[index]
        prev_volume = recent_volume.iloc[index - 1]
        is_shrinking_volume = volume_value < volume_mean * SHRINKING_DOWN_RATIO and volume_value < prev_volume
        if is_down_day and is_shrinking_volume:
            shrinking_down_volumes.append(float(volume_value))

    return shrinking_down_volumes


def count_shrinking_down_days(close: pd.Series, volume: pd.Series, lookback: int = SHRINKING_DOWN_LOOKBACK) -> int:
    return len(get_shrinking_down_day_volumes(close, volume, lookback=lookback))


def summarize_score_components(score_components: dict[str, list[tuple[str, int]]]) -> tuple[int, list[tuple[str, int]], dict[str, int]]:
    total_score = 0
    signals: list[tuple[str, int]] = []
    group_scores: dict[str, int] = {}

    for group_name, components in score_components.items():
        group_score = sum(weight for _, weight in components)
        group_scores[group_name] = group_score
        total_score += group_score
        signals.extend(components)

    return total_score, signals, group_scores


def build_dedup_score_components(score_components: dict[str, list[tuple[str, int]]]) -> dict[str, list[tuple[str, int]]]:
    dedup_components: dict[str, list[tuple[str, int]]] = {
        "base": list(score_components.get("base", [])),
    }

    for group_name in ("trend_confirmation", "position_quality", "market_cap", "liquidity"):
        group_items = score_components.get(group_name, [])
        positive_components = sorted((item for item in group_items if item[1] > 0), key=lambda item: item[1], reverse=True)
        negative_components = [item for item in group_items if item[1] < 0]
        cap = GROUP_SCORE_CAPS[group_name]
        running_score = 0
        dedup_group: list[tuple[str, int]] = []
        for signal_name, raw_weight in positive_components:
            if running_score >= cap:
                break
            adjusted_weight = min(raw_weight, cap - running_score)
            if adjusted_weight <= 0:
                continue
            signal_label = signal_name if adjusted_weight == raw_weight else f"{signal_name}[去重后{adjusted_weight}]"
            dedup_group.append((signal_label, adjusted_weight))
            running_score += adjusted_weight
        dedup_group.extend(negative_components)
        dedup_components[group_name] = dedup_group

    return dedup_components


def is_breakout_ready_second_cross_setup(
    cross_type: str,
    vol_ratio: float,
    rsi_val: float,
    bias: float,
    space_to_high: float,
) -> bool:
    return (
        cross_type == "second"
        and space_to_high < SPACE_TO_HIGH_TIGHT_THRESHOLD
        and vol_ratio >= BREAKOUT_READY_MIN_VOLUME_RATIO
        and rsi_val <= BREAKOUT_READY_MAX_RSI
        and bias <= BREAKOUT_READY_MAX_BIAS_RATIO
    )


def build_space_to_high_penalty_components(space_to_high: float, breakout_ready_second_cross: bool = False) -> dict[str, list[tuple[str, int]]]:
    components = {
        "base": [],
        "trend_confirmation": [],
        "position_quality": [],
    }
    if space_to_high < SPACE_TO_HIGH_TIGHT_THRESHOLD:
        penalty = SPACE_TO_HIGH_BREAKOUT_READY_PENALTY if breakout_ready_second_cross else SPACE_TO_HIGH_TIGHT_PENALTY
        components["position_quality"].append((f"前高压力过近({space_to_high:.1%})", penalty))
    elif space_to_high < SPACE_TO_HIGH_WARN_THRESHOLD:
        components["position_quality"].append((f"前高压力偏大({space_to_high:.1%})", SPACE_TO_HIGH_WARN_PENALTY))
    return components


def detect_strategy_setups(
    cross_type: str,
    cross_age: int | None,
    shrinking_down_count: int,
    bias: float,
    vol_ratio: float,
    is_up_day: bool,
    space_to_high: float,
    breakout_ready_second_cross: bool,
    close_price: float,
    ma20_price: float,
    low_price: float,
) -> list[str]:
    strategy_setups: list[str] = []

    holds_ma20 = ma20_price > 0 and close_price >= ma20_price and low_price >= ma20_price * TREND_INIT_MA20_HOLD_RATIO
    trend_init_ready = (
        cross_type in ("first", "second")
        and (cross_age or 0) <= TREND_INIT_MAX_CROSS_AGE
        and shrinking_down_count >= 1
        and bias <= TREND_INIT_MAX_BIAS_RATIO
        and is_up_day
        and vol_ratio >= TREND_INIT_MIN_VOLUME_RATIO
        and holds_ma20
    )
    if trend_init_ready:
        strategy_setups.append(STRATEGY_TREND_INIT)

    breakout_accel_ready = (
        cross_type == "second"
        and shrinking_down_count >= SHRINKING_DOWN_MIN_COUNT
        and breakout_ready_second_cross
        and is_up_day
        and vol_ratio >= VOLUME_BREAKOUT
        and space_to_high < SPACE_TO_HIGH_TIGHT_THRESHOLD
    )
    if breakout_accel_ready:
        strategy_setups.append(STRATEGY_BREAKOUT_ACCEL)

    return strategy_setups


def analyze_stock(
    code: str,
    name: str,
    kline: pd.DataFrame,
    scoring_mode: str = "dedup",
    float_mv_yi: float = None,
    liquidity_ratio_pct: float | None = None,
) -> dict | None:
    if kline.empty or len(kline) < MA_LONG + 5:
        return None

    close = kline["close"].astype(float)
    volume = kline["volume"].astype(float)
    high = kline["high"].astype(float)

    if "ma5" in kline.columns:
        ma5 = kline["ma5"].astype(float)
        ma20 = kline["ma20"].astype(float)
        ma10 = kline["ma10"].astype(float)
        ma60 = kline["ma60"].astype(float)
        rsi_val = float(kline["rsi"].iloc[-1])
    else:
        ma5 = calc_ma(close, MA_SHORT)
        ma20 = calc_ma(close, MA_LONG)
        ma10 = calc_ma(close, 10)
        ma60 = calc_ma(close, 60)
        rsi = calc_rsi(close)
        rsi_val = float(rsi.iloc[-1])

    if pd.isna(ma5.iloc[-1]) or pd.isna(ma20.iloc[-1]) or ma5.iloc[-1] <= ma20.iloc[-1]:
        return None

    cross_type, cross_age = detect_golden_cross_type_and_age(ma5, ma20, close)
    if cross_type == "none":
        return None
    cross_decay_factor = calc_golden_cross_decay_factor(cross_age or 0)

    angle = calc_ma_angle(ma5.iloc[-1], ma20.iloc[-1], ma5.iloc[-2], ma20.iloc[-2])

    bullish_alignment = bool(
        not pd.isna(ma10.iloc[-1])
        and not pd.isna(ma60.iloc[-1])
        and ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1]
    )

    if len(volume) < 21:
        return None
    avg_volume_20 = volume.iloc[-21:-1].mean()
    if avg_volume_20 == 0 or math.isnan(avg_volume_20):
        return None
    vol_ratio = volume.iloc[-1] / avg_volume_20
    latest_open = float(kline["open"].iloc[-1])
    latest_low = float(kline["low"].iloc[-1])
    is_up_day = (
        len(close) >= 2
        and float(kline["close"].iloc[-1]) > latest_open
        and close.iloc[-1] > close.iloc[-2]
    )
    bias = (close.iloc[-1] - ma20.iloc[-1]) / ma20.iloc[-1]

    if pd.isna(rsi_val) or rsi_val > MAX_RSI:
        return None

    high_60 = high.iloc[-60:].max() if len(high) >= 60 else high.max()
    space_to_high = (high_60 / close.iloc[-1] - 1) if close.iloc[-1] > 0 else 0
    breakout_ready_second_cross = is_breakout_ready_second_cross_setup(
        cross_type=cross_type,
        vol_ratio=float(vol_ratio),
        rsi_val=float(rsi_val),
        bias=float(bias),
        space_to_high=float(space_to_high),
    )

    shrinking_down_volumes = get_shrinking_down_day_volumes(close, volume)
    shrinking_down_count = len(shrinking_down_volumes)
    breakout_multiplier = 0.0
    if shrinking_down_count >= SHRINKING_DOWN_MIN_COUNT:
        max_shrink_vol = max(shrinking_down_volumes)
        if max_shrink_vol > 0:
            breakout_multiplier = volume.iloc[-1] / max_shrink_vol
            if breakout_multiplier < 1.0:
                return None

    score_components: dict[str, list[tuple[str, int]]] = {
        "base": [],
        "trend_confirmation": [],
        "position_quality": [],
        "market_cap": [],
        "liquidity": [],
    }
    if cross_type == "first":
        score_components["base"].append(("首次金叉", max(1, int(SIGNAL_WEIGHTS["golden_cross_first"] * cross_decay_factor))))
    elif cross_type == "second":
        score_components["base"].append(("二次金叉", max(1, int(SIGNAL_WEIGHTS["golden_cross_second"] * cross_decay_factor))))
    elif cross_type == "high":
        score_components["base"].append(("高位金叉", max(1, int(SIGNAL_WEIGHTS["golden_cross_high"] * cross_decay_factor))))

    if breakout_ready_second_cross:
        score_components["base"].append(("临近前高的强二次金叉", SIGNAL_WEIGHTS["second_cross_breakout_ready"]))

    if bullish_alignment:
        score_components["trend_confirmation"].append(("多头排列", 15))

    if shrinking_down_count >= SHRINKING_DOWN_MIN_COUNT:
        score_components["trend_confirmation"].append(
            (f"60日内两次缩量下跌({shrinking_down_count}次)", SIGNAL_WEIGHTS["pullback_shrink_twice"])
        )
        if breakout_multiplier >= 1.0:
            bonus = min(int((breakout_multiplier - 1.0) * 15), 30)
            if bonus > 0:
                score_components["trend_confirmation"].append((f"爆发倍量({breakout_multiplier:.2f}x)", bonus))

    if is_up_day:
        if vol_ratio >= 2.0:
            score_components["trend_confirmation"].append((f"强放量突破({vol_ratio:.1f}x)", SIGNAL_WEIGHTS["volume_strong"]))
        elif vol_ratio >= VOLUME_BREAKOUT:
            score_components["trend_confirmation"].append((f"放量突破({vol_ratio:.1f}x)", SIGNAL_WEIGHTS["volume_breakout"]))
        elif vol_ratio >= VOLUME_MODERATE:
            score_components["trend_confirmation"].append((f"温和放量({vol_ratio:.1f}x)", SIGNAL_WEIGHTS["volume_moderate"]))
        elif vol_ratio >= VOLUME_WEAK:
            score_components["trend_confirmation"].append((f"轻度放量({vol_ratio:.1f}x)", SIGNAL_WEIGHTS["volume_light"]))

    if angle > 15:
        score_components["trend_confirmation"].append((f"强势夹角({angle}°)", SIGNAL_WEIGHTS["ma_angle_strong"]))
    elif angle > 5:
        score_components["trend_confirmation"].append((f"温和夹角({angle}°)", SIGNAL_WEIGHTS["ma_angle_moderate"]))

    if 40 <= rsi_val <= 60:
        score_components["position_quality"].append((f"RSI健康({rsi_val:.0f})", SIGNAL_WEIGHTS["rsi_healthy"]))
    elif 30 <= rsi_val <= 75:
        score_components["position_quality"].append((f"RSI可接受({rsi_val:.0f})", SIGNAL_WEIGHTS["rsi_ok"]))

    if bias < 0.05:
        score_components["position_quality"].append((f"低乖离({bias:.1%})", SIGNAL_WEIGHTS["bias_low"]))
    elif bias < 0.10:
        score_components["position_quality"].append((f"乖离适中({bias:.1%})", SIGNAL_WEIGHTS["bias_ok"]))
    elif bias > 0.10:
        # 乖离率大于10%才扣分，由于越大于10%扣分的边际效益越高（二次方扣分），但在20%内不要扣太多（如 15%扣2分，20%扣8分，25%扣18分，30%扣32分）
        excess_bias = bias - 0.10
        bias_penalty = int((excess_bias * 100) ** 2 * -0.08) # -0.08的系数正好让 10%(excess)= -8分
        score_components["position_quality"].append((f"乖离偏高警告({bias:.1%})", bias_penalty))

    if float_mv_yi is not None:
        if float_mv_yi < 20:
            return None # 20亿以下一票否决
        if float_mv_yi > MARKET_CAP_PENALTY_THRESHOLD_YI:
            # 超过 2000 亿后开始扣分，市值越大扣得越多，但保持对数级增长，避免过度惩罚。
            excess_scale = math.log10(float_mv_yi / MARKET_CAP_PENALTY_THRESHOLD_YI)
            mv_penalty = -max(1, int(excess_scale * 12))
            score_components["market_cap"].append((f"超大市值扣分({float_mv_yi:.1f}亿)", mv_penalty))

    liquidity_label, liquidity_score = score_liquidity_ratio(liquidity_ratio_pct)
    if liquidity_label is not None:
        score_components["liquidity"].append((liquidity_label, liquidity_score))

    legacy_score, legacy_signals, legacy_group_scores = summarize_score_components(score_components)
    dedup_score_components = build_dedup_score_components(score_components)
    space_penalty_components = build_space_to_high_penalty_components(
        space_to_high,
        breakout_ready_second_cross=breakout_ready_second_cross,
    )
    for group_name, components in space_penalty_components.items():
        dedup_score_components.setdefault(group_name, []).extend(components)
    dedup_score, dedup_signals, dedup_group_scores = summarize_score_components(dedup_score_components)

    selected_score = dedup_score if scoring_mode == "dedup" else legacy_score
    selected_signals = dedup_signals if scoring_mode == "dedup" else legacy_signals

    if scoring_mode == "dedup" and selected_score < DEDUP_MIN_SIGNAL_SCORE:
        return None

    strategy_setups = detect_strategy_setups(
        cross_type=cross_type,
        cross_age=cross_age,
        shrinking_down_count=shrinking_down_count,
        bias=float(bias),
        vol_ratio=float(vol_ratio),
        is_up_day=is_up_day,
        space_to_high=float(space_to_high),
        breakout_ready_second_cross=breakout_ready_second_cross,
        close_price=float(close.iloc[-1]),
        ma20_price=float(ma20.iloc[-1]),
        low_price=latest_low,
    )
    if not strategy_setups:
        return None

    primary_strategy = (
        STRATEGY_BREAKOUT_ACCEL if STRATEGY_BREAKOUT_ACCEL in strategy_setups else strategy_setups[0]
    )

    return {
        "code": code,
        "name": name,
        "close": round(float(close.iloc[-1]), 2),
        "pct_change": round(((close.iloc[-1] / close.iloc[-2]) - 1) * 100, 2) if len(close) >= 2 else 0.0,
        "cross_type": cross_type,
        "cross_type_cn": {"first": "首次金叉", "second": "二次金叉", "high": "高位金叉"}.get(cross_type, ""),
        "vol_ratio": round(float(vol_ratio), 2),
        "angle": angle,
        "rsi": round(float(rsi_val), 1),
        "bias": round(float(bias) * 100, 2),
        "space_to_high": round(float(space_to_high) * 100, 1),
        "ma5": round(float(ma5.iloc[-1]), 2),
        "ma20": round(float(ma20.iloc[-1]), 2),
        "bullish_alignment": bullish_alignment,
        "score": selected_score,
        "signals": selected_signals,
        "score_mode": scoring_mode,
        "legacy_score": legacy_score,
        "dedup_score": dedup_score,
        "legacy_signals": legacy_signals,
        "dedup_signals": dedup_signals,
        "score_groups": {
            "legacy": legacy_group_scores,
            "dedup": dedup_group_scores,
        },
        "liquidity_ratio_pct": round(float(liquidity_ratio_pct), 2) if liquidity_ratio_pct is not None else None,
        "strategy_setups": strategy_setups,
        "strategy_labels": [STRATEGY_LABELS[item] for item in strategy_setups],
        "primary_strategy": primary_strategy,
        "primary_strategy_label": STRATEGY_LABELS[primary_strategy],
    }