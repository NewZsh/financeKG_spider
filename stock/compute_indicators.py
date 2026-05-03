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


def detect_golden_cross_type(ma5: pd.Series, ma20: pd.Series, close: pd.Series) -> str:
    n = len(ma5)
    if n < MA_LONG + 5:
        return "none"
    if pd.isna(ma5.iloc[-1]) or pd.isna(ma20.iloc[-1]) or ma5.iloc[-1] <= ma20.iloc[-1]:
        return "none"

    cross_idx = None
    for index in range(n - 1, max(0, n - 10), -1):
        if index - 1 < 0:
            break
        if any(pd.isna(series.iloc[index]) or pd.isna(series.iloc[index - 1]) for series in (ma5, ma20)):
            continue
        if ma5.iloc[index] > ma20.iloc[index] and ma5.iloc[index - 1] <= ma20.iloc[index - 1]:
            cross_idx = index
            break

    if cross_idx is None:
        for index in range(max(0, n - 10), max(0, n - 60), -1):
            if index - 1 < 0:
                break
            if any(pd.isna(series.iloc[index]) or pd.isna(series.iloc[index - 1]) for series in (ma5, ma20)):
                continue
            if ma5.iloc[index] > ma20.iloc[index] and ma5.iloc[index - 1] <= ma20.iloc[index - 1]:
                cross_idx = index
                break

    if cross_idx is None:
        return "none"

    cross_age = n - 1 - cross_idx
    if cross_age > 10:
        return "none"

    lookback = min(60, len(close))
    low_60 = close.iloc[-lookback:].min()
    if low_60 > 0 and (close.iloc[-1] / low_60 - 1) > 0.30:
        return "high"

    start = max(0, cross_idx - min(30, n - 1))
    for index in range(cross_idx - 1, start, -1):
        if index - 1 < 0:
            break
        if any(pd.isna(series.iloc[index]) or pd.isna(series.iloc[index - 1]) for series in (ma5, ma20)):
            continue
        if ma5.iloc[index] < ma20.iloc[index] and ma5.iloc[index - 1] >= ma20.iloc[index - 1]:
            return "second"

    return "first"


def get_shrinking_down_day_volumes(
    close: pd.Series,
    volume: pd.Series,
    lookback: int = SHRINKING_DOWN_LOOKBACK,
) -> list[float]:
    if len(close) < 2 or len(volume) < lookback:
        return []

    recent_close = close.iloc[-lookback:]
    recent_volume = volume.iloc[-lookback:]
    volume_mean = recent_volume.mean()
    if volume_mean <= 0 or math.isnan(volume_mean):
        return []

    shrinking_down_volumes: list[float] = []
    for index in range(1, len(recent_close)):
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

    for group_name in ("trend_confirmation", "position_quality"):
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


def analyze_stock(code: str, name: str, kline: pd.DataFrame, scoring_mode: str = "dedup") -> dict | None:
    if kline.empty or len(kline) < MA_LONG + 5:
        return None

    close = kline["close"].astype(float)
    amount = kline["amount"].astype(float)
    high = kline["high"].astype(float)

    ma5 = calc_ma(close, MA_SHORT)
    ma20 = calc_ma(close, MA_LONG)
    if pd.isna(ma5.iloc[-1]) or pd.isna(ma20.iloc[-1]) or ma5.iloc[-1] <= ma20.iloc[-1]:
        return None

    cross_type = detect_golden_cross_type(ma5, ma20, close)
    if cross_type == "none":
        return None

    angle = calc_ma_angle(ma5.iloc[-1], ma20.iloc[-1], ma5.iloc[-2], ma20.iloc[-2])

    ma10 = calc_ma(close, 10)
    ma60 = calc_ma(close, 60)
    bullish_alignment = bool(
        not pd.isna(ma10.iloc[-1])
        and not pd.isna(ma60.iloc[-1])
        and ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1] > ma60.iloc[-1]
    )

    if len(amount) < 21:
        return None
    avg_amount_20 = amount.iloc[-21:-1].mean()
    if avg_amount_20 == 0 or math.isnan(avg_amount_20):
        return None
    vol_ratio = amount.iloc[-1] / avg_amount_20
    bias = (close.iloc[-1] - ma20.iloc[-1]) / ma20.iloc[-1]
    if bias > MAX_BIAS_RATIO:
        return None

    rsi = calc_rsi(close)
    rsi_val = rsi.iloc[-1]
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

    shrinking_down_volumes = get_shrinking_down_day_volumes(close, amount)
    shrinking_down_count = len(shrinking_down_volumes)
    if shrinking_down_count >= SHRINKING_DOWN_MIN_COUNT:
        required_breakout_amount = max(shrinking_down_volumes) * BREAKOUT_VOLUME_VS_PULLBACK_RATIO
        if amount.iloc[-1] < required_breakout_amount:
            return None

    score_components: dict[str, list[tuple[str, int]]] = {
        "base": [],
        "trend_confirmation": [],
        "position_quality": [],
    }
    if cross_type == "first":
        score_components["base"].append(("首次金叉", SIGNAL_WEIGHTS["golden_cross_first"]))
    elif cross_type == "second":
        score_components["base"].append(("二次金叉", SIGNAL_WEIGHTS["golden_cross_second"]))
    elif cross_type == "high":
        score_components["base"].append(("高位金叉", SIGNAL_WEIGHTS["golden_cross_high"]))

    if breakout_ready_second_cross:
        score_components["base"].append(("临近前高的强二次金叉", SIGNAL_WEIGHTS["second_cross_breakout_ready"]))

    if bullish_alignment:
        score_components["trend_confirmation"].append(("多头排列", 15))

    if shrinking_down_count >= SHRINKING_DOWN_MIN_COUNT:
        score_components["trend_confirmation"].append(
            (f"60日内两次缩量下跌({shrinking_down_count}次)", SIGNAL_WEIGHTS["pullback_shrink_twice"])
        )

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
    }