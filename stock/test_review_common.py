from unittest.mock import patch

import pandas as pd

from stock.compute_indicators import analyze_stock


def build_kline(with_shrinking_down_days: bool) -> pd.DataFrame:
    close_values = []
    price = 10.0
    for index in range(70):
        if index < 58:
            price += 0.02
        elif index == 58:
            price -= 0.03
        elif index == 63:
            price -= 0.02
        else:
            price += 0.015
        close_values.append(round(price, 2))

    volume_values = [1000.0] * 70
    volume_values[-20:] = [1200.0] * 20
    volume_values[-1] = 1800.0
    if with_shrinking_down_days:
        volume_values[57] = 1100.0
        volume_values[58] = 700.0
        volume_values[62] = 1080.0
        volume_values[63] = 680.0
    else:
        volume_values[57] = 1100.0
        volume_values[58] = 1120.0
        volume_values[62] = 1080.0
        volume_values[63] = 1090.0

    rows = []
    for index, close in enumerate(close_values):
        rows.append(
            {
                "date": pd.Timestamp("2026-01-01") + pd.Timedelta(days=index),
                "open": round(close - 0.05, 2),
                "high": round(close * 1.3, 2),
                "low": round(close - 0.15, 2),
                "close": close,
                "volume": volume_values[index],
                "amount": volume_values[index],
            }
        )
    return pd.DataFrame(rows)


def test_analyze_stock_adds_score_for_two_shrinking_down_days() -> None:
    shrinking_df = build_kline(with_shrinking_down_days=True)
    plain_df = build_kline(with_shrinking_down_days=False)

    with patch("stock.compute_indicators.detect_golden_cross_type", return_value="first"), patch(
        "stock.compute_indicators.calc_rsi",
        return_value=pd.Series([55.0] * len(shrinking_df)),
    ):
        shrinking_result = analyze_stock("000001", "样例", shrinking_df)
        plain_result = analyze_stock("000001", "样例", plain_df)

    assert shrinking_result is not None
    assert plain_result is not None

    signal_names = [item[0] for item in shrinking_result["signals"]]
    shrink_signals = [name for name in signal_names if name.startswith("60日内两次缩量下跌")]

    assert shrink_signals
    assert shrinking_result["legacy_score"] - plain_result["legacy_score"] == 30
    assert shrinking_result["dedup_score"] >= plain_result["dedup_score"]
    assert shrinking_result["legacy_score"] >= shrinking_result["dedup_score"]


def test_analyze_stock_defaults_to_dedup_mode() -> None:
    shrinking_df = build_kline(with_shrinking_down_days=True)

    with patch("stock.compute_indicators.detect_golden_cross_type", return_value="first"), patch(
        "stock.compute_indicators.calc_rsi",
        return_value=pd.Series([55.0] * len(shrinking_df)),
    ):
        result = analyze_stock("000001", "样例", shrinking_df)

    assert result is not None
    assert result["score_mode"] == "dedup"
    assert result["score"] == result["dedup_score"]
    assert result["legacy_score"] > result["dedup_score"]


def test_analyze_stock_penalizes_tight_space_to_high_in_dedup_mode() -> None:
    roomy_df = build_kline(with_shrinking_down_days=True)
    tight_df = roomy_df.copy()
    tight_df["high"] = (tight_df["close"] * 1.03).round(2)

    with patch("stock.compute_indicators.detect_golden_cross_type", return_value="first"), patch(
        "stock.compute_indicators.calc_rsi",
        return_value=pd.Series([55.0] * len(roomy_df)),
    ):
        roomy_result = analyze_stock("000001", "样例", roomy_df)
        tight_dedup_result = analyze_stock("000001", "样例", tight_df)
        tight_legacy_result = analyze_stock("000001", "样例", tight_df, scoring_mode="legacy")

    assert roomy_result is not None
    assert tight_dedup_result is None
    assert tight_legacy_result is not None


def test_analyze_stock_keeps_breakout_ready_second_cross_with_tight_space_to_high() -> None:
    breakout_ready_df = build_kline(with_shrinking_down_days=False)
    breakout_ready_df["high"] = (breakout_ready_df["close"] * 1.03).round(2)
    breakout_ready_df.loc[breakout_ready_df.index[-1], ["volume", "amount"]] = 2400.0

    with patch("stock.compute_indicators.detect_golden_cross_type", return_value="second"), patch(
        "stock.compute_indicators.calc_rsi",
        return_value=pd.Series([55.0] * len(breakout_ready_df)),
    ):
        result = analyze_stock("000001", "样例", breakout_ready_df)

    assert result is not None
    assert result["dedup_score"] >= 85
    signal_names = [item[0] for item in result["signals"]]
    assert "临近前高的强二次金叉" in signal_names


def test_analyze_stock_requires_breakout_volume_to_double_prior_shrinking_down_days() -> None:
    insufficient_breakout_df = build_kline(with_shrinking_down_days=True)
    insufficient_breakout_df.loc[insufficient_breakout_df.index[-1], ["volume", "amount"]] = 1300.0

    with patch("stock.compute_indicators.detect_golden_cross_type", return_value="first"), patch(
        "stock.compute_indicators.calc_rsi",
        return_value=pd.Series([55.0] * len(insufficient_breakout_df)),
    ):
        result = analyze_stock("000001", "样例", insufficient_breakout_df)

    assert result is None


def test_analyze_stock_does_not_filter_low_volume_ratio_anymore() -> None:
    weak_volume_df = build_kline(with_shrinking_down_days=False)
    weak_volume_df.loc[weak_volume_df.index[-21:-1], ["volume", "amount"]] = 1000.0
    weak_volume_df.loc[weak_volume_df.index[-1], ["volume", "amount"]] = 600.0

    with patch("stock.compute_indicators.detect_golden_cross_type", return_value="first"), patch(
        "stock.compute_indicators.calc_rsi",
        return_value=pd.Series([55.0] * len(weak_volume_df)),
    ):
        result = analyze_stock("000001", "样例", weak_volume_df, scoring_mode="legacy")

    assert result is not None
    signal_names = [item[0] for item in result["signals"]]
    assert all("放量" not in name for name in signal_names)


def test_analyze_stock_does_not_filter_above_bollinger_upper_anymore() -> None:
    strong_close_df = build_kline(with_shrinking_down_days=False)
    strong_close_df.loc[strong_close_df.index[-1], "close"] = strong_close_df.loc[strong_close_df.index[-2], "close"] * 1.08
    strong_close_df.loc[strong_close_df.index[-1], "high"] = strong_close_df.loc[strong_close_df.index[-1], "close"] * 1.02
    strong_close_df.loc[strong_close_df.index[-1], "open"] = strong_close_df.loc[strong_close_df.index[-1], "close"] * 0.99
    strong_close_df.loc[strong_close_df.index[-1], "low"] = strong_close_df.loc[strong_close_df.index[-1], "close"] * 0.98

    with patch("stock.compute_indicators.detect_golden_cross_type", return_value="first"), patch(
        "stock.compute_indicators.calc_rsi",
        return_value=pd.Series([55.0] * len(strong_close_df)),
    ):
        result = analyze_stock("000001", "样例", strong_close_df, scoring_mode="legacy")

    assert result is not None