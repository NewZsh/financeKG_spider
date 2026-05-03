from stock.backtest import build_portfolio_equity_curve, should_skip_signal_for_cooldown, simulate_trade_from_bars


def test_simulate_trade_hits_stop_loss() -> None:
    bars = [
        {"trade_date": "2026-04-02", "high": 10.2, "low": 8.9, "close": 9.4},
    ]

    result = simulate_trade_from_bars("2026-04-01", 10.0, bars)

    assert result["exit_reason"] == "stop_loss"
    assert result["exit_price"] == 9.0
    assert result["return_pct"] == -10.0


def test_simulate_trade_hits_profit_drawdown() -> None:
    bars = [
        {"trade_date": "2026-04-02", "high": 12.0, "low": 10.8, "close": 11.4},
        {"trade_date": "2026-04-03", "high": 11.6, "low": 10.3, "close": 10.4},
    ]

    result = simulate_trade_from_bars("2026-04-01", 10.0, bars)

    assert result["exit_reason"] == "take_profit_drawdown"
    assert result["exit_date"] == "2026-04-03"
    assert result["return_pct"] == 4.0
    assert result["max_profit_pct"] == 14.0


def test_simulate_trade_ignores_intraday_profit_for_drawdown() -> None:
    bars = [
        {"trade_date": "2026-04-02", "high": 12.0, "low": 9.9, "close": 10.0},
        {"trade_date": "2026-04-03", "high": 10.2, "low": 9.7, "close": 9.8},
    ]

    result = simulate_trade_from_bars("2026-04-01", 10.0, bars)

    assert result["exit_reason"] == "end_of_data"
    assert result["exit_date"] == "2026-04-03"
    assert result["max_profit_pct"] == 0.0


def test_simulate_trade_requires_prior_closing_profit_before_drawdown_exit() -> None:
    bars = [
        {"trade_date": "2026-04-02", "high": 10.1, "low": 9.5, "close": 9.8},
        {"trade_date": "2026-04-03", "high": 10.0, "low": 9.6, "close": 9.7},
    ]

    result = simulate_trade_from_bars("2026-04-01", 10.0, bars)

    assert result["exit_reason"] == "end_of_data"
    assert result["exit_date"] == "2026-04-03"
    assert result["max_profit_pct"] == 0.0


def test_should_skip_signal_for_cooldown_blocks_rebuy_within_30_days() -> None:
    signal = {"code": "000001"}
    cooldown_until_by_code = {"000001": "2026-05-01"}

    assert should_skip_signal_for_cooldown("2026-04-15", signal, cooldown_until_by_code) is True
    assert should_skip_signal_for_cooldown("2026-05-01", signal, cooldown_until_by_code) is True
    assert should_skip_signal_for_cooldown("2026-05-02", signal, cooldown_until_by_code) is False


def test_build_portfolio_equity_curve_tracks_equity() -> None:
    trades = [
        {
            "code": "000001",
            "entry_date": "2026-04-01",
            "exit_date": "2026-04-02",
            "entry_price": 10.0,
            "exit_price": 11.0,
            "score": 80,
        }
    ]
    trading_dates = ["2026-04-01", "2026-04-02"]
    close_lookup = {
        ("000001", "2026-04-01"): 10.0,
        ("000001", "2026-04-02"): 11.0,
    }

    executed_trades, skipped_trades, curve_rows, portfolio_summary = build_portfolio_equity_curve(
        trades,
        trading_dates,
        close_lookup,
        initial_capital=100000.0,
        position_size=10000.0,
        max_positions=2,
    )

    assert len(executed_trades) == 1
    assert not skipped_trades
    assert curve_rows[-1]["equity"] == 101000.0
    assert portfolio_summary["final_equity"] == 101000.0
    assert portfolio_summary["total_return_pct"] == 1.0