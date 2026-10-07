# -*- coding: utf-8 -*-
"""
绛栫暐鍚嶇О锛?涓绘澘寮哄娍鑲♀€滃洜瀛愭墦鍒?- 娲楃洏纭 - 鐩樹腑涓婄牬鏄ㄥ紑鍗充粙鍏モ€濈煭绾跨瓥鐣?
璁捐鐩爣锛?1. 淇濈暀 s1.py 宸查獙璇佺殑鎵ц灞備笌椋庢帶灞傝兘鍔涖€?2. 灏嗙洏鍓嶇獊鐮存壂鎻忔敼閫犳垚鈥滃洜瀛愬揩鐓?-> 杩炵画鎵撳垎 -> Top-N 鍏ユ睜鈥濈殑淇″彿灞傘€?3. 鍦ㄥ畬鏁村洜瀛愮爺绌舵鏋惰惤鍦板墠锛屽厛鐢ㄥ彲瑙ｉ噴鐨勭嚎鎬у姞鏉冪増鏈浛浠ｅ竷灏旇鍒欏爢鍙犮€?
璇存槑锛?- 鏈剼鏈槸 plan.md 涓€滈樁娈?4: 鎵ц灞傛帴绠♀€濈殑绗竴鐗堝疄鐜般€?- 浠嶇劧淇濈暀蹇呰鐨勪氦鏄撴姢鏍忥紝閬垮厤淇″彿灞傚皻鏈绾挎牎楠屽墠杩囧害鏀惧鑲＄エ姹犮€?"""

import numpy as np
import pandas as pd
from jqdata import *

MAX_TRADE_VALUE = 100000
MAX_BUYS_PER_DAY = 5
MAX_WATCH_POOL_SIZE = 500
MIN_BUY_BUDGET = 10000

SIGNAL_SCORE_FLOOR = 60.0
SIGNAL_ENTRY_FLOOR = 55.0
AGE_DECAY_PER_DAY = 1.5
FACTOR_EFFECTIVENESS_REVIEW_DAY = 1
MIN_ACTIVE_FACTORS = 6
COMBINATION_METHOD = 'blended_icir'
FACTOR_MONITOR_HORIZON_DAYS = 5
FACTOR_MONITOR_MIN_SAMPLE_SIZE = 20
FACTOR_MONITOR_MIN_OBSERVATIONS = 12
FACTOR_MONITOR_MAX_HISTORY = 90
FACTOR_MONITOR_CANDIDATE_CAP = 120

FACTOR_STATUS_RULES = {
	'disable_ic_mean': 0.01,
	'disable_icir': 0.00,
	'disable_monotonicity_break': 0.50,
	'warn_ic_mean': 0.02,
	'warn_icir': 0.30,
	'warn_monotonicity_break': 0.30,
	'warning_weight_multiplier': 0.50,
}

FACTOR_LIBRARY = {
	'trend_above_ma5': {
		'family': 'trend',
		'base_weight': 0.09,
		'lower': 0.00,
		'upper': 0.08,
	},
	'ma_alignment_spread': {
		'family': 'trend',
		'base_weight': 0.09,
		'lower': 0.00,
		'upper': 0.12,
	},
	'day_return_intraday': {
		'family': 'momentum',
		'base_weight': 0.08,
		'lower': 0.01,
		'upper': 0.09,
	},
	'day_return_overnight': {
		'family': 'momentum',
		'base_weight': 0.07,
		'lower': 0.01,
		'upper': 0.09,
	},
	'alpha101_002_like': {
		'family': 'momentum',
		'base_weight': 0.08,
		'lower': -0.80,
		'upper': 0.80,
	},
	'vol_zscore_22d': {
		'family': 'volume',
		'base_weight': 0.09,
		'lower': 0.30,
		'upper': 3.00,
	},
	'volume_momentum': {
		'family': 'volume',
		'base_weight': 0.07,
		'lower': 0.00,
		'upper': 2.00,
	},
	'alpha101_price_volume_corr': {
		'family': 'volume',
		'base_weight': 0.08,
		'lower': -0.60,
		'upper': 0.90,
	},
	'reclaim_open_strength': {
		'family': 'reversal',
		'base_weight': 0.07,
		'lower': -0.02,
		'upper': 0.12,
	},
	'alpha101_101_like': {
		'family': 'reversal',
		'base_weight': 0.08,
		'lower': -0.80,
		'upper': 0.80,
	},
	'breakout_distance_20d': {
		'family': 'reversal',
		'base_weight': 0.06,
		'lower': -0.02,
		'upper': 0.12,
	},
	'volatility_contraction': {
		'family': 'volatility',
		'base_weight': 0.07,
		'lower': 0.80,
		'upper': 2.50,
	},
	'alpha101_014_like': {
		'family': 'volatility',
		'base_weight': 0.07,
		'lower': -0.20,
		'upper': 0.20,
	},
}

REGIME_FAMILY_WEIGHTS = {
	'bull': {
		'trend': 1.20,
		'momentum': 1.15,
		'volume': 1.10,
		'reversal': 0.85,
		'volatility': 0.90,
	},
	'bear': {
		'trend': 0.75,
		'momentum': 0.70,
		'volume': 0.85,
		'reversal': 1.20,
		'volatility': 1.20,
	},
	'range': {
		'trend': 0.95,
		'momentum': 0.90,
		'volume': 1.00,
		'reversal': 1.05,
		'volatility': 1.10,
	},
}

FACTOR_RESEARCH_STATUS = {
	'trend_above_ma5': {'ic_mean': 0.051, 'icir': 0.96, 'monotonicity_break_rate': 0.08},
	'ma_alignment_spread': {'ic_mean': 0.047, 'icir': 0.82, 'monotonicity_break_rate': 0.12},
	'day_return_intraday': {'ic_mean': 0.043, 'icir': 0.75, 'monotonicity_break_rate': 0.14},
	'day_return_overnight': {'ic_mean': 0.031, 'icir': 0.52, 'monotonicity_break_rate': 0.19},
	'alpha101_002_like': {'ic_mean': 0.038, 'icir': 0.58, 'monotonicity_break_rate': 0.16},
	'vol_zscore_22d': {'ic_mean': 0.055, 'icir': 0.91, 'monotonicity_break_rate': 0.10},
	'volume_momentum': {'ic_mean': 0.029, 'icir': 0.41, 'monotonicity_break_rate': 0.20},
	'alpha101_price_volume_corr': {'ic_mean': 0.034, 'icir': 0.49, 'monotonicity_break_rate': 0.18},
	'reclaim_open_strength': {'ic_mean': 0.042, 'icir': 0.67, 'monotonicity_break_rate': 0.14},
	'alpha101_101_like': {'ic_mean': 0.033, 'icir': 0.44, 'monotonicity_break_rate': 0.22},
	'breakout_distance_20d': {'ic_mean': 0.027, 'icir': 0.36, 'monotonicity_break_rate': 0.24},
	'volatility_contraction': {'ic_mean': 0.024, 'icir': 0.33, 'monotonicity_break_rate': 0.21},
	'alpha101_014_like': {'ic_mean': 0.018, 'icir': 0.22, 'monotonicity_break_rate': 0.31},
}

FACTOR_COMPUTE_REGISTRY = {}


def order_buy_once(security, total_value, reference_price):
	if reference_price is None or np.isnan(reference_price) or reference_price <= 0:
		return 0

	capped_value = min(float(total_value), MAX_TRADE_VALUE)
	total_amount = int(capped_value / reference_price / 100) * 100
	if total_amount < 100:
		return 0

	order(security, total_amount)
	return total_amount


def order_amount_in_chunks(security, total_amount, reference_price, is_limit=False):
	if total_amount == 0:
		return

	total_amount = int(total_amount)
	if total_amount == 0:
		return

	order_style = LimitOrderStyle(reference_price) if is_limit else None

	if reference_price is None or np.isnan(reference_price) or reference_price <= 0:
		order(security, total_amount, style=order_style)
		return

	max_chunk_amount = int(MAX_TRADE_VALUE / reference_price / 100) * 100
	if max_chunk_amount <= 0:
		order(security, total_amount, style=order_style)
		return

	direction = 1 if total_amount > 0 else -1
	remaining_amount = abs(total_amount)

	while remaining_amount > 0:
		if remaining_amount < 100:
			chunk_amount = remaining_amount
		else:
			chunk_amount = min(remaining_amount, max_chunk_amount)
			if (remaining_amount - chunk_amount) > 0 and (remaining_amount - chunk_amount) < 100:
				chunk_amount = remaining_amount

		order(security, direction * chunk_amount, style=order_style)
		remaining_amount -= chunk_amount


def clip_linear(value, lower, upper):
	if upper <= lower:
		return 0.0
	clipped_value = min(max(float(value), lower), upper)
	return (clipped_value - lower) / (upper - lower)


def safe_divide(numerator, denominator, default=0.0):
	if denominator is None or denominator == 0 or np.isnan(denominator):
		return default
	return float(numerator) / float(denominator)


def ts_rank_pct(series):
	if len(series) == 0:
		return 0.0
	ranks = pd.Series(series, dtype='float64').rank(pct=True)
	return float(ranks.iloc[-1])


def corr_last(series_a, series_b, window):
	if len(series_a) < window or len(series_b) < window:
		return 0.0
	value = pd.Series(series_a[-window:], dtype='float64').corr(pd.Series(series_b[-window:], dtype='float64'))
	if np.isnan(value):
		return 0.0
	return float(value)


def sanitize_factor_value(value):
	if value is None:
		return 0.0
	try:
		value = float(value)
	except (TypeError, ValueError):
		return 0.0
	if np.isnan(value) or np.isinf(value):
		return 0.0
	return value


def register_factor_compute(factor_name):
	def decorator(func):
		FACTOR_COMPUTE_REGISTRY[factor_name] = func
		return func
	return decorator


def build_feature_store(df):
	if df is None or len(df) < 40:
		return None

	close_series = pd.Series(df['close'], dtype='float64')
	open_series = pd.Series(df['open'], dtype='float64')
	high_series = pd.Series(df['high'], dtype='float64')
	low_series = pd.Series(df['low'], dtype='float64')
	volume_series = pd.Series(df['volume'], dtype='float64')
	daily_returns = close_series.pct_change().dropna()
	log_volume = np.log(volume_series + 1.0)
	open_nonzero = open_series.replace(0, np.nan)
	intraday_body = ((close_series - open_series) / open_nonzero).replace([np.inf, -np.inf], 0.0).fillna(0.0)

	return {
		'close': close_series,
		'open': open_series,
		'high': high_series,
		'low': low_series,
		'volume': volume_series,
		'daily_returns': daily_returns,
		'log_volume': log_volume,
		'intraday_body': intraday_body,
		'last_close': float(close_series.iloc[-1]),
		'last_open': float(open_series.iloc[-1]),
		'last_high': float(high_series.iloc[-1]),
		'last_low': float(low_series.iloc[-1]),
		'prev_close': float(close_series.iloc[-2]),
		'prev_volume': float(volume_series.iloc[-2]),
		'last_volume': float(volume_series.iloc[-1]),
		'ma5': float(close_series.iloc[-5:].mean()),
		'ma10': float(close_series.iloc[-10:].mean()),
		'ma20': float(close_series.iloc[-20:].mean()),
		'max20': float(close_series.iloc[-20:].max()),
		'max3_open': float(open_series.iloc[-3:].max()),
		'vol_mean_22': float(volume_series.iloc[-22:-1].mean()) if len(volume_series) >= 22 else 0.0,
		'vol_std_22': float(volume_series.iloc[-22:-1].std()) if len(volume_series) >= 22 else 0.0,
		'recent_vol_5': float(daily_returns.iloc[-5:].std()) if len(daily_returns) >= 5 else 0.0,
		'recent_vol_20': float(daily_returns.iloc[-20:].std()) if len(daily_returns) >= 20 else 0.0,
	}


@register_factor_compute('trend_above_ma5')
def factor_trend_above_ma5(features):
	return safe_divide(features['last_close'], features['ma5'], default=1.0) - 1 if features['ma5'] > 0 else 0.0


@register_factor_compute('ma_alignment_spread')
def factor_ma_alignment_spread(features):
	ma5 = features['ma5']
	ma10 = features['ma10']
	ma20 = features['ma20']
	if ma5 <= 0 or ma10 <= 0 or ma20 <= 0:
		return 0.0
	return (safe_divide(features['last_close'], ma5, default=1.0) - 1) + (safe_divide(ma5, ma10, default=1.0) - 1) + (safe_divide(ma10, ma20, default=1.0) - 1)


@register_factor_compute('day_return_intraday')
def factor_day_return_intraday(features):
	return safe_divide(features['last_close'], features['last_open'], default=1.0) - 1 if features['last_open'] > 0 else 0.0


@register_factor_compute('day_return_overnight')
def factor_day_return_overnight(features):
	return safe_divide(features['last_close'], features['prev_close'], default=1.0) - 1 if features['prev_close'] > 0 else 0.0


@register_factor_compute('alpha101_002_like')
def factor_alpha101_002_like(features):
	volume_delta_2 = features['log_volume'].diff(2).fillna(0.0)
	return -corr_last(volume_delta_2.rank(pct=True).fillna(0.0), features['intraday_body'].rank(pct=True).fillna(0.0), 6)


@register_factor_compute('vol_zscore_22d')
def factor_vol_zscore_22d(features):
	vol_std = features['vol_std_22']
	if vol_std <= 0:
		return 0.0
	return (features['last_volume'] - features['vol_mean_22']) / vol_std


@register_factor_compute('volume_momentum')
def factor_volume_momentum(features):
	return safe_divide(features['last_volume'], features['prev_volume'], default=1.0) - 1 if features['prev_volume'] > 0 else 0.0


@register_factor_compute('alpha101_price_volume_corr')
def factor_alpha101_price_volume_corr(features):
	return corr_last(features['close'], features['log_volume'], 5)


@register_factor_compute('reclaim_open_strength')
def factor_reclaim_open_strength(features):
	return safe_divide(features['last_close'], features['max3_open'], default=1.0) - 1 if features['max3_open'] > 0 else 0.0


@register_factor_compute('alpha101_101_like')
def factor_alpha101_101_like(features):
	return safe_divide(features['last_close'] - features['last_open'], (features['last_high'] - features['last_low']) + 0.001, default=0.0)


@register_factor_compute('breakout_distance_20d')
def factor_breakout_distance_20d(features):
	return safe_divide(features['last_close'], features['max20'], default=1.0) - 1 if features['max20'] > 0 else 0.0


@register_factor_compute('volatility_contraction')
def factor_volatility_contraction(features):
	if features['recent_vol_5'] <= 0:
		return 0.0
	return safe_divide(features['recent_vol_20'], features['recent_vol_5'], default=0.0)


@register_factor_compute('alpha101_014_like')
def factor_alpha101_014_like(features):
	return_delta_3 = features['daily_returns'].diff(3).fillna(0.0) if len(features['daily_returns']) > 0 else pd.Series(dtype='float64')
	if len(return_delta_3) == 0:
		return 0.0
	return -float(return_delta_3.iloc[-1]) * corr_last(features['open'], features['volume'], 10)


def compute_registered_factors(features):
	factor_values = {}
	for factor_name in FACTOR_LIBRARY.keys():
		compute_func = FACTOR_COMPUTE_REGISTRY.get(factor_name)
		factor_values[factor_name] = sanitize_factor_value(compute_func(features) if compute_func else 0.0)
	return factor_values


def detect_market_regime(context):
	index_hist = get_bars('000300.XSHG', count=90, unit='1d', fields=['close'], include_now=False)
	if index_hist is None or len(index_hist) < 60:
		return 'range'

	close_series = pd.Series(index_hist['close'], dtype='float64')
	ma20 = float(close_series.iloc[-20:].mean())
	ma60 = float(close_series.iloc[-60:].mean())
	ma60_prev = float(close_series.iloc[-61:-1].mean()) if len(close_series) >= 61 else ma60
	last_close = float(close_series.iloc[-1])

	if last_close > ma60 and ma60 >= ma60_prev and last_close >= ma20:
		return 'bull'
	if last_close < ma60 and ma60 <= ma60_prev:
		return 'bear'
	return 'range'


def init_factor_monitor_state():
	g.factor_monitor_pending = []
	g.factor_monitor_stats = {}
	g.latest_factor_report = {}
	for factor_name in FACTOR_LIBRARY.keys():
		g.factor_monitor_stats[factor_name] = {
			'ic_series': [],
			'monotonicity_break_series': [],
			'spread_series': [],
			'sample_sizes': [],
			'updated_dates': [],
		}


def compute_rank_ic(values, forward_returns):
	if len(values) < 2 or len(forward_returns) < 2:
		return None
	series_values = pd.Series(values, dtype='float64')
	series_returns = pd.Series(forward_returns, dtype='float64')
	ic_value = series_values.rank(pct=True).corr(series_returns.rank(pct=True))
	if ic_value is None or np.isnan(ic_value):
		return None
	return float(ic_value)


def compute_quantile_spread_and_break(values, forward_returns, quantiles=5):
	frame = pd.DataFrame({'factor': values, 'ret': forward_returns}).dropna()
	if len(frame) < max(quantiles * 3, FACTOR_MONITOR_MIN_SAMPLE_SIZE):
		return None, None

	frame['bucket'] = pd.qcut(frame['factor'].rank(method='first'), quantiles, labels=False, duplicates='drop')
	bucket_mean = frame.groupby('bucket')['ret'].mean().tolist()
	if len(bucket_mean) < 3:
		return None, None

	spread = float(bucket_mean[-1] - bucket_mean[0])
	increasing = all(bucket_mean[idx] <= bucket_mean[idx + 1] for idx in range(len(bucket_mean) - 1))
	decreasing = all(bucket_mean[idx] >= bucket_mean[idx + 1] for idx in range(len(bucket_mean) - 1))
	return spread, 0.0 if (increasing or decreasing) else 1.0


def append_factor_monitor_metric(factor_name, current_date, ic_value, monotonicity_break, quantile_spread, sample_size):
	stats = g.factor_monitor_stats.setdefault(
		factor_name,
		{'ic_series': [], 'monotonicity_break_series': [], 'spread_series': [], 'sample_sizes': [], 'updated_dates': []},
	)
	stats['ic_series'].append(float(ic_value))
	stats['monotonicity_break_series'].append(float(monotonicity_break))
	stats['spread_series'].append(float(quantile_spread))
	stats['sample_sizes'].append(int(sample_size))
	stats['updated_dates'].append(str(current_date))

	for key in ('ic_series', 'monotonicity_break_series', 'spread_series', 'sample_sizes', 'updated_dates'):
		stats[key] = stats[key][-FACTOR_MONITOR_MAX_HISTORY:]


def register_factor_monitor_snapshot(current_date, candidate_rows):
	monitor_rows = []
	for signal_score, stock, snapshot, signal_payload in candidate_rows[:FACTOR_MONITOR_CANDIDATE_CAP]:
		monitor_rows.append({
			'stock': stock,
			'entry_close': snapshot['last_close'],
			'factor_values': snapshot['factor_values'],
			'signal_score': signal_score,
			'regime': signal_payload.get('signal_regime', g.current_regime),
		})

	if len(monitor_rows) < FACTOR_MONITOR_MIN_SAMPLE_SIZE:
		return

	g.factor_monitor_pending.append({
		'day_index': g.trading_day_index,
		'as_of_date': str(current_date),
		'rows': monitor_rows,
	})
	g.factor_monitor_pending = g.factor_monitor_pending[-FACTOR_MONITOR_MAX_HISTORY:]


def evaluate_factor_monitor_snapshots(context):
	if not getattr(g, 'factor_monitor_pending', None):
		return

	current_date = context.current_dt.date()
	remaining_snapshots = []
	evaluated_snapshot_count = 0

	for snapshot in g.factor_monitor_pending:
		if g.trading_day_index - snapshot['day_index'] < FACTOR_MONITOR_HORIZON_DAYS:
			remaining_snapshots.append(snapshot)
			continue

		valid_rows = []
		for row in snapshot['rows']:
			hist = get_bars(row['stock'], count=1, unit='1d', fields=['close'], include_now=False)
			if hist is None or len(hist) < 1:
				continue
			current_close = float(hist['close'][-1])
			if current_close <= 0 or row['entry_close'] <= 0:
				continue
			forward_return = current_close / row['entry_close'] - 1
			valid_rows.append({
				'stock': row['stock'],
				'forward_return': forward_return,
				'factor_values': row['factor_values'],
			})

		if len(valid_rows) < FACTOR_MONITOR_MIN_SAMPLE_SIZE:
			continue

		for factor_name in FACTOR_LIBRARY.keys():
			factor_values = [item['factor_values'].get(factor_name, 0.0) for item in valid_rows]
			forward_returns = [item['forward_return'] for item in valid_rows]
			ic_value = compute_rank_ic(factor_values, forward_returns)
			spread, monotonicity_break = compute_quantile_spread_and_break(factor_values, forward_returns)
			if ic_value is None or spread is None or monotonicity_break is None:
				continue
			append_factor_monitor_metric(
				factor_name=factor_name,
				current_date=current_date,
				ic_value=ic_value,
				monotonicity_break=monotonicity_break,
				quantile_spread=spread,
				sample_size=len(valid_rows),
			)

		evaluated_snapshot_count += 1

	g.factor_monitor_pending = remaining_snapshots
	if evaluated_snapshot_count > 0:
		log.info(f"馃搱 鍥犲瓙鐩戞帶鏇存柊瀹屾垚 -> evaluated_snapshots={evaluated_snapshot_count}")


def blend_factor_metrics(factor_name):
	prior = FACTOR_RESEARCH_STATUS.get(factor_name, {})
	prior_ic_mean = float(prior.get('ic_mean', 0.03))
	prior_icir = float(prior.get('icir', 0.50))
	prior_mono = float(prior.get('monotonicity_break_rate', 0.20))
	prior_spread = float(prior.get('quantile_spread', 0.01))

	stats = getattr(g, 'factor_monitor_stats', {}).get(factor_name, {})
	ic_series = stats.get('ic_series', [])
	mono_series = stats.get('monotonicity_break_series', [])
	spread_series = stats.get('spread_series', [])
	obs_count = len(ic_series)

	if obs_count == 0:
		return {
			'ic_mean': prior_ic_mean,
			'icir': prior_icir,
			'monotonicity_break_rate': prior_mono,
			'quantile_spread': prior_spread,
			'observation_count': 0,
			'online_ic_mean': None,
			'online_icir': None,
		}

	online_ic_mean = float(np.mean(ic_series))
	online_ic_std = float(np.std(ic_series))
	online_icir = online_ic_mean / online_ic_std if online_ic_std > 1e-8 else (1.5 if abs(online_ic_mean) > 1e-8 else 0.0)
	online_mono = float(np.mean(mono_series)) if mono_series else prior_mono
	online_spread = float(np.mean(spread_series)) if spread_series else prior_spread
	blend_ratio = min(0.75, obs_count / 40.0)

	return {
		'ic_mean': prior_ic_mean * (1 - blend_ratio) + online_ic_mean * blend_ratio,
		'icir': prior_icir * (1 - blend_ratio) + online_icir * blend_ratio,
		'monotonicity_break_rate': prior_mono * (1 - blend_ratio) + online_mono * blend_ratio,
		'quantile_spread': prior_spread * (1 - blend_ratio) + online_spread * blend_ratio,
		'observation_count': obs_count,
		'online_ic_mean': online_ic_mean,
		'online_icir': online_icir,
	}


def build_factor_runtime_state():
	runtime_state = {}
	for factor_name, factor_meta in FACTOR_LIBRARY.items():
		metrics = blend_factor_metrics(factor_name)
		ic_mean = float(metrics['ic_mean'])
		icir = float(metrics['icir'])
		mono_break = float(metrics['monotonicity_break_rate'])
		observation_count = int(metrics['observation_count'])

		state = {
			'family': factor_meta['family'],
			'base_weight': factor_meta['base_weight'],
			'ic_mean': ic_mean,
			'icir': icir,
			'monotonicity_break_rate': mono_break,
			'quantile_spread': float(metrics['quantile_spread']),
			'observation_count': observation_count,
			'online_ic_mean': metrics['online_ic_mean'],
			'online_icir': metrics['online_icir'],
			'status': 'active',
			'weight_multiplier': 1.0,
		}

		quality_multiplier = 0.35 + clip_linear(max(icir, 0.0), 0.0, 1.20) * 0.85
		if ic_mean < 0:
			quality_multiplier *= 0.5
		state['quality_multiplier'] = round(quality_multiplier, 4)

		if (
			ic_mean < FACTOR_STATUS_RULES['disable_ic_mean']
			or icir < FACTOR_STATUS_RULES['disable_icir']
			or mono_break > FACTOR_STATUS_RULES['disable_monotonicity_break']
		):
			state['status'] = 'disabled'
			state['weight_multiplier'] = 0.0
		elif (
			ic_mean < FACTOR_STATUS_RULES['warn_ic_mean']
			or icir < FACTOR_STATUS_RULES['warn_icir']
			or mono_break > FACTOR_STATUS_RULES['warn_monotonicity_break']
		):
			state['status'] = 'warning'
			state['weight_multiplier'] = round(FACTOR_STATUS_RULES['warning_weight_multiplier'] * quality_multiplier, 4)
		else:
			state['weight_multiplier'] = round(quality_multiplier, 4)

		runtime_state[factor_name] = state

	return runtime_state


def emit_factor_effectiveness_report(context):
	current_date = context.current_dt.date()
	report_rows = []
	for factor_name, state in g.factor_runtime_state.items():
		report_rows.append({
			'factor_name': factor_name,
			'status': state['status'],
			'ic_mean': round(state['ic_mean'], 4),
			'icir': round(state['icir'], 4),
			'monotonicity_break_rate': round(state['monotonicity_break_rate'], 4),
			'weight_multiplier': round(state['weight_multiplier'], 4),
			'observation_count': state.get('observation_count', 0),
		})

	report_rows.sort(key=lambda item: (item['status'] != 'active', -item['icir'], -item['ic_mean']))
	g.latest_factor_report = {
		'date': str(current_date),
		'regime': g.current_regime,
		'rows': report_rows,
	}

	log.info(f"馃搳 鍥犲瓙鏈堝害鎶ュ憡 -> date={current_date}, regime={g.current_regime}")
	for row in report_rows[:5]:
		log.info(
			f"TOP factor={row['factor_name']} status={row['status']} ic={row['ic_mean']:.4f} "
			f"icir={row['icir']:.4f} mono_break={row['monotonicity_break_rate']:.4f} w={row['weight_multiplier']:.4f}"
		)
	for row in [item for item in report_rows if item['status'] != 'active'][:5]:
		log.warn(
			f"WEAK factor={row['factor_name']} status={row['status']} ic={row['ic_mean']:.4f} "
			f"icir={row['icir']:.4f} mono_break={row['monotonicity_break_rate']:.4f} w={row['weight_multiplier']:.4f}"
		)


def refresh_factor_effectiveness_state(context):
	current_date = context.current_dt.date()
	g.factor_runtime_state = build_factor_runtime_state()

	status_counter = {'active': 0, 'warning': 0, 'disabled': 0}
	for state in g.factor_runtime_state.values():
		status_counter[state['status']] += 1
	log.info(
		f"馃И 鍥犲瓙鐘舵€佸埛鏂?-> active={status_counter['active']}, "
		f"warning={status_counter['warning']}, disabled={status_counter['disabled']}"
	)

	if current_date.day == FACTOR_EFFECTIVENESS_REVIEW_DAY and getattr(g, 'factor_review_month', None) != (current_date.year, current_date.month):
		g.factor_review_month = (current_date.year, current_date.month)
		emit_factor_effectiveness_report(context)


def compute_factor_snapshot(df):
	features = build_feature_store(df)
	if features is None:
		return None
	factor_values = compute_registered_factors(features)
	realized_vol_ratio = safe_divide(features['recent_vol_5'], features['recent_vol_20'], default=0.0)
	breakout_price = max(features['last_open'], features['last_close'])

	return {
		'last_close': features['last_close'],
		'last_open': features['last_open'],
		'last_high': features['last_high'],
		'last_low': features['last_low'],
		'last_volume': features['last_volume'],
		'prev_close': features['prev_close'],
		'prev_volume': features['prev_volume'],
		'ma5': features['ma5'],
		'ma10': features['ma10'],
		'ma20': features['ma20'],
		'realized_vol_ratio': realized_vol_ratio,
		'breakout_price': breakout_price,
		'factor_values': factor_values,
	}


def passes_signal_guard(snapshot, factor_state):
	if snapshot is None:
		return False

	factor_values = snapshot['factor_values']
	trend_ok = snapshot['last_close'] > snapshot['ma5'] > snapshot['ma10']
	price_ok = (
		factor_values['day_return_intraday'] > 0.02
		or factor_values['day_return_overnight'] > 0.02
	)
	volume_ok = factor_values['vol_zscore_22d'] > 0.3 and snapshot['last_volume'] > snapshot['prev_volume']
	stability_ok = snapshot['realized_vol_ratio'] < 2.8 if snapshot['realized_vol_ratio'] > 0 else True
	active_factor_count = sum(1 for state in factor_state.values() if state['status'] != 'disabled')
	return trend_ok and price_ok and volume_ok and stability_ok and active_factor_count >= MIN_ACTIVE_FACTORS


def compute_signal_payload(snapshot, regime, factor_state):
	if snapshot is None:
		return {
			'signal_score': 0.0,
			'family_scores': {},
			'factor_breakdown': {},
			'active_factor_count': 0,
		}

	factor_values = snapshot['factor_values']
	family_scores = {}
	factor_breakdown = {}
	total_score = 0.0
	active_factor_count = 0

	for factor_name, factor_meta in FACTOR_LIBRARY.items():
		state = factor_state.get(factor_name, {})
		if state.get('status') == 'disabled':
			continue

		active_factor_count += 1
		raw_value = factor_values.get(factor_name, 0.0)
		normalized_value = clip_linear(raw_value, factor_meta['lower'], factor_meta['upper'])
		family = factor_meta['family']
		family_multiplier = REGIME_FAMILY_WEIGHTS.get(regime, REGIME_FAMILY_WEIGHTS['range']).get(family, 1.0)
		if COMBINATION_METHOD == 'equal_weight':
			effective_weight = factor_meta['base_weight'] * family_multiplier
		elif COMBINATION_METHOD == 'icir_weighted':
			effective_weight = factor_meta['base_weight'] * max(state.get('icir', 0.0), 0.0) * family_multiplier
		else:
			effective_weight = factor_meta['base_weight'] * state.get('weight_multiplier', 1.0) * family_multiplier
		contribution = normalized_value * effective_weight

		total_score += contribution
		family_scores[family] = family_scores.get(family, 0.0) + contribution
		factor_breakdown[factor_name] = {
			'raw_value': round(raw_value, 6),
			'normalized_score': round(normalized_value, 4),
			'effective_weight': round(effective_weight, 4),
			'contribution': round(contribution, 4),
			'status': state.get('status', 'active'),
		}

	return {
		'signal_score': round(total_score * 100.0, 2),
		'family_scores': {family: round(score * 100.0, 2) for family, score in family_scores.items()},
		'factor_breakdown': factor_breakdown,
		'active_factor_count': active_factor_count,
	}


def build_watch_pool_entry(current_date, snapshot, signal_payload, regime):
	return {
		'breakout_date': current_date,
		'max_raise_vol': snapshot['last_volume'],
		'breakout_price': snapshot['breakout_price'],
		'age': 0,
		'max_drop_vol': 0.0,
		'signal_score': signal_payload['signal_score'],
		'family_scores': signal_payload['family_scores'],
		'factor_breakdown': signal_payload['factor_breakdown'],
		'active_factor_count': signal_payload['active_factor_count'],
		'factor_snapshot': snapshot,
		'signal_regime': regime,
		'signal_version': 'factor_framework_v2',
	}


def refresh_watch_pool_score(info, regime, factor_state):
	snapshot = info.get('factor_snapshot') or {}
	if snapshot:
		signal_payload = compute_signal_payload(snapshot, regime, factor_state)
		base_score = signal_payload['signal_score']
		info['family_scores'] = signal_payload['family_scores']
		info['factor_breakdown'] = signal_payload['factor_breakdown']
		info['active_factor_count'] = signal_payload['active_factor_count']
		info['signal_regime'] = regime
	else:
		base_score = float(info.get('signal_score', 0.0))
	decayed_score = max(0.0, base_score - info.get('age', 0) * AGE_DECAY_PER_DAY)
	info['signal_score'] = round(decayed_score, 2)
	return info['signal_score']


def initialize(context):
	set_benchmark('000300.XSHG')
	set_option('use_real_price', True)
	log.info('銆愬洜瀛愪俊鍙风増鏈?s2銆戝惎鍔?..')

	set_order_cost(
		OrderCost(
			close_tax=0.001,
			open_commission=0.0003,
			close_commission=0.0003,
			min_commission=5,
		),
		type='stock',
	)

	g.buy_cost_dict = {}
	g.max_pnl_dict = {}
	g.blacklist_dict = {}
	g.consecutive_loss_count = 0
	g.freeze_days_left = 0

	g.watch_pool = {}
	g.position_lock_stocks = set()
	g.trailing_stop_last_date = {}
	g.today_buy_count = 0

	g.intraday_unconfirmed_buys = set()
	g.next_open_forced_sell = set()

	g.is_market_safe = False
	g.watch_pool_static = {}
	g.pending_exit_stocks = {}
	g.factor_runtime_state = {}
	g.factor_review_month = None
	g.current_regime = 'range'
	g.trading_day_index = 0
	init_factor_monitor_state()

	run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
	run_daily(market_open, time='open', reference_security='000300.XSHG')
	run_daily(market_intraday, time='every_bar', reference_security='000300.XSHG')
	run_daily(after_market_close, time='15:30', reference_security='000300.XSHG')


def get_main_board_pool(context):
	current_date = context.current_dt.date()
	all_stocks = list(get_all_securities(['stock'], date=current_date).index)
	current_data = get_current_data()

	main_board_stocks = []
	for stock in all_stocks:
		if stock.startswith('300') or stock.startswith('301') or stock.startswith('688') or stock.startswith('8') or stock.startswith('4'):
			continue

		if current_data[stock].is_st or 'ST' in current_data[stock].name or '閫€' in current_data[stock].name:
			continue

		if current_data[stock].paused:
			continue

		info = get_security_info(stock)
		if info is not None and (current_date - info.start_date).days < 180:
			continue

		main_board_stocks.append(stock)

	return main_board_stocks


def before_market_open(context):
	current_date = context.current_dt.date()
	g.today_buy_count = 0
	g.trading_day_index += 1
	evaluate_factor_monitor_snapshots(context)
	g.current_regime = detect_market_regime(context)
	refresh_factor_effectiveness_state(context)

	if g.freeze_days_left > 0:
		g.freeze_days_left -= 1
		log.warn(f"馃毃 绛栫暐澶勪簬鏁翠綋浜忔崯鐔旀柇淇濇姢涓紝鍓╀綑 {g.freeze_days_left} 澶╀笉杩涜浠讳綍涔板叆銆?)
		return

	for stock in list(g.blacklist_dict.keys()):
		if (current_date - g.blacklist_dict[stock]).days > 5:
			del g.blacklist_dict[stock]

	for stock in list(g.pending_exit_stocks.keys()):
		if stock not in context.portfolio.positions:
			g.pending_exit_stocks.pop(stock, None)

	for stock in list(g.position_lock_stocks):
		if stock not in context.portfolio.positions and stock not in g.pending_exit_stocks:
			g.position_lock_stocks.discard(stock)

	for stock in list(g.trailing_stop_last_date.keys()):
		if stock not in context.portfolio.positions and stock not in g.pending_exit_stocks:
			g.trailing_stop_last_date.pop(stock, None)

	current_data_watch = get_current_data()
	for stock in list(g.watch_pool.keys()):
		info = g.watch_pool[stock]

		if current_data_watch[stock].is_st or 'ST' in current_data_watch[stock].name or '閫€' in current_data_watch[stock].name:
			del g.watch_pool[stock]
			log.info(f"馃棏锔?瑙傚療姹犳窐姹?ST/閫€甯傝偂 -> {stock}")
			continue

		info['age'] += 1
		if info['age'] > 15:
			del g.watch_pool[stock]
			continue

		hist_1d = get_bars(stock, count=40, unit='1d', fields=['open', 'high', 'low', 'close', 'volume'], include_now=False)
		if hist_1d is None or len(hist_1d) < 35:
			del g.watch_pool[stock]
			continue

		recent_two = hist_1d[-2:]
		t2_close = recent_two['close'][0]
		t1_open = recent_two['open'][1]
		t1_close = recent_two['close'][1]
		t1_vol = recent_two['volume'][1]

		if (t1_close <= t1_open) and ((t1_close / t2_close) <= 0.901):
			del g.watch_pool[stock]
			continue

		if t1_close >= t1_open:
			if t1_vol > info['max_raise_vol']:
				info['max_raise_vol'] = t1_vol
		else:
			if t1_vol > info['max_raise_vol']:
				del g.watch_pool[stock]
				continue
			info['max_drop_vol'] = max(info['max_drop_vol'], t1_vol)

		refreshed_snapshot = compute_factor_snapshot(hist_1d)
		if refreshed_snapshot is None:
			del g.watch_pool[stock]
			continue

		info['factor_snapshot'] = refreshed_snapshot
		info['breakout_price'] = max(info.get('breakout_price', 0.0), refreshed_snapshot['breakout_price'])
		refresh_watch_pool_score(info, g.current_regime, g.factor_runtime_state)
		if info['signal_score'] < SIGNAL_ENTRY_FLOOR:
			del g.watch_pool[stock]

	main_board_pool = get_main_board_pool(context)

	candidates = []
	for stock in main_board_pool:
		if stock in g.watch_pool or stock in context.portfolio.positions or stock in g.blacklist_dict or stock in g.position_lock_stocks:
			continue

		df = get_bars(stock, count=40, unit='1d', fields=['open', 'high', 'low', 'close', 'volume'], include_now=False)
		snapshot = compute_factor_snapshot(df)
		if snapshot is None or not passes_signal_guard(snapshot, g.factor_runtime_state):
			continue

		signal_payload = compute_signal_payload(snapshot, g.current_regime, g.factor_runtime_state)
		signal_payload['signal_regime'] = g.current_regime
		signal_score = signal_payload['signal_score']
		if signal_score < SIGNAL_SCORE_FLOOR:
			continue

		candidates.append((signal_score, stock, snapshot, signal_payload))

	candidates.sort(key=lambda item: item[0], reverse=True)
	register_factor_monitor_snapshot(current_date, candidates)

	available_slots = MAX_WATCH_POOL_SIZE - len(g.watch_pool)
	if available_slots > 0:
		for signal_score, stock, snapshot, signal_payload in candidates[:available_slots]:
			g.watch_pool[stock] = build_watch_pool_entry(current_date, snapshot, signal_payload, g.current_regime)
			family_scores = signal_payload['family_scores']
			log.info(
				f"馃幆 鍥犲瓙娉ㄥ叆瑙傚療姹?-> {stock} "
				f"(regime={g.current_regime}, score={signal_score:.2f}, "
				f"trend={family_scores.get('trend', 0.0):.2f}, momentum={family_scores.get('momentum', 0.0):.2f}, "
				f"volume={family_scores.get('volume', 0.0):.2f}, reversal={family_scores.get('reversal', 0.0):.2f})"
			)

	index_data = get_bars('000300.XSHG', count=60, unit='1d', fields=['close'], include_now=False)
	if index_data is not None and len(index_data) > 0:
		g.is_market_safe = index_data['close'][-1] >= index_data['close'].mean()
	else:
		g.is_market_safe = False

	g.watch_pool_static = {}
	for stock, info in g.watch_pool.items():
		hist = get_bars(stock, count=20, unit='1d', fields=['open', 'close'], include_now=False)
		if hist is not None and len(hist) >= 19:
			g.watch_pool_static[stock] = {
				't1_open': hist['open'][-1],
				't1_close': hist['close'][-1],
				'sum_4': hist['close'][-4:].sum(),
				'sum_9': hist['close'][-9:].sum(),
				'signal_score': info.get('signal_score', 0.0),
			}


def market_open(context):
	current_data = get_current_data()
	current_date = context.current_dt.date()

	if getattr(g, 'next_open_forced_sell', None):
		for security in list(g.next_open_forced_sell):
			if security in context.portfolio.positions:
				pos = context.portfolio.positions[security]
				amount = pos.closeable_amount
				if amount > 0:
					log.warn(f"馃洃 娆℃棩寮€鐩樺己鍒舵竻浠?鏃犳棩绾跨‘璁? -> {security} , 鍗栧嚭鏁伴噺: {amount}")
					order_amount_in_chunks(security, -amount, None)
					g.trailing_stop_last_date[security] = current_date
			g.next_open_forced_sell.discard(security)

	for security in list(context.portfolio.positions.keys()):
		data = current_data[security]
		pos = context.portfolio.positions[security]

		if data.day_open <= data.low_limit:
			if g.trailing_stop_last_date.get(security) == current_date:
				continue

			amount = pos.closeable_amount
			if amount > 0:
				log.error(f"鈿狅笍 [寮€鐩橀闄╂嫤鎴猐 鑲＄エ {security} 浠婃棩寮€鐩樺嵆璺屽仠锛佹寕璺屽仠浠烽檺浠峰崟鍗栧嚭 {amount} 鑲?..")
				order_amount_in_chunks(security, -amount, data.low_limit, is_limit=True)
				g.trailing_stop_last_date[security] = current_date

			if security not in g.pending_exit_stocks:
				g.pending_exit_stocks[security] = data.low_limit

	if g.pending_exit_stocks:
		log.warning(f"馃攧 [寰呰ˉ鍗栨睜鎵弿] 姝ｅ湪妫€鏌?{len(g.pending_exit_stocks)} 鍙巻鍙查攣姝绘爣鐨?..")
		for security in list(g.pending_exit_stocks.keys()):
			if security not in context.portfolio.positions:
				g.pending_exit_stocks.pop(security, None)
				continue

			data = current_data[security]
			pos = context.portfolio.positions[security]

			if g.trailing_stop_last_date.get(security) == current_date:
				continue

			if data.day_open <= data.low_limit:
				amount = pos.closeable_amount
				if amount > 0:
					log.error(f"馃幆 鑲＄エ {security} 浠婃棩渚濈劧璺屽仠寮€鐩樸€傜户缁寕璺屽仠浠烽檺浠峰崠鍗曪紝鏁伴噺: {amount}")
					order_amount_in_chunks(security, -amount, data.low_limit, is_limit=True)
					g.trailing_stop_last_date[security] = current_date
			else:
				log.info(f"鈩癸笍 鑲＄エ {security} 浠婃棩宸叉墦寮€璺屽仠寮€鐩橈紝灏嗚瀵熻嚦 14:57 鍐冲畾鏄惁鐣欐椿鍙?..")

	for security in list(context.portfolio.positions.keys()):
		if security in g.pending_exit_stocks:
			continue
		if current_data[security].is_st or 'ST' in current_data[security].name or '閫€' in current_data[security].name:
			amount = context.portfolio.positions[security].total_amount
			log.error(f"鈿狅笍 鎸佷粨鑲?{security} 浠婃棩鍙樹负ST/閫€锛屽紑鐩樺己鍒舵竻浠擄紝鏁伴噺: {amount}")
			reference_price = current_data[security].day_open or current_data[security].last_price
			order_amount_in_chunks(security, -amount, reference_price)


def market_intraday(context):
	current_dt = context.current_dt
	current_date = current_dt.date()
	current_time = current_dt.strftime('%H:%M')
	current_data = get_current_data()

	buy_budget = context.portfolio.available_cash - context.portfolio.total_value * 0.30

	if len(g.watch_pool) > 0 and buy_budget > MIN_BUY_BUDGET and g.today_buy_count < MAX_BUYS_PER_DAY:
		if getattr(g, 'is_market_safe', False):
			triggered_stocks = []
			remaining_buy_slots = MAX_BUYS_PER_DAY - g.today_buy_count

			for stock in list(g.watch_pool.keys()):
				if len(triggered_stocks) >= remaining_buy_slots:
					break

				info = g.watch_pool[stock]
				if info['age'] < 1:
					continue

				if stock in g.position_lock_stocks or stock in context.portfolio.positions:
					continue

				static_info = getattr(g, 'watch_pool_static', {}).get(stock)
				if not static_info:
					continue

				t1_open = static_info['t1_open']
				t1_close = static_info['t1_close']
				current_price = current_data[stock].last_price
				open_price = current_data[stock].day_open

				if np.isnan(current_price) or current_price <= 0:
					continue

				ma5 = (static_info['sum_4'] + current_price) / 5
				ma10 = (static_info['sum_9'] + current_price) / 10

				if t1_close >= t1_open:
					continue

				if current_price < open_price or current_price < t1_open:
					continue

				breakout_ref = info.get('breakout_price', 0.0)
				if current_price < breakout_ref:
					continue

				if not (ma5 > ma10):
					continue

				signal_score = max(info.get('signal_score', 0.0), static_info.get('signal_score', 0.0))
				triggered_stocks.append((signal_score, stock, current_price, t1_open))

			if len(triggered_stocks) > 0:
				triggered_stocks.sort(key=lambda item: item[0], reverse=True)
				selected_stocks = triggered_stocks[:remaining_buy_slots]
				cash_per_stock = buy_budget / len(selected_stocks)

				for signal_score, stock, current_price, yesterday_open in selected_stocks:
					ordered_amount = order_buy_once(stock, cash_per_stock, current_price)
					if ordered_amount >= 100:
						est_value = ordered_amount * current_price
						log.info(
							f"馃洅銆愮洏涓拱鍏ヨЕ鍙戙€憑stock} "
							f"score={signal_score:.2f}, 褰撳墠浠?{current_price:.2f} 涓婄牬鏄ㄥ紑 {yesterday_open:.2f}锛?
							f"璁″垝閲戦: {cash_per_stock:.2f}锛屽疄闄呭崟娆′拱鍏?{ordered_amount} 鑲★紝鍙傝€冨競鍊? {est_value:.2f}"
						)
						g.buy_cost_dict[stock] = current_price
						g.max_pnl_dict[stock] = 0.0
						g.position_lock_stocks.add(stock)
						g.intraday_unconfirmed_buys.add(stock)
						g.today_buy_count += 1
						del g.watch_pool[stock]
						g.watch_pool_static.pop(stock, None)

	if current_time < '14:57':
		return

	if g.pending_exit_stocks:
		for security in list(g.pending_exit_stocks.keys()):
			if security not in context.portfolio.positions:
				g.pending_exit_stocks.pop(security, None)
				continue

			if g.trailing_stop_last_date.get(security) == current_date:
				continue

			data = current_data[security]
			if data.last_price > data.low_limit:
				hist_20d = get_bars(security, count=20, unit='1d', fields=['close'], include_now=True)
				if len(hist_20d) < 20:
					continue
				ma20 = hist_20d['close'].mean()

				locked_price = g.pending_exit_stocks[security]
				if data.last_price >= locked_price and data.last_price >= ma20:
					log.info(f"鉁?[閿佹鏀跺] {security} 褰撳墠浠?{data.last_price:.2f} 鎴愬姛鏀跺閿佹浠?{locked_price:.2f} 涓旂獊鐮?MA20({ma20:.2f})锛屽墧闄ゅ嚭琛ュ崠姹狅紝鎭㈠姝ｅ父鎸佷粨鐘舵€併€?)
					g.pending_exit_stocks.pop(security, None)
				else:
					log.error(f"鉂?[閿佹鏈敹澶峕 {security} 14:57 鏈兘鏀跺閿佹浠?{locked_price:.2f} 鎴?MA20({ma20:.2f})锛岀珛鍗虫寕闄愪环鍗曟竻浠?..")
					amount = context.portfolio.positions[security].closeable_amount
					if amount > 0:
						order_amount_in_chunks(security, -amount, data.last_price, is_limit=True)
						g.trailing_stop_last_date[security] = current_date
					g.pending_exit_stocks.pop(security, None)

	current_positions = context.portfolio.positions
	for security in list(current_positions.keys()):
		if security in g.pending_exit_stocks:
			continue

		if g.trailing_stop_last_date.get(security) == current_date:
			continue

		position = current_positions[security]
		if position.closeable_amount == 0:
			continue

		hist_data = get_bars(security, count=20, unit='1d', fields=['close'], include_now=True)
		if len(hist_data) < 20:
			continue

		current_price = current_data[security].last_price if security in current_data else np.nan
		if np.isnan(current_price):
			continue

		ma5 = hist_data['close'][-5:].mean()
		ma10 = hist_data['close'][-10:].mean()
		ma20 = hist_data['close'][-20:].mean()
		my_cost = g.buy_cost_dict.get(security, position.avg_cost)
		total_pnl_ratio = (current_price - my_cost) / my_cost

		if security not in g.max_pnl_dict:
			g.max_pnl_dict[security] = max(0.0, total_pnl_ratio)
		else:
			g.max_pnl_dict[security] = max(g.max_pnl_dict[security], total_pnl_ratio)

		max_pnl = g.max_pnl_dict[security]
		should_sell = False
		sell_amount = 0
		full_exit = False
		reason = ''
		is_loss = False

		if total_pnl_ratio > 0 and max_pnl >= 0.10:
			if max_pnl <= 0.30:
				stop_profit_line = max_pnl * 0.70
			else:
				stop_profit_line = max_pnl * 0.60
			if total_pnl_ratio <= stop_profit_line:
				should_sell = True
				if position.closeable_amount < 500:
					sell_amount = position.closeable_amount
					full_exit = True
					reason = f"14:57鍒╂鼎鍥炴挙瑙﹀彂闃舵姝㈢泩锛屽墿浣欎粨浣嶄笉瓒?00鑲″叏閮ㄥ崠鍑?鏈€楂樻诞鐩?{max_pnl * 100:.2f}%)"
				else:
					sell_amount = int((position.closeable_amount / 2) // 100) * 100
					if sell_amount <= 0:
						sell_amount = position.closeable_amount
						full_exit = True
						reason = f"14:57鍒╂鼎鍥炴挙瑙﹀彂闃舵姝㈢泩锛屼粨浣嶄笉瓒充竴鎵嬫敼涓哄叏閮ㄥ崠鍑?鏈€楂樻诞鐩?{max_pnl * 100:.2f}%)"
					else:
						reason = f"14:57鍒╂鼎鍥炴挙瑙﹀彂闃舵姝㈢泩锛屽厛鍗栧嚭鍗婁粨(鏈€楂樻诞鐩?{max_pnl * 100:.2f}%)"
		elif total_pnl_ratio <= -0.05:
			should_sell = True
			is_loss = True
			sell_amount = position.closeable_amount
			full_exit = True
			reason = '瑙﹀強-5%鍒氭€ф鎹熺嚎'
		elif current_price < ma20:
			should_sell = True
			is_loss = True
			sell_amount = position.closeable_amount
			full_exit = True
			reason = '14:57浠锋牸璺岀牬20鏃ュ潎绾?
		elif ma20 > ma5 or ma20 > ma10:
			should_sell = True
			is_loss = True
			sell_amount = position.closeable_amount
			full_exit = True
			reason = f"MA20({ma20:.2f})>MA5({ma5:.2f})鎴朚A10({ma10:.2f})锛屽潎绾胯秼鍔胯浆寮?

		if should_sell:
			log.warn(f"馃毃銆愮洏涓Е鍙戝崠鍑恒€戣偂绁? {security}, 鍘熷洜: {reason}锛屽綋鍓嶇泩浜? {total_pnl_ratio * 100:.2f}%")
			order_amount_in_chunks(security, -sell_amount, current_price)
			g.trailing_stop_last_date[security] = current_date

			if should_sell and (not full_exit) and (not is_loss):
				g.max_pnl_dict[security] = max(0.0, total_pnl_ratio)

			if full_exit:
				if is_loss:
					g.consecutive_loss_count += 1
					if g.consecutive_loss_count >= 5:
						g.freeze_days_left = 8
						log.error('馃挜馃挜馃挜 绛栫暐鏁翠綋杩炵画浜忔崯5娆★紝瑙﹀彂鐔旀柇淇濇姢锛岀┖浠撻潰澹?涓氦鏄撴棩锛?)
				else:
					g.consecutive_loss_count = 0

				g.blacklist_dict[security] = current_date
				if security in g.buy_cost_dict:
					del g.buy_cost_dict[security]
				if security in g.max_pnl_dict:
					del g.max_pnl_dict[security]
				g.trailing_stop_last_date.pop(security, None)
				g.position_lock_stocks.discard(security)


def after_market_close(context):
	todays_orders = get_orders()
	if not todays_orders:
		return

	for order_id, order_obj in todays_orders.items():
		if order_obj.action == 'close':
			if order_obj.status.name in ['canceled', 'rejected']:
				security = order_obj.security
				if security in context.portfolio.positions:
					if security not in g.pending_exit_stocks:
						current_data = get_current_data()
						g.pending_exit_stocks[security] = current_data[security].low_limit
						log.error(f"馃毃 [鏃ラ姝婚攣鎷︽埅] 鍙戠幇鎸佷粨鑲?{security} 浠婃棩瑙﹀彂甯歌鍑忎粨鍗存湭鑳界鍦猴紒鏀剁洏渚濈劧鏈夊疄浠撱€傝穼鍋滈攣姝讳环: {current_data[security].low_limit}")
						log.error('璇ヨ偂宸茶寮鸿鎷栧叆銆愭瘡鏃ユ纾曡ˉ鍗曟睜銆戯紝鏄庢棩锛堝寘鍚笅鍛ㄤ竴锛夊紑鐩樼涓€鍒嗛挓鑷姩鎸夊競浠风户缁崠鍑猴紒')

	if getattr(g, 'intraday_unconfirmed_buys', None):
		for stock in list(g.intraday_unconfirmed_buys):
			hist = get_bars(stock, count=1, unit='1d', fields=['open', 'close'], include_now=True)
			today_open = hist['open'][-1]
			today_close = hist['close'][-1]
			if today_close < today_open:
				g.next_open_forced_sell.add(stock)
				log.warn(f"馃摑 鐩樺悗鍒ゅ畾锛歿stock} 浠婃棩鏀剁洏 {today_close:.2f} 浣庝簬寮€鐩?{today_open:.2f}锛屾鏃ュ紑鐩樺皢寮哄埗甯備环娓呬粨(鍥犵洏涓拱鍏ユ湭鑾风‘璁?銆?)

		g.intraday_unconfirmed_buys.clear()
