# -*- coding: utf-8 -*-
"""
策略名称：
全主板"多级别联立·日线主操作"缠论策略 (s3)  —— v6.0

======================================================================
★ v6.0 核心思想：纠正 v5 的"级别错配"，真正落实缠论多级别联立
======================================================================
v5 的回测暴露根因：名义"骑周线主升浪"，但入场信号、止损、止盈全在 30 分钟
级别，且入场缺日线级确认 → 卖太早（被 30 分结构止损/顶背驰反复甩下车）、
买太早（周线闸通过但日线已见顶，30 分二买在日线下跌起点触发）。

v6 把级别职责严格对齐缠论正统：
    周线  → 定方向：是否处于上升走势（≥2 中枢上移 / ZG 上行 / 现价>ZG）。
            对所有候选股生效（不是只 trend 模式），即"能不做"。
    日线  → 主操作级：一/二/三类买卖点都在日线判定。买点确认后进入日线买点池；
            持仓持有到日线卖点（日线顶背驰）才离场，不在 30 分级别过早止盈。
    30分  → 只做精细 timing：日线买点已确认的前提下，用 30 分次级别回调结束点
            找更好的入场价。30 分不再是独立信号发生器，只是日线买点的细化入口。

止损/止盈级别（关键修复）：
    - 止损位 = 日线级结构位（一类低点 L1 / 中枢上沿 ZG / 信号笔新低），比 30 分
      宽约 20~50 倍，能骑住主升浪早期的正常回调（解决"卖太早"）。
    - 止盈 = 持有到日线卖点（日线顶背驰），不预设固定百分比；保留一条"日线级宽
      棘轮"(激活+20% / 回撤15%) 仅作流动性/慢 bleed 补丁（解决"买太早"后的
      过早兑现）。30 分顶背驰止盈彻底移除。

大盘层（与个股级别正交，不是后视镜）：
    - 沪深300 vs MA60 判 trend/range/bear，只控制【整体暴露】：
      bear→不开新仓（系统性逃生，edge 来自熊市避跌）；trend→不留现金更进取；
      range→留 30% 现金。它【不】用来给个股分"趋势仓/震荡仓"设定不同止损宽度
      （那是用事后走势反推参数，违反 basic tenets）。个股止损宽度一律日线级，
      由该股的日线 thesis 级别在入场时即锁定。
    - 已移除 v3/v4/v5 的"连续亏损熔断冻结"（用户确认不要）。

保留：永久持仓（中行 PB<0.8 / 长电 PE<20 各 5000）+ 货币ETF 现金管理（511880）。
"""

# ==================== 全局常量 ====================
# 单笔分单抗冲击阈值（元），超过则拆单；真正的单只仓位上限由下方比例常量决定。
MAX_TRADE_VALUE = 100000
MAX_BUYS_PER_DAY = 5           # 每日最大买入只数

# ---- 仓位管理（比例仓位，自适应账户规模）----
PER_STOCK_MAX_RATIO = 0.20     # range 模式单只上限 = 总资产 × 20%（最多~3~4只并存）
TREND_PER_STOCK_MAX_RATIO = 0.30  # trend 模式单只上限 = 总资产 × 30%（防等权退化全仓1只）
CASH_RESERVE_RATIO = 0.30      # range 模式强制保留现金比例
TREND_CASH_RESERVE = 0.0       # trend 模式不留现金（更进取）

# ---- 黑名单冷静期（区分盈亏，避免割肉后手痒接飞刀 / 止盈后保留二次买点）----
BLACKLIST_LOSS_DAYS = 5        # 亏损出场：黑名单 5 天
BLACKLIST_PROFIT_DAYS = 2      # 止盈出场：只黑 2 天
TREND_BLACKLIST_DAYS = 5       # trend 模式平铺 5 天（不分盈亏）

# ---- 盘前三道闸参数 ----
PRESCREEN_LOW_WINDOW = 30      # 日线预筛：回踩到 N 日低点区域
PRESCREEN_RECENT = 5           # 日线预筛：低点须发生在最近 N 日内
LONG_POS_WINDOW = 120          # 长窗口位置过滤观察窗口（约半年）
LONG_POS_MAX = 0.60            # 现价 120 日分位上限：>0.60 视为山腰/高位拒绝（仅 range 模式）
DAILY_BUY_FRESH = 5            # 日线买点信号新鲜度：须在最近 N 个交易日内成立
DAILY_SELL_FRESH = 5           # 日线卖点信号新鲜度
DAILY_BUY_POOL_CAP = 80        # 日线买点池上限（性能）
BARS_DAY_FOR_CHAN = 250        # 日线缠论分析所用 K 线根数
BARS_WEEK_FOR_CHAN = 120       # 周线缠论分析所用 K 线根数（约2.3年，足够识别周线中枢上移）
BARS_30M_FOR_CHAN = 250        # 30分钟缠论分析所用 K 线根数

# ---- 30分钟精细 timing 参数 ----
INTRADAY_FRESH_BARS = 3        # 30分次级别回调笔新鲜度

# ---- 止损参数（v6：日线级结构止损为主，刚性降级灾难兜底）----
HARD_STOP_RATIO = -0.08        # 刚性止损线：纯灾难兜底（隔夜跳空/极端行情）
STRUCT_STOP_BUFFER = 0.01      # 结构止损缓冲：跌破结构位 1% 才触发（防插针）
MAX_STOP_DISTANCE = 0.12       # 入场前风险过滤：日线结构止损位距买入价 >12% 则放弃该信号
                                #   （日线级止损天然比30分宽，故阈值放宽到12%）

# ---- 利润棘轮（日线级宽补丁：仅在接近离场时锁利，不提前兑现）----
TRAIL_ACTIVATE_RATIO = 0.20    # 激活阈值：最高价 ≥ 成本 ×(1+20%)
TRAIL_DRAWDOWN_RATIO = 0.15    # 触发阈值：现价 ≤ 最高水位 ×(1-15%)

# ---- 中枢时效 ----
ZS_MAX_AGE = 60                # 最近中枢终点距今超过 N 根 K 线视为过期（日线/30分钟通用）

# ---- 周线方向闸开关 ----
# 是否纳入一类买点(日线底背驰抄底)；False 则只做二买/三买(买强势股回调)。
BUY_1ST_ENABLED = True

# 30分钟K线收盘时刻（沪深两市）。仅在这些时刻做缠论检测与下单/止损。
BAR_30M_TIMES = {'10:00', '10:30', '11:00', '11:30', '13:30', '14:00', '14:30'}

# ==================== 大盘状态（系统性暴露层，非个股止损宽度） ====================
REGIME_INDEX = '000300.XSHG'      # 大盘状态判断基准（沪深300）
REGIME_TREND_BARS = 70            # 取大盘近 N 日 K 线判断状态
REGIME_MA_LONG = 60               # 中期均线：收盘 > MA60 视为中期多头
REGIME_MA_SHORT = 20              # 短期均线：MA20 上行视为短期强势
REGIME_CONFIRM_DAYS = 3           # trend 须连续 N 日满足条件才切换(防熊市反弹误判)
REGIME_MA_COMPARE_GAP = 5         # MA60 与 N 日前的 MA60 比较以判断上行/下行

# ==================== v4.3 永久持仓（深度价值，不参与止损/止盈/跌停拦截） ====================
PERM_HOLD_STOCKS = [
    {'code': '601988.XSHG', 'name': '中国银行', 'metric': 'pb', 'thresh': 0.8, 'budget': 5000},
    {'code': '600900.XSHG', 'name': '长江电力', 'metric': 'pe', 'thresh': 20, 'budget': 5000},
]
PERM_HOLD_CODES = {c['code'] for c in PERM_HOLD_STOCKS}

# ==================== 货币ETF现金管理（闲置现金赚隔夜利息，加仓前按需变现） ====================
CASH_ETF = '511880.XSHG'        # 华宝添益货币ETF（T+0，年化~2%）
CASH_ETF_RESERVE = 10000        # 保留至少 1 万元现金应对临时需求


# ======================================================================
# 〇、缠论核心算法（自包含，纯 numpy/pandas 实现；与级别无关）
# ======================================================================

def _fractal(high, low):
    """顶/底分型识别。range(1, n-1) 不取最后一根 → 分型靠已收盘右侧K线确认，无未来函数。"""
    n = len(high)
    f = np.zeros(n, dtype=int)
    for i in range(1, n - 1):
        if high[i] > high[i-1] and high[i] > high[i+1] and low[i] > low[i-1] and low[i] > low[i+1]:
            f[i] = 1
        elif low[i] < low[i-1] and low[i] < low[i+1] and high[i] < high[i-1] and high[i] < high[i+1]:
            f[i] = -1
    return f


def _bi_list(high, low):
    """缠论"笔"：相邻异性质分型连线，且两分型间至少间隔4根K线。
    返回 [(start_idx, end_idx, direction, start_price, end_price)]，direction: 1=上升笔, -1=下降笔"""
    f = _fractal(high, low)
    pts = [(i, v) for i, v in enumerate(f) if v != 0]
    bi = []
    last = None
    for idx, v in pts:
        if last is None:
            last = (idx, v)
            continue
        if v == last[1]:
            if v == 1:
                if high[idx] > high[last[0]]:
                    last = (idx, v)
            else:
                if low[idx] < low[last[0]]:
                    last = (idx, v)
            continue
        if abs(idx - last[0]) >= 4:
            sp = low[last[0]] if last[1] == -1 else high[last[0]]
            ep = high[idx] if v == 1 else low[idx]
            direction = 1 if last[1] == -1 else -1
            bi.append((last[0], idx, direction, sp, ep))
            last = (idx, v)
        else:
            last = (idx, v)
    return bi


def _macd_hist(close):
    """标准 MACD 红绿柱: hist = (DIF - DEA) * 2, 12/26/9 参数。"""
    close = pd.Series(close)
    ema_f = close.ewm(span=12, adjust=False).mean()
    ema_s = close.ewm(span=26, adjust=False).mean()
    dif = ema_f - ema_s
    dea = dif.ewm(span=9, adjust=False).mean()
    return ((dif - dea) * 2).values


def _zhongshu_from_bi(bi):
    """简化中枢：连续3笔的价格区间存在重叠 → 中枢 [ZD, ZG]。"""
    zsl = []
    i = 0
    while i + 2 < len(bi):
        rngs = []
        for b in bi[i:i+3]:
            rngs.append((min(b[3], b[4]), max(b[3], b[4])))
        overlap_low = max(r[0] for r in rngs)
        overlap_high = min(r[1] for r in rngs)
        if overlap_high > overlap_low:
            zsl.append((bi[i][0], bi[i+2][1], overlap_high, overlap_low))
            i += 2
        else:
            i += 1
    return zsl


def detect_beichi_signal(high, low, close, recent_bars=3, zs_max_age=ZS_MAX_AGE):
    """检测最近是否出现"背驰"信号（与级别无关）。
    返回: (signal, last_zg, last_zd, sig_ext)
      signal  : 'buy' / 'sell' / None
      sig_ext : 底背驰=信号笔新低(结构止损位)；顶背驰=信号笔新高。"""
    bi = _bi_list(high, low)
    if len(bi) < 3:
        return None, None, None, None
    n = len(close)
    zsl = _zhongshu_from_bi(bi)
    last_zg, last_zd = None, None
    if zsl:
        zs = zsl[-1]
        if (n - 1 - zs[1]) <= zs_max_age:
            last_zg, last_zd = zs[2], zs[3]
    hist = _macd_hist(close)
    cur, prev = bi[-1], bi[-3]
    if cur[1] < n - recent_bars:
        return None, last_zg, last_zd, None
    if cur[2] != prev[2]:
        return None, last_zg, last_zd, None
    area_cur = abs(np.nansum(hist[cur[0]:cur[1]+1]))
    area_prev = abs(np.nansum(hist[prev[0]:prev[1]+1]))
    if area_prev <= 0:
        return None, last_zg, last_zd, None
    if cur[2] == -1 and cur[4] < prev[4] and area_cur < area_prev:
        return 'buy', last_zg, last_zd, cur[4]
    if cur[2] == 1 and cur[4] > prev[4] and area_cur < area_prev:
        return 'sell', last_zg, last_zd, cur[4]
    return None, last_zg, last_zd, None


# ======================================================================
# 一、多级别联立·各级信号函数
# ======================================================================

def is_monthly_trend_up(security, context):
    """月线大势过滤（粗）：月线收盘 ≥ 月线 MA10 → 大级别非长期空头。
    盘前对候选池预先计算，盘中信任（月线日内不变）。"""
    try:
        mdf = get_bars(security, count=12, unit='1M', fields=['close'], include_now=False)
        if mdf is None or len(mdf) < 10:
            return False
        closes = np.asarray(mdf['close'], dtype=float)
        ma10 = closes[-10:].mean()
        return closes[-1] >= ma10
    except Exception as e:
        log.warn("月线数据获取失败 %s: %s" % (security, e))
        return False


def is_weekly_rising(security, context):
    """周线方向闸（主）：周线"中枢上移"判定——个股处于周线级上升走势。
    要求：① 存在≥2个中枢；② 最近中枢ZG > 前一中枢ZG（中枢上移）；
    ③ 现价站在最近中枢ZG上方（上升段进行中）。满足→该股"能交易"。
    对所有候选股生效（v6 不再局限于 trend 模式），即缠论"周线定方向"。"""
    try:
        bars = get_bars(security, count=BARS_WEEK_FOR_CHAN, unit='1w',
                        fields=['high', 'low', 'close'], include_now=False)
    except Exception as e:
        log.warn("周线数据获取失败 %s: %s" % (security, e))
        return False
    if bars is None or len(bars) < 60:
        return False
    high = np.asarray(bars['high'], dtype=float)
    low = np.asarray(bars['low'], dtype=float)
    close = np.asarray(bars['close'], dtype=float)
    bi = _bi_list(high, low)
    if len(bi) < 5:
        return False
    zsl = _zhongshu_from_bi(bi)
    if len(zsl) < 2:
        return False
    zg_last, zg_prev = zsl[-1][2], zsl[-2][2]
    if zg_last <= zg_prev:
        return False
    if close[-1] <= zg_last:
        return False
    return True


def detect_daily_buy(security, high, low, close, recent_bars=DAILY_BUY_FRESH, zs_max_age=ZS_MAX_AGE):
    """日线主操作级·买卖点检测（v6 核心）：日线一/二/三类买点。
    返回 (kind, stop_ref) 或 (None, None)。
      kind ∈ {'1st' 一类(底背驰抄底), '2nd' 二类(回调不破一类低), '3rd' 三类(突破后回踩不进中枢)}
      stop_ref = 日线级结构止损位（持有到它被跌破才离场，比30分宽约20~50倍）：
        1st → 信号笔新低（背驰低点）
        2nd → 一类低点 L1（回调不破的强支撑）
        3rd → 中枢上沿 ZG（回踩不进中枢的边界）
    实现"骑强势股回调"：优先二/三买（顺势），可选纳入一类（抄底）。"""
    bi = _bi_list(high, low)
    if len(bi) < 5:
        return None, None
    n = len(close)
    zsl = _zhongshu_from_bi(bi)
    last_zg, last_zd = None, None
    if zsl:
        zs = zsl[-1]
        if (n - 1 - zs[1]) <= zs_max_age:
            last_zg, last_zd = zs[2], zs[3]

    # ---- 三买（3rd）：中枢后突破→回踩不进中枢 → 顺势追涨买点 ----
    if last_zg is not None:
        later = [b for b in bi if b[0] >= zs[1]]
        up = None
        for b in later:
            if b[2] == 1 and b[4] > last_zg:
                up = b
                break
        if up is not None:
            down = None
            for b in later:
                if b[0] > up[0] and b[2] == -1 and b[4] >= last_zg:
                    down = b
            if down is not None and down[1] >= n - recent_bars:
                return '3rd', last_zg   # 止损位=中枢上沿ZG

    # ---- 二买（2nd）：一类低点L1后回升→回落不破L1 → 安全再入场 ----
    hist = _macd_hist(close)
    cur, prev = bi[-1], bi[-3]
    l1 = None
    if cur[2] == -1 and cur[1] >= n - recent_bars * 3:
        area_cur = abs(np.nansum(hist[cur[0]:cur[1] + 1]))
        area_prev = abs(np.nansum(hist[prev[0]:prev[1] + 1]))
        if area_prev > 0 and cur[4] < prev[4] and area_cur < area_prev:
            l1 = cur[4]   # 一类买点(底背驰)低点
    if l1 is not None:
        after = [b for b in bi if b[0] > cur[0]]
        if len(after) >= 2 and after[0][2] == 1:
            down2 = after[1]
            if down2[2] == -1 and down2[4] > l1 and down2[1] >= n - recent_bars:
                return '2nd', l1   # 止损位=一类低点L1

    # ---- 一类买点（1st，底背驰抄底）：中枢下方 + 信号笔新低 ----
    if BUY_1ST_ENABLED:
        sig, zg, zd, sig_low = detect_beichi_signal(
            high, low, close, recent_bars=recent_bars, zs_max_age=zs_max_age)
        if sig == 'buy':
            if zg is not None and close[-1] <= zg:   # fail-closed：必须有效中枢内/下方
                return '1st', sig_low
    return None, None


def detect_daily_sell(high, low, close, recent_bars=DAILY_SELL_FRESH, zs_max_age=ZS_MAX_AGE):
    """日线主操作级·卖点（v6 主离场信号）：日线顶背驰（持有至日线卖点）。
    返回 (signal_bool, sell_high)。卖出端 fail-open：无中枢或现价>ZG 都放行。"""
    sig, zg, zd, sig_high = detect_beichi_signal(
        high, low, close, recent_bars=recent_bars, zs_max_age=zs_max_age)
    if sig == 'sell':
        if zg is None or close[-1] > zg:
            return True, sig_high
    return False, None


def detect_30m_timing(high, low, close, daily_stop_ref, recent_bars=INTRADAY_FRESH_BARS, zs_max_age=ZS_MAX_AGE):
    """30分精细 timing（v6 降级为入口细化）：仅在日线买点已确认后调用。
    触发条件（满足任一）：
      A) 最近一根30分下降笔(次级别回踩)刚完成，且低点未破日线结构位
         → 次级别回调结束，可上车（拿到比日线信号日更好的入场价）；
      B) 30分底背驰(次级别动能衰竭)且现价未破日线结构位。
    日线结构位被破 → 返回 False（由调用方将该日线买点判失效移出池）。"""
    bi = _bi_list(high, low)
    if len(bi) < 3:
        return False
    n = len(close)
    last = bi[-1]
    if last[2] == -1 and last[1] >= n - recent_bars:
        if last[4] >= daily_stop_ref * (1.0 - STRUCT_STOP_BUFFER):
            return True
    sig, zg, zd, _ = detect_beichi_signal(
        high, low, close, recent_bars=recent_bars, zs_max_age=zs_max_age)
    if sig == 'buy' and close[-1] > daily_stop_ref * (1.0 - STRUCT_STOP_BUFFER):
        return True
    return False


# ======================================================================
# 二、下单工具函数
# ======================================================================

def order_buy_once(security, target_value, reference_price):
    """单次建仓买入。target_value 已由调用方按【比例仓位 + 实时剩余预算】算好。"""
    if reference_price is None or np.isnan(reference_price) or reference_price <= 0:
        return 0
    total_amount = int(float(target_value) / reference_price / 100) * 100
    if total_amount < 100:
        return 0
    order_amount_in_chunks(security, total_amount, reference_price)
    return total_amount


def order_amount_in_chunks(security, total_amount, reference_price, is_limit=False):
    """分单下单：每单金额不超过 MAX_TRADE_VALUE，避免大单冲击；支持限价单。"""
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


# ======================================================================
# 三、框架初始化
# ======================================================================

def initialize(context):
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    log.info('【缠论多级别联立·日线主操作版 s3 v6.0】启动...')

    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003,
                             close_commission=0.0003, min_commission=5), type='stock')

    g.buy_cost_dict = {}            # 个股真实买入成本 {stock: price}
    g.stop_ref_dict = {}            # 【日线级结构止损位】{stock: 日线结构位}（v6 核心）
    g.high_watermark_dict = {}      # 利润棘轮水位 {stock: price}
    g.blacklist_dict = {}           # 黑名单 {stock: (加入日期, 冷静天数)}
    g.position_lock_stocks = set()  # 持仓周期锁：清仓前不允许再次买入
    g.trailing_stop_last_date = {}  # 个股当日卖出去重 {stock: date}
    g.today_buy_count = 0           # 当日已买入只数
    g.active_mode = 'range'         # 当日市场状态('trend'/'range'/'bear')
    g.trend_confirm_count = 0      # trend 连续确认计数(防熊市反弹误判)
    g.perm_hold_done = set()       # 已完成永久建仓的股票
    g.daily_buy_pool = {}           # 【日线买点池】{stock: {'kind':.., 'stop_ref':..}}（盘前构建）
    g.daily_sell_signals = set()   # 当日日线卖点信号（盘前对持仓股计算）

    g.pending_exit_stocks = {}      # 跌停死磕补卖池 {stock: 锁死价}

    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    run_daily(market_open, time='open', reference_security='000300.XSHG')
    run_daily(market_intraday, time='every_bar', reference_security='000300.XSHG')
    run_daily(after_market_close, time='15:30', reference_security='000300.XSHG')


# ======================================================================
# 四、股票池过滤：全主板
# ======================================================================

def get_main_board_pool(context):
    """动态筛出当天：纯沪深主板 + 非ST + 非停牌 + 上市满180天 的正常股。"""
    current_date = context.current_dt.date()
    all_stocks = list(get_all_securities(['stock'], date=current_date).index)
    current_data = get_current_data()
    main_board_stocks = []
    for stock in all_stocks:
        if stock == CASH_ETF:
            continue
        if stock.startswith('300') or stock.startswith('301') or \
           stock.startswith('688') or stock.startswith('8') or stock.startswith('4'):
            continue
        if current_data[stock].is_st or 'ST' in current_data[stock].name or '退' in current_data[stock].name:
            continue
        if current_data[stock].paused:
            continue
        info = get_security_info(stock)
        if info is not None and (current_date - info.start_date).days < 180:
            continue
        main_board_stocks.append(stock)
    return main_board_stocks


# ======================================================================
# 五、大盘状态分类器（系统性暴露层：只控制整体仓位，不决定个股止损宽度）
# ======================================================================

def classify_market_regime(context):
    """trend / range / bear：
      trend：价格>MA60 且 MA60 上行，连续 REGIME_CONFIRM_DAYS 日确认（慢进）
      bear ：价格<MA60 且 MA60 下行 → 不开新仓（系统性逃生）
      range：其余
    进出非对称：进入用 MA60(慢、抗反弹插针)，维持用 MA20(快、防见顶滞后)。"""
    try:
        bars = get_bars(REGIME_INDEX, count=REGIME_TREND_BARS, unit='1d',
                        fields=['close'], include_now=False)
        if bars is None or len(bars) < REGIME_MA_LONG + REGIME_MA_COMPARE_GAP + 2:
            return 'range'
        closes = np.asarray(bars['close'], dtype=float)
        ma60_now = closes[-REGIME_MA_LONG:].mean()
        ma60_prev = closes[-(REGIME_MA_LONG + REGIME_MA_COMPARE_GAP):-REGIME_MA_COMPARE_GAP].mean()
        ma20_now = closes[-REGIME_MA_SHORT:].mean()
        ma20_prev = closes[-REGIME_MA_SHORT - 1:-1].mean()
        price = closes[-1]
        ma60_up = ma60_now > ma60_prev
        ma60_down = ma60_now < ma60_prev
        ma20_up = ma20_now > ma20_prev
        trend_enter = (price > ma60_now) and ma60_up
        trend_stay = (price > ma20_now) and ma20_up
        bear_cond = (price < ma60_now) and ma60_down
        if g.active_mode == 'trend':
            if not trend_stay:
                g.trend_confirm_count = 0
                return 'bear' if bear_cond else 'range'
            return 'trend'
        else:
            if trend_enter:
                g.trend_confirm_count += 1
            else:
                g.trend_confirm_count = 0
            if g.trend_confirm_count >= REGIME_CONFIRM_DAYS:
                return 'trend'
            elif bear_cond:
                return 'bear'
            else:
                return 'range'
    except Exception as e:
        log.warn("大盘状态判断失败，按 range 处理: %s" % e)
        return 'range'


# ======================================================================
# 六、盘前：三道闸构建"日线买点池" + 持仓股日线卖点计算
# ======================================================================

def before_market_open(context):
    current_date = context.current_dt.date()
    g.today_buy_count = 0
    g.daily_buy_pool = {}
    g.daily_sell_signals = set()
    g.candidate_info = {}

    g.active_mode = classify_market_regime(context)
    mode_desc = {'trend': '趋势多头(trend)→更进取', 'range': '弱势震荡(range)→留30%现金',
                 'bear': '熊市下行(bear)→不开新仓'}[g.active_mode]
    log.info("🌐 今日市场状态: %s" % mode_desc)

    # 黑名单清理
    for stock in list(g.blacklist_dict.keys()):
        entry = g.blacklist_dict[stock]
        if isinstance(entry, tuple):
            entry_date, ban_days = entry
        else:
            entry_date, ban_days = entry, BLACKLIST_LOSS_DAYS
        if (current_date - entry_date).days > ban_days:
            del g.blacklist_dict[stock]

    # 死磕池 / 锁仓 / 去重表 清洗
    for stock in list(g.pending_exit_stocks.keys()):
        if stock not in context.portfolio.positions:
            g.pending_exit_stocks.pop(stock, None)
    for stock in list(g.position_lock_stocks):
        if stock not in context.portfolio.positions and stock not in g.pending_exit_stocks:
            g.position_lock_stocks.discard(stock)
    for stock in list(g.trailing_stop_last_date.keys()):
        if stock not in context.portfolio.positions and stock not in g.pending_exit_stocks:
            g.trailing_stop_last_date.pop(stock, None)

    # bear(熊市下行)状态不开新仓（持仓日线卖点/止损照常由 _check_stop_loss 处理）
    if g.active_mode == 'bear':
        log.info("🐻 熊市状态，今日不开新仓（持仓风控照常）")
        _compute_daily_sell_for_held(context)
        return

    # 构建日线买点池：① 廉价日线预筛 → ② 月线多头 → ③ 周线上升(方向闸, 全模式) → ④ 日线买点(重)
    is_trend = (g.active_mode == 'trend')
    main_board_pool = get_main_board_pool(context)
    checked_heavy = 0
    for stock in sorted(main_board_pool):
        if len(g.daily_buy_pool) >= DAILY_BUY_POOL_CAP:
            break
        if stock in context.portfolio.positions or stock in g.blacklist_dict \
           or stock in g.position_lock_stocks or stock in PERM_HOLD_CODES:
            continue

        # ① 廉价日线预筛（回踩 + 120日位置）
        pre = get_bars(stock, count=LONG_POS_WINDOW, unit='1d',
                       fields=['high', 'low', 'close'], include_now=False)
        if pre is None or len(pre) < PRESCREEN_LOW_WINDOW:
            continue
        lows_all = np.asarray(pre['low'], dtype=float)
        highs_all = np.asarray(pre['high'], dtype=float)
        closes_all = np.asarray(pre['close'], dtype=float)
        lows30 = lows_all[-PRESCREEN_LOW_WINDOW:]
        if np.nanmin(lows30[-PRESCREEN_RECENT:]) > np.nanmin(lows30):
            continue
        # 120日位置过滤（仅 range 模式；trend 不拒创新高赢家）
        if not is_trend:
            lo_120 = np.nanmin(lows_all)
            hi_120 = np.nanmax(highs_all)
            rng = hi_120 - lo_120
            if rng > 1e-6:
                pos_pct = (closes_all[-1] - lo_120) / rng
                if pos_pct > LONG_POS_MAX:
                    continue

        # ② 月线多头过滤
        if not is_monthly_trend_up(stock, context):
            continue

        # ③ 周线上升（方向闸，全模式生效）
        if not is_weekly_rising(stock, context):
            continue

        # ④ 日线买点（重计算，压到极小池）
        bars = get_bars(stock, count=BARS_DAY_FOR_CHAN, unit='1d',
                        fields=['high', 'low', 'close'], include_now=False)
        if bars is None or len(bars) < 100:
            continue
        high = np.asarray(bars['high'], dtype=float)
        low = np.asarray(bars['low'], dtype=float)
        close = np.asarray(bars['close'], dtype=float)
        checked_heavy += 1
        kind, stop_ref = detect_daily_buy(stock, high, low, close)
        if kind is None:
            continue
        # 入场前风险过滤（用昨日收盘近似）：日线结构止损距现价 >12% 放弃
        approx_entry = close[-1]
        if stop_ref is None:
            continue
        struct_trigger = stop_ref * (1.0 - STRUCT_STOP_BUFFER)
        if struct_trigger < approx_entry * (1.0 - MAX_STOP_DISTANCE):
            continue
        g.daily_buy_pool[stock] = {'kind': kind, 'stop_ref': stop_ref}

    log.info("📋 今日日线买点池 %d 只 [%s模式, 上限%d]（日线重计算 %d 次）"
             % (len(g.daily_buy_pool), g.active_mode, DAILY_BUY_POOL_CAP, checked_heavy))

    # 对当前持仓股计算日线卖点信号（盘前用昨日日线数据，全天稳定）
    _compute_daily_sell_for_held(context)


def _compute_daily_sell_for_held(context):
    """对当前持仓（非永久/非ETF/非死磕）计算日线顶背驰卖点，存入 g.daily_sell_signals。"""
    for security in list(context.portfolio.positions.keys()):
        if security == CASH_ETF or security in g.perm_hold_done \
           or security in g.pending_exit_stocks:
            continue
        try:
            bars = get_bars(security, count=BARS_DAY_FOR_CHAN, unit='1d',
                            fields=['high', 'low', 'close'], include_now=False)
        except Exception:
            continue
        if bars is None or len(bars) < 100:
            continue
        high = np.asarray(bars['high'], dtype=float)
        low = np.asarray(bars['low'], dtype=float)
        close = np.asarray(bars['close'], dtype=float)
        sig, _ = detect_daily_sell(high, low, close)
        if sig:
            g.daily_sell_signals.add(security)


# ======================================================================
# 七、开盘：跌停拦截 + 死磕池处理 + ST强制清仓
# ======================================================================

def check_perm_hold(context, current_data):
    """v4.3 永久持仓检查：深度价值条件满足用固定预算买入并永久持有。"""
    todo = [c for c in PERM_HOLD_STOCKS if c['code'] not in g.perm_hold_done]
    if not todo:
        return
    codes = [c['code'] for c in todo]
    try:
        df = get_fundamentals(query(valuation.code, valuation.pe_ratio, valuation.pb_ratio)
                              .filter(valuation.code.in_(codes)))
    except Exception as e:
        log.warn("永久持仓估值查询失败: %s" % e)
        return
    if df is None or len(df) == 0:
        return
    val_map = {row['code']: (row['pe_ratio'], row['pb_ratio']) for _, row in df.iterrows()}
    for cfg in todo:
        code = cfg['code']
        if code in context.portfolio.positions or code in g.perm_hold_done:
            g.perm_hold_done.add(code)
            continue
        if code not in val_map:
            continue
        pe, pb = val_map[code]
        pe = float(pe) if pe else 0.0
        pb = float(pb) if pb else 0.0
        if cfg['metric'] == 'pb':
            hit = pb > 0 and pb < cfg['thresh']
            cur_val = pb
        else:
            hit = pe > 0 and pe < cfg['thresh']
            cur_val = pe
        if not hit:
            continue
        price = current_data[code].last_price if code in current_data else 0.0
        if price <= 0 or (isinstance(price, float) and np.isnan(price)):
            continue
        if price >= current_data[code].high_limit:
            continue
        ordered = order_buy_once(code, cfg['budget'], price)
        if ordered >= 100:
            g.perm_hold_done.add(code)
            g.buy_cost_dict[code] = price
            log.info("💎【永久持仓·深度价值买入】%s %s=%.2f < %.2f，买入 %d 股(≈%.0f元)，永久持有"
                     % (cfg['name'], cfg['metric'].upper(), cur_val, cfg['thresh'],
                        ordered, ordered * price))


def market_open(context):
    current_data = get_current_data()
    current_date = context.current_dt.date()
    check_perm_hold(context, current_data)

    # 开盘即跌停的持仓：立即挂跌停价限价单卖出（永久持仓/ETF跳过）
    for security in list(context.portfolio.positions.keys()):
        if security == CASH_ETF or security in g.perm_hold_done:
            continue
        data = current_data[security]
        pos = context.portfolio.positions[security]
        if data.day_open <= data.low_limit:
            if g.trailing_stop_last_date.get(security) == current_date:
                continue
            amount = pos.closeable_amount
            if amount > 0:
                log.error("⚠️ [开盘风险拦截] %s 开盘即跌停！挂跌停价限价卖出 %d 股" % (security, amount))
                order_amount_in_chunks(security, -amount, data.low_limit, is_limit=True)
                g.trailing_stop_last_date[security] = current_date
            if security not in g.pending_exit_stocks:
                g.pending_exit_stocks[security] = data.low_limit

    # 历史死磕池优先处理
    if g.pending_exit_stocks:
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
                    log.error("🎯 %s 依然跌停开盘，继续挂跌停价死磕，数量 %d" % (security, amount))
                    order_amount_in_chunks(security, -amount, data.low_limit, is_limit=True)
                    g.trailing_stop_last_date[security] = current_date
            else:
                log.info("ℹ️ %s 今日已打开跌停，观察至 14:57 决定去留..." % security)

    # 持仓突变 ST/退 → 开盘强制清仓
    for security in list(context.portfolio.positions.keys()):
        if security == CASH_ETF or security in g.pending_exit_stocks:
            continue
        if current_data[security].is_st or 'ST' in current_data[security].name \
           or '退' in current_data[security].name:
            amount = context.portfolio.positions[security].total_amount
            log.error("⚠️ 持仓 %s 变为ST/退，开盘强制清仓 %d 股" % (security, amount))
            reference_price = current_data[security].day_open or current_data[security].last_price
            order_amount_in_chunks(security, -amount, reference_price)


# ======================================================================
# 八、盘中每分钟：30分钟收盘做风控/买入；14:57 终检+死磕
# ======================================================================

def market_intraday(context):
    current_dt = context.current_dt
    current_date = current_dt.date()
    current_time = current_dt.strftime('%H:%M')
    current_data = get_current_data()

    if current_time in BAR_30M_TIMES:
        _check_stop_loss(context, current_data, current_date)      # 先风控（含日线卖点）
        _process_30m_signals(context, current_data, current_date)  # 再买点(timing)

    if current_time < '14:57':
        return

    # 死磕池 14:57 生死劫
    if g.pending_exit_stocks:
        for security in list(g.pending_exit_stocks.keys()):
            if security not in context.portfolio.positions:
                g.pending_exit_stocks.pop(security, None)
                continue
            if g.trailing_stop_last_date.get(security) == current_date:
                continue
            data = current_data[security]
            if data.last_price > data.low_limit:
                hist_20d = get_bars(security, count=20, unit='1d', fields=['close'], include_now=False)
                if len(hist_20d) < 20:
                    continue
                ma20 = hist_20d['close'].mean()
                locked_price = g.pending_exit_stocks[security]
                if data.last_price >= locked_price and data.last_price >= ma20:
                    log.info("✅ [锁死收复] %s 现价 %.2f 收复锁死价 %.2f 且破 MA20(%.2f)，恢复持仓"
                             % (security, data.last_price, locked_price, ma20))
                    g.pending_exit_stocks.pop(security, None)
                else:
                    log.error("❌ [锁死未收复] %s 14:57 未收复，限价清仓" % security)
                    amount = context.portfolio.positions[security].closeable_amount
                    if amount > 0:
                        order_amount_in_chunks(security, -amount, data.last_price, is_limit=True)
                        g.trailing_stop_last_date[security] = current_date
                    g.pending_exit_stocks.pop(security, None)

    _check_stop_loss(context, current_data, current_date)


# ----------------------------------------------------------------------
# 八·A：止损检查（v6：日线级结构止损为主，日线卖点为首要离场，宽棘轮补丁）
# ----------------------------------------------------------------------

def _ma_stop_triggered(security, current_price, total_pnl_ratio):
    """无日线结构止损位时的回退均线止损（MA20 跌破 / 均线转弱 / -5%硬）。"""
    try:
        dbars = get_bars(security, count=30, unit='1d', fields=['close'], include_now=False)
    except Exception:
        dbars = None
    if dbars is not None and len(dbars) >= 20:
        dc = np.asarray(dbars['close'], dtype=float)
        ma5 = dc[-5:].mean()
        ma10 = dc[-10:].mean()
        ma20 = dc[-20:].mean()
        if total_pnl_ratio <= -0.05:
            return True, "回退-.5%% 刚性止损"
        if current_price < ma20:
            return True, "回退-跌破MA20(%.2f)" % ma20
        if ma20 > ma5 or ma20 > ma10:
            return True, "回退-均线转弱(MA20>MA5/MA10)"
        return False, ""
    if total_pnl_ratio <= -0.05:
        return True, "回退-.5%% 刚性止损(数据不足)"
    return False, ""


def _check_stop_loss(context, current_data, current_date):
    current_positions = context.portfolio.positions
    for security in list(current_positions.keys()):
        if security == CASH_ETF or security in g.perm_hold_done \
           or security in g.pending_exit_stocks:
            continue
        if g.trailing_stop_last_date.get(security) == current_date:
            continue
        position = current_positions[security]
        if position.closeable_amount == 0:
            continue
        current_price = current_data[security].last_price if security in current_data else np.nan
        if np.isnan(current_price) or current_price <= 0:
            continue
        if current_price <= current_data[security].low_limit:
            continue

        my_cost = g.buy_cost_dict.get(security, position.avg_cost)
        total_pnl_ratio = (current_price - my_cost) / my_cost

        hwm = g.high_watermark_dict.get(security, my_cost)
        if current_price > hwm:
            hwm = current_price
            g.high_watermark_dict[security] = hwm

        should_sell = False
        reason = ""

        stop_ref = g.stop_ref_dict.get(security)
        if stop_ref is not None:
            # a) 日线级结构止损：跌破日线买点结构位=走势证伪（比30分宽20~50倍，能骑主升浪）
            if current_price < stop_ref * (1.0 - STRUCT_STOP_BUFFER):
                should_sell = True
                reason = "跌破日线结构位 %.2f（日线级结构止损·证伪）" % stop_ref
            # b) 刚性止损（灾难兜底）
            elif total_pnl_ratio <= HARD_STOP_RATIO:
                should_sell = True
                reason = "触及 %.0f%% 刚性止损线（灾难兜底）" % (HARD_STOP_RATIO * 100)
            # c) 日线级宽棘轮（仅在接近离场时锁利，不提前兑现）
            elif hwm >= my_cost * (1.0 + TRAIL_ACTIVATE_RATIO) \
                    and current_price <= hwm * (1.0 - TRAIL_DRAWDOWN_RATIO):
                should_sell = True
                reason = "日线级利润棘轮：最高 %.2f(+%.1f%%) 回撤%.0f%%" \
                         % (hwm, (hwm - my_cost) / my_cost * 100, TRAIL_DRAWDOWN_RATIO * 100)
            # d) 日线卖点（持有至日线顶背驰才离场，不在30分过早止盈）
            elif security in g.daily_sell_signals:
                should_sell = True
                reason = "日线顶背驰卖点（持有至日线卖点离场）"
        else:
            should_sell, reason = _ma_stop_triggered(security, current_price, total_pnl_ratio)

        if should_sell:
            log.warn("🚨【风控卖出】%s 原因: %s，当前盈亏 %.2f%%"
                     % (security, reason, total_pnl_ratio * 100))
            order_amount_in_chunks(security, -position.closeable_amount, current_price)
            g.trailing_stop_last_date[security] = current_date
            _post_exit_cleanup(security, current_date, is_loss=(total_pnl_ratio < 0))


# ----------------------------------------------------------------------
# 八·B：30分钟收盘时刻的买入处理器（仅做日线买点的30分精细 timing）
# ----------------------------------------------------------------------

def _process_30m_signals(context, current_data, current_date):
    # bear 不开新仓；达到当日上限则不买
    if g.active_mode == 'bear' or g.today_buy_count >= MAX_BUYS_PER_DAY:
        return

    # 仓位管理：trend 不留现金 / range 留 30%
    cash_reserve = TREND_CASH_RESERVE if g.active_mode == 'trend' else CASH_RESERVE_RATIO
    etf_pos = context.portfolio.positions.get(CASH_ETF)
    etf_value = 0.0
    if etf_pos is not None and etf_pos.closeable_amount > 0:
        ep = current_data[CASH_ETF].last_price if CASH_ETF in current_data else 0.0
        etf_value = etf_pos.closeable_amount * ep
    buy_budget = context.portfolio.available_cash + etf_value \
                 - context.portfolio.total_value * cash_reserve
    if buy_budget <= 10000:
        return

    triggered_stocks = []
    for stock in sorted(g.daily_buy_pool.keys()):
        if g.today_buy_count + len(triggered_stocks) >= MAX_BUYS_PER_DAY:
            break
        if stock in g.position_lock_stocks or stock in context.portfolio.positions \
           or stock in g.blacklist_dict or stock in PERM_HOLD_CODES:
            continue
        info = g.daily_buy_pool[stock]
        daily_stop_ref = info['stop_ref']
        kind = info['kind']
        try:
            bars = get_bars(stock, count=BARS_30M_FOR_CHAN, unit='30m',
                            fields=['high', 'low', 'close'], include_now=True)
        except Exception as e:
            log.warn("30分timing数据失败 %s: %s" % (stock, e))
            continue
        if bars is None or len(bars) < 60:
            continue
        high = np.asarray(bars['high'], dtype=float)
        low = np.asarray(bars['low'], dtype=float)
        close = np.asarray(bars['close'], dtype=float)

        # 日线结构已破（现价跌破日线止损位）→ 该日线买点失效，移出池
        current_price = current_data[stock].last_price
        if np.isnan(current_price) or current_price <= 0:
            continue
        if current_price <= daily_stop_ref * (1.0 - STRUCT_STOP_BUFFER):
            g.daily_buy_pool.pop(stock, None)
            continue

        # 30分精细 timing：日线买点已确认，等次级别回调结束再上车
        if not detect_30m_timing(high, low, close, daily_stop_ref, recent_bars=INTRADAY_FRESH_BARS):
            continue
        if current_price >= current_data[stock].high_limit:
            continue
        # 入场前风险过滤：日线结构止损距现价 ≤ MAX_STOP_DISTANCE
        struct_trigger = daily_stop_ref * (1.0 - STRUCT_STOP_BUFFER)
        if struct_trigger < current_price * (1.0 - MAX_STOP_DISTANCE):
            continue
        triggered_stocks.append((stock, current_price, daily_stop_ref, kind))

    if len(triggered_stocks) > 0:
        _redeem_cash_etf(context)
        is_trend = (g.active_mode == 'trend')
        n = len(triggered_stocks)
        trend_eq = buy_budget / n
        trend_cap = context.portfolio.total_value * TREND_PER_STOCK_MAX_RATIO
        per_stock_cap = min(trend_eq, trend_cap) if is_trend \
                        else (context.portfolio.total_value * PER_STOCK_MAX_RATIO)
        spent = 0.0
        for stock, current_price, daily_stop_ref, kind in triggered_stocks:
            if g.today_buy_count >= MAX_BUYS_PER_DAY:
                break
            remaining = buy_budget - spent
            if remaining <= 10000:
                break
            target_value = min(per_stock_cap, remaining)
            ordered_amount = order_buy_once(stock, target_value, current_price)
            if ordered_amount >= 100:
                spent += ordered_amount * current_price
                g.buy_cost_dict[stock] = current_price
                g.position_lock_stocks.add(stock)
                g.today_buy_count += 1
                g.daily_buy_pool.pop(stock, None)
                # v6 核心：止损位 = 日线级结构位，骑主升浪不被30分噪声洗出
                g.stop_ref_dict[stock] = daily_stop_ref
                g.high_watermark_dict[stock] = current_price
                log.info("🛒【日线%s买·30分timing】%s 周线上升+日线%s买点，30分回踩结束入场，"
                         "现价 %.2f 买入 %d 股(≈%.0f元/日线池%d只)，日线止损位 %.2f（距价%.1f%%）"
                         % (kind, stock, kind, current_price, ordered_amount,
                            ordered_amount * current_price, n,
                            daily_stop_ref, (current_price - daily_stop_ref) / current_price * 100))


def _post_exit_cleanup(security, current_date, is_loss):
    """清仓善后：黑名单(区分盈亏) / 清缓存 / 解锁。已移除连续亏损熔断逻辑。"""
    if is_loss:
        ban_days = TREND_BLACKLIST_DAYS if g.active_mode == 'trend' else BLACKLIST_LOSS_DAYS
    else:
        ban_days = TREND_BLACKLIST_DAYS if g.active_mode == 'trend' else BLACKLIST_PROFIT_DAYS
    g.blacklist_dict[security] = (current_date, ban_days)
    g.buy_cost_dict.pop(security, None)
    g.stop_ref_dict.pop(security, None)
    g.high_watermark_dict.pop(security, None)
    g.position_lock_stocks.discard(security)
    g.daily_sell_signals.discard(security)


# ======================================================================
# 九、货币ETF现金管理
# ======================================================================

def _redeem_cash_etf(context):
    """加仓前变现：把持有的货币ETF(511880)转为可用现金。仅确有加仓意图时调用。"""
    pos = context.portfolio.positions.get(CASH_ETF)
    if pos is None or pos.closeable_amount <= 0:
        return
    try:
        order(CASH_ETF, -pos.closeable_amount)
        log.info("💵 [货币ETF变现] 卖出 511880 %d 份，释放现金用于加仓" % pos.closeable_amount)
    except Exception as e:
        log.warn("货币ETF变现失败: %s" % e)


def _invest_cash_etf(context, current_data):
    """盘后现金管理：闲余现金买入 511880 货币ETF 赚隔夜利息。"""
    try:
        avail = context.portfolio.available_cash
        invest_cash = avail - CASH_ETF_RESERVE
        if invest_cash < 10000:
            return
        price = current_data[CASH_ETF].last_price if CASH_ETF in current_data else 0.0
        if price <= 0:
            return
        amount = int(invest_cash / price / 100) * 100
        if amount < 100:
            return
        order(CASH_ETF, amount)
        log.info("💵 [货币ETF现金管理] 闲余 %.0f 元买入 511880 %d 份(年化~2%%)" % (invest_cash, amount))
    except Exception as e:
        log.warn("货币ETF买入失败: %s" % e)


def after_market_close(context):
    """复盘今日全部卖出单，跌停流动性枯竭导致撤单/拒单且仍有实仓的，入补卖池。"""
    todays_orders = get_orders()
    if todays_orders:
        for order_id, order_obj in todays_orders.items():
            if order_obj.action == 'close':
                if order_obj.status.name in ['canceled', 'rejected']:
                    security = order_obj.security
                    if security in context.portfolio.positions:
                        if security not in g.pending_exit_stocks:
                            current_data = get_current_data()
                            g.pending_exit_stocks[security] = current_data[security].low_limit
                            log.error("🚨 [死锁拦截] %s 今日卖出未成交且仍有实仓，"
                                      "锁死价 %.2f，已入死磕补卖池，明日开盘继续卖出！"
                                      % (security, current_data[security].low_limit))
    _invest_cash_etf(context, get_current_data())
