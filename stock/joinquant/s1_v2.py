# -*- coding: utf-8 -*-
"""
策略名称：
全主板"波浪理论"策略 (s1) —— v2.0 宽松摆动 + 斐波那契 · 比例仓位 + 永久持仓 + 货币ETF

======================================================================
★ v2.0 核心思想：让 s1 真正按波浪理论运行（名实相符）
======================================================================
v1.0(s1.py) 名义上"基于波浪理论"，但代码里没有任何波浪划分算法——它本质是一个
"放量突破→次日阴线洗盘→盘中上破昨开"的动量 N 字策略，只是借了波浪叙事外壳。
v2.0 落地真正的波浪计数（宽松版）：
  · 底层用"N 根K线左右极值"识别摆动顶/底（swing pivot），这是波浪数浪与缠论笔
    共用的"分形摆动点"原语；
  · 在摆动点上套用宽松 Elliott 规则：找"上升推动(浪1) + 回调(浪2)"结构，
    浪2 回调满足斐波那契比例（0.236~0.786）且"浪2 不破浪1起点" → 视为洗盘完成；
  · 突破浪1高点 = 浪3 启动 → 买入；跌破浪2低点 = 波浪证伪 → 结构止损离场。
  · 不追求严格 Elliott 规则（浪级递归/1.618延长），先用"宽松摆动+比例"跑通，
    信号多、噪声大，后续再收紧（融合版方向层后议）。

======================================================================
★ 资金与风控（沿用并改进 s3 系列已验证框架）
======================================================================
  · 比例仓位（自适应账户规模）：单只上限 = 总资产 × PER_STOCK_MAX_RATIO（range 0.20）
    / TREND_PER_STOCK_MAX_RATIO（trend 0.30），始终留 CASH_RESERVE_RATIO(0.30) 现金；
    对任意账户规模，单票占比、分散度、留现金比例恒定（修掉 s1 原"10万硬帽"的非自适应）。
  · 永久持仓（深度价值锁仓）：中国银行 PB<0.8 / 长江电力 PE<20 各 5000 预算买入后
    永久持有，不参与止损/止盈/跌停拦截。
  · 货币ETF现金管理：无交易意图时闲余现金买 511880 华宝添益（T+0，年化~2%）赚隔夜
    利息；有加仓意图时（候选池非空）盘前变现 511880 转可用现金。
  · 结构止损为主：止损位 = 浪2 低点（波浪证伪位），跌破即离场（替代 s1 原 -5% 硬止损
    与均线止损的"方向打架"）；刚性 -8% 仅作灾难兜底；浮盈棘轮锁定利润。
  · 无熔断逻辑（用户确认移除）。大盘三分状态（trend/range/bear）由 MA 判定，bear 空仓。
  · 无未来函数：所有日线取数 include_now=False（盘前/14:57 边界一致）；盘中仅用实时
    盘口 last_price 触发，不拿未发生的收盘价反推成交。

======================================================================
一、策略总纲（波浪落地版）
======================================================================
【波浪买点】= 宽松摆动 + 斐波那契，逐级收敛：
    第1级·廉价预筛（盘前，每日一次）：近5日回踩30日低点 且 现价120日分位≤60%
        （拒绝"山腰高位"接货，并大幅缩减进入波浪重算的个股数量）。
    第2级·波浪检测（盘前，日线250根）：找 L0→H1→L2（浪1+浪2）结构，
        浪2 回撤比例∈[0.236,0.786] 且 浪2 不破浪1起点 → 突破 H1=浪3启动。
        → 通过者进入 g.elliott_setups（{stock:(trigger=浪1高, stop_ref=浪2低)}）。
    第3级·盘中突破触发（每根bar，仅价格比较）：现价 > trigger → 按【比例仓位】买入，
        存入 stop_ref（浪2低点）作为结构止损位。

【波浪卖点（止损/止盈，盘中30分节拍 + 14:57 终检）】：
    a) 结构止损：现价 < 浪2低点×(1-缓冲) → 波浪证伪，离场；
    b) 刚性止损：亏损 ≥ 8%（纯灾难兜底）；
    c) 利润棘轮：浮盈曾达10%后从最高水位回撤8% → 锁利离场。

======================================================================
二、与 s1 保持一致 / 改进的部分
======================================================================
1. 股票池：全主板（过滤 300/301/688/8/4、ST/*ST/退、停牌、上市<180天、511880货币ETF）。
2. 仓位管理：比例仓位（替代 s1 原"10万硬帽/等权退化"），单周期一次、每日≤MAX_BUYS_PER_DAY只。
3. 跌停等特殊情况：开盘跌停挂跌停价死磕 / 补卖池 / 14:57 锁死收复判定 / 15:30 盘后废单审计。
4. 风控善后：黑名单——亏损出场5天/止盈出场2天（无熔断）。
5. 信号层：s1 的"动量 N 字" → v2 的真实波浪计数（名实相符，失效信号机制化）。

======================================================================
三、性能设计
======================================================================
  - before_market_open（盘前，每日一次）：廉价预筛（120根）→ 波浪重算（250根，池满即停）。
  - market_intraday（盘中 every_bar）：每根bar仅做"突破价格比较"（极廉价）；
    重风控在30分收盘节拍 + 14:57；货币ETF变现/投入按需。
"""

from jqdata import *
import numpy as np
import pandas as pd


# ==================== 全局常量 ====================
# MAX_TRADE_VALUE 由"仓位上限"降级为"单笔分单抗冲击阈值"——
#   单笔下单金额超过它就拆成多笔，避免大单冲击；真正的单只仓位上限
#   由下面的 PER_STOCK_MAX_RATIO（按账户总资产比例）决定。
MAX_TRADE_VALUE = 100000       # 单笔分单抗冲击阈值（元），超过则拆单
MAX_BUYS_PER_DAY = 5           # 每日最大买入只数

# ---- 仓位管理（比例仓位，自适应账户规模）----
# 单只个股建仓市值上限 = 账户总资产 × PER_STOCK_MAX_RATIO。
#   0.20 = 单只最多占总资产20% → 配合"留30%现金"，最多约3~4只并存。
PER_STOCK_MAX_RATIO = 0.20
CASH_RESERVE_RATIO = 0.30      # 强制保留现金比例（不动用总资产的30%）

# ---- trend 模式更进取（= V2.0 风格：近满仓进攻）----
TREND_PER_STOCK_MAX_RATIO = 0.30  # trend 模式单票上限(总资产×30%，防等权退化全仓1只)
TREND_CASH_RESERVE = 0.0          # trend 不强留现金（近满仓）

# ---- 黑名单冷静期（区分盈亏）----
BLACKLIST_LOSS_DAYS = 5        # 亏损出场：黑名单 5 天
BLACKLIST_PROFIT_DAYS = 2       # 止盈出场：只黑 2 天

# ---- 盘前廉价预筛（保留：回踩 + 长窗口位置，缩小候选池后再做波浪重算）----
PRESCREEN_LOW_WINDOW = 30      # 日线预筛：回踩到 N 日低点区域
PRESCREEN_RECENT = 5           # 日线预筛：低点须发生在最近 N 日内
LONG_POS_WINDOW = 120          # 长窗口位置过滤观察窗口（约半年）
LONG_POS_MAX = 0.60            # 现价分位上限：> 0.60 视为山腰/高位，拒绝
MAX_CANDIDATES = 40            # range 监控候选池上限（控制每日波浪重算次数）
TREND_MAX_CANDIDATES = 300     # trend 宽入口：候选池上限放大到 300
BARS_DAY_FOR_ELLIOTT = 250     # 日线波浪分析所用K线根数

# ---- 宽松摆动 + 斐波那契 波浪参数（s1_v2 核心）----
# 用"N 根K线左右极值"识别摆动顶/底（宽松版，非严格 Elliott 规则程序化）。
ELLIOTT_PIVOT_LEFT = 3         # 摆动点左侧确认根数
ELLIOTT_PIVOT_RIGHT = 3        # 摆动点右侧确认根数
ELLIOTT_MIN_PIVOTS = 3         # 至少 L0,H1,L2 三个摆动点才构成"浪1+浪2"
ELLIOTT_FRESH = 30             # 浪2低点(L2)须落在最近 N 根K线内（形态新鲜）
# 浪2 回撤占浪1 的比例（宽松斐波那契区间，非严格 0.618）：
WAVE2_MIN = 0.236              # 浪2 至少回撤浪1 的 23.6%（否则不算回调）
WAVE2_MAX = 0.786              # 浪2 至多回撤浪1 的 78.6%（超过则浪级可疑）
ELLIOTT_BREAK_TOL = 0.05       # 现价已突破浪1高点超过 5% → 视为已深入浪3，跳过找更新形态
ELLIOTT_MAX_STOP_DIST = 0.12   # 入场前风险过滤：浪2低点距突破价 >12% 则放弃（单笔风险过大）

# ---- 止损参数（结构止损为主，刚性降级为灾难兜底）----
# 入场前用 ELLIOTT_MAX_STOP_DIST 保证结构止损触发价距买入价 ≤12%，
#   刚性线放到 -8% → 正常破位永远是结构止损先触发。
HARD_STOP_RATIO = -0.08        # 刚性止损线：纯灾难兜底（隔夜跳空/极端行情）
STRUCT_STOP_BUFFER = 0.01      # 结构止损缓冲：跌破浪2低点 1% 才触发（防插针）
# 波浪证伪位 = 浪2 低点（L2）。价格跌破 L2 = 浪级失效 → 离场（替代 -5% 硬止损的"方向打架"）。

# ---- 利润棘轮（浮盈的最后防线）----
TRAIL_ACTIVATE_RATIO = 0.10    # 激活阈值：最高价 ≥ 成本 ×(1+10%)
TRAIL_DRAWDOWN_RATIO = 0.08    # 触发阈值：现价 ≤ 最高水位 ×(1-8%)

# ---- 大盘状态分类器（MA 系，非缠论；仅用于 bear 空仓 + trend/range 进取度切换）----
REGIME_INDEX = '000300.XSHG'      # 大盘状态判断基准（沪深300）
REGIME_TREND_BARS = 70            # 取大盘近 N 日K线判断状态
REGIME_MA_LONG = 60               # 中期均线：收盘 > MA60 视为中期多头
REGIME_MA_SHORT = 20              # 短期均线：MA20 上行视为短期强势
REGIME_CONFIRM_DAYS = 3           # trend 须连续 N 日满足条件才切换(防熊市反弹误判)
REGIME_MA_COMPARE_GAP = 5         # MA60 与 N 日前的 MA60 比较以判断上行/下行

# ---- 永久持仓（深度价值买入并长期持有，不参与止损/止盈/跌停拦截）----
PERM_HOLD_STOCKS = [
    {'code': '601988.XSHG', 'name': '中国银行', 'metric': 'pb', 'thresh': 0.8, 'budget': 5000},
    {'code': '600900.XSHG', 'name': '长江电力', 'metric': 'pe', 'thresh': 20, 'budget': 5000},
]
PERM_HOLD_CODES = {c['code'] for c in PERM_HOLD_STOCKS}

# ---- 货币ETF现金管理（闲置现金赚隔夜利息，加仓前按需变现）----
CASH_ETF = '511880.XSHG'        # 华宝添益货币ETF（T+0，年化~2%）
CASH_ETF_RESERVE = 10000        # 保留至少 1 万元现金应对临时需求

# ---- 盘中重计算节拍（复用原30分收盘时刻作为"风控节拍"）----
BAR_CHECK_TIMES = {'10:00', '10:30', '11:00', '11:30', '13:30', '14:00', '14:30'}


# ======================================================================
# 〇、波浪核心算法（宽松摆动 + 斐波那契比例，纯 numpy 实现）
#     —— 与级别无关：喂日线就是日线级别波浪，喂周线就是周线级别。
# ======================================================================

def find_swing_pivots(high, low, left=ELLIOTT_PIVOT_LEFT, right=ELLIOTT_PIVOT_RIGHT):
    """宽松摆动点识别（波浪数浪的底层原语）。
    第 i 根K线为 left+right 窗口内的最高(低) → 摆动顶(底)。
    返回 [(idx, price, kind)]，kind ∈ {'H' 顶, 'L' 底}。
    同性质连续摆动取更极端者，得到干净交替序列（无未来函数：窗口不含最后 right 根）。"""
    n = len(high)
    raw = []
    for i in range(left, n - right):
        win_h = high[i - left:i + right + 1]
        win_l = low[i - left:i + right + 1]
        is_h = high[i] >= max(win_h)
        is_l = low[i] <= min(win_l)
        if is_h and not is_l:
            raw.append((i, high[i], 'H'))
        elif is_l and not is_h:
            raw.append((i, low[i], 'L'))
    out = []
    for idx, p, k in raw:
        if out and out[-1][2] == k:
            if (k == 'H' and p > out[-1][1]) or (k == 'L' and p < out[-1][1]):
                out[-1] = (idx, p, k)
        else:
            out.append((idx, p, k))
    return out


def detect_elliott_buy(security):
    """宽松波浪买点检测（纯波浪，非严格 Elliott 规则程序化）。
    在日线上找最近一段"上升推动(浪1) + 回调(浪2)"结构，浪2 回调满足
    斐波那契比例 → 视为洗盘完成，突破浪1高点 = 浪3 启动 → 买入信号。

    判定逻辑：
      取交替摆动点序列，从最近往回扫，找 L0(浪1起点)→H1(浪1顶)→L2(浪2底)：
        · 浪1 向上 (H1 > L0)
        · 浪2 回撤比例 r2=(H1-L2)/(H1-L0) ∈ [WAVE2_MIN, WAVE2_MAX]（斐波那契）
        · 浪2 不破浪1起点 (L2 > L0) —— 核心 Elliott 规则，便宜且关键
        · 浪2 低点新鲜（落在最近 ELLIOTT_FRESH 根K线内）
        · 现价未过度深入浪3（距突破价 ≤ ELLIOTT_BREAK_TOL）
        · 结构止损距离可控（浪2低点距突破价 ≤ ELLIOTT_MAX_STOP_DIST）
    返回 (signal, trigger_price, stop_ref_price)：
      signal        : 'buy' / None
      trigger_price : 突破该价 = 浪3 启动（= 浪1 高点 H1）
      stop_ref_price: 波浪证伪位（= 浪2 低点 L2，跌破即浪级失效，结构止损）
    """
    try:
        bars = get_bars(security, count=BARS_DAY_FOR_ELLIOTT, unit='1d',
                        fields=['high', 'low', 'close'], include_now=False)
    except Exception as e:
        log.warn("日线数据获取失败 %s: %s" % (security, e))
        return None, None, None
    if bars is None or len(bars) < 120:
        return None, None, None
    high = np.asarray(bars['high'], dtype=float)
    low = np.asarray(bars['low'], dtype=float)
    close = np.asarray(bars['close'], dtype=float)

    pivots = find_swing_pivots(high, low)
    if len(pivots) < ELLIOTT_MIN_PIVOTS:
        return None, None, None

    n = len(close)
    last_close = close[-1]

    # 从最近往回扫，找第一个合格的"浪1+浪2"形态
    for end in range(len(pivots) - 1, ELLIOTT_MIN_PIVOTS - 2, -1):
        L0_p, H1_p, L2_p = pivots[end - 2], pivots[end - 1], pivots[end]
        if not (L0_p[2] == 'L' and H1_p[2] == 'H' and L2_p[2] == 'L'):
            continue
        L0, H1, L2 = L0_p[1], H1_p[1], L2_p[1]
        wave1 = H1 - L0
        if wave1 <= 0:
            continue
        # 斐波那契回撤比例（宽松）
        r2 = (H1 - L2) / wave1
        if not (WAVE2_MIN <= r2 <= WAVE2_MAX):
            continue
        # 核心 Elliott 规则：浪2 不破浪1起点
        if not (L2 > L0):
            continue
        # 浪2 低点须新鲜（形态未过期）
        if L2_p[0] < n - ELLIOTT_FRESH:
            continue
        trigger = H1
        stop_ref = L2
        # 不追已深入浪3 的形态（找更新鲜的）
        if last_close > trigger * (1.0 + ELLIOTT_BREAK_TOL):
            continue
        # 入场前风险过滤：结构止损位(浪2低点)距突破价不能太远
        if (trigger - stop_ref) / trigger > ELLIOTT_MAX_STOP_DIST:
            continue
        return 'buy', trigger, stop_ref

    return None, None, None


# ======================================================================
# 一、下单工具函数
# ======================================================================

def order_buy_once(security, target_value, reference_price):
    """单次建仓买入：
    target_value 已由调用方按【比例仓位 + 实时剩余预算】算好，本函数
    不再自行封顶 MAX_TRADE_VALUE（那已降级为"分单阈值"）；按手取整后
    交给 order_amount_in_chunks 分单下单以抗大单冲击。返回实际下单股数。"""
    if reference_price is None or np.isnan(reference_price) or reference_price <= 0:
        return 0
    total_amount = int(float(target_value) / reference_price / 100) * 100
    if total_amount < 100:
        return 0
    order_amount_in_chunks(security, total_amount, reference_price)  # 按MAX_TRADE_VALUE分单抗冲击
    return total_amount


def order_amount_in_chunks(security, total_amount, reference_price, is_limit=False):
    """分单下单：每单金额不超过 MAX_TRADE_VALUE，避免大单冲击；
    支持限价单（用于跌停死磕挂单）。"""
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
            # 不足一手的碎股一并打包
            if (remaining_amount - chunk_amount) > 0 and (remaining_amount - chunk_amount) < 100:
                chunk_amount = remaining_amount
        order(security, direction * chunk_amount, style=order_style)
        remaining_amount -= chunk_amount


# ======================================================================
# 二、框架初始化
# ======================================================================

def initialize(context):
    set_benchmark('000300.XSHG')
    set_option('use_real_price', True)
    log.info('【波浪理论 s1 v2.0 · 宽松摆动+斐波那契 · 比例仓位+永久持仓+货币ETF】启动...')

    # 主板标准佣金（最低5元）
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003,
                             close_commission=0.0003, min_commission=5), type='stock')

    # ---- 交易状态 ----
    g.buy_cost_dict = {}            # 个股真实买入成本 {stock: price}
    g.stop_ref_dict = {}            # 【结构止损参考位】{stock: 浪2低点(波浪证伪位)}
    g.high_watermark_dict = {}      # 【利润棘轮】持仓期最高价水位 {stock: price}
    g.blacklist_dict = {}           # 黑名单 {stock: (加入日期, 冷静天数)}，到期自动移出
    g.position_lock_stocks = set()  # 持仓周期锁：清仓前不允许再次买入
    g.trailing_stop_last_date = {}  # 个股当日卖出去重 {stock: date}
    g.today_buy_count = 0           # 当日已买入只数
    g.active_mode = 'range'         # 当日市场状态('trend'/'range'/'bear')，盘前由 classify_market_regime 设定
    g.trend_confirm_count = 0       # trend 连续确认计数(防熊市反弹误判)
    g.perm_hold_done = set()        # 已完成永久建仓的股票(建仓后不再止损/止盈/重复买入)

    # ---- 波浪买点候选池（盘前构建）----
    g.candidate_pool = set()        # {stock, ...}
    g.elliott_setups = {}           # {stock: (trigger=浪1高点, stop_ref=浪2低点)}

    # ---- 跌停死磕机制 ----
    g.pending_exit_stocks = {}      # {stock: 锁死价} 因跌停未能离场的补卖池

    # 四大定时任务
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    run_daily(market_open, time='open', reference_security='000300.XSHG')
    run_daily(market_intraday, time='every_bar', reference_security='000300.XSHG')
    run_daily(after_market_close, time='15:30', reference_security='000300.XSHG')


# ======================================================================
# 三、股票池过滤：全主板（非ST、非创业、非科创、非北交所、非货币ETF）
# ======================================================================

def get_main_board_pool(context):
    """动态筛出当天：纯沪深主板 + 非ST + 非停牌 + 上市满180天 的正常股。
    排除货币ETF(511880)，避免被当成股票候选。"""
    current_date = context.current_dt.date()
    all_stocks = list(get_all_securities(['stock'], date=current_date).index)
    current_data = get_current_data()

    main_board_stocks = []
    for stock in all_stocks:
        if stock == CASH_ETF:
            continue
        # 过滤创业板(300/301)、科创板(688)、北交所(8/4开头)
        if stock.startswith('300') or stock.startswith('301') or \
           stock.startswith('688') or stock.startswith('8') or stock.startswith('4'):
            continue
        # 过滤 ST / *ST / 退市
        if current_data[stock].is_st or 'ST' in current_data[stock].name or '退' in current_data[stock].name:
            continue
        # 过滤停牌
        if current_data[stock].paused:
            continue
        # 过滤上市不足180天的次新股
        info = get_security_info(stock)
        if info is not None and (current_date - info.start_date).days < 180:
            continue
        main_board_stocks.append(stock)
    return main_board_stocks


# ======================================================================
# 三·B、大盘状态分类器（盘前调用，决定当日用 trend / range / bear 哪套参数）
# ======================================================================

def classify_market_regime(context):
    """三分状态·慢进快出：用沪深300判断 trend / range / bear。
      进入trend(慢进)：价格>MA60 且 MA60上行，连续 REGIME_CONFIRM_DAYS 日确认
            → 防熊市反弹段误判为 trend 满仓进攻。
      维持trend(快出)：一旦 价格<MA20 或 MA20 下行 → 立即退出至 range/bear
            → 防见顶时 MA60 滞后仍上行、trend 不退而满仓吃急跌。
      bear  (熊市/下行)：价格<MA60 且 MA60 下行 → 不开新仓。
      range (震荡)：其余。
    设计依据：
      ① 进出非对称——进入用 MA60(慢、稳，抗反弹插针)，退出用 MA20(快，防见顶滞后)；
      ② bear 立即生效(下行市快速空仓防御)。"""
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

        trend_enter = (price > ma60_now) and ma60_up   # 严格进入：中期多头+中期上行
        trend_stay = (price > ma20_now) and ma20_up    # 宽松维持：短期多头+短期上行(失效即快出)
        bear_cond = (price < ma60_now) and ma60_down

        if g.active_mode == 'trend':
            # 已在 trend：维持条件不满足 → 立即退出(快出，防见顶满仓吃跌)
            if not trend_stay:
                g.trend_confirm_count = 0
                return 'bear' if bear_cond else 'range'
            return 'trend'
        else:
            # 不在 trend：严格条件+连续确认才进入(慢进，防熊市反弹误判)
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
# 四、盘前：风控清理 + 两道闸构建"波浪买点候选池"
#     第1闸: 日线廉价预筛(回踩+位置，最便宜, 先砍大头)
#     第2闸: 日线波浪检测(250根, 找 浪1+浪2 结构, 放最后, 池满即停)
# ======================================================================

def before_market_open(context):
    current_date = context.current_dt.date()
    g.today_buy_count = 0
    g.candidate_pool = set()
    g.elliott_setups = {}          # {stock: (trigger, stop_ref)}

    # 盘前先判市场状态(三分：trend/range/bear)
    g.active_mode = classify_market_regime(context)
    mode_desc = {'trend': '趋势多头(trend)→进取', 'range': '弱势震荡(range)→防御',
                 'bear': '熊市下行(bear)→不开新仓'}[g.active_mode]
    log.info("🌐 今日市场状态: %s" % mode_desc)

    # 4.1 黑名单清理（亏损5天/止盈2天）
    for stock in list(g.blacklist_dict.keys()):
        entry = g.blacklist_dict[stock]
        # 兼容旧格式（若曾存单一日期，按亏损天数处理）
        if isinstance(entry, tuple):
            entry_date, ban_days = entry
        else:
            entry_date, ban_days = entry, BLACKLIST_LOSS_DAYS
        if (current_date - entry_date).days > ban_days:
            del g.blacklist_dict[stock]

    # 4.2 死磕池 / 锁仓 / 去重表 清洗
    for stock in list(g.pending_exit_stocks.keys()):
        if stock not in context.portfolio.positions:
            g.pending_exit_stocks.pop(stock, None)
    for stock in list(g.position_lock_stocks):
        if stock not in context.portfolio.positions and stock not in g.pending_exit_stocks:
            g.position_lock_stocks.discard(stock)
    for stock in list(g.trailing_stop_last_date.keys()):
        if stock not in context.portfolio.positions and stock not in g.pending_exit_stocks:
            g.trailing_stop_last_date.pop(stock, None)

    # 4.3 bear(熊市下行)状态不开新仓，跳过候选池构建（持仓风控照常由盘中 _check_stop_loss 处理）
    if g.active_mode == 'bear':
        log.info("🐻 熊市状态，今日不开新仓（持仓风控照常）")
        return

    # 4.4 【构建波浪买点候选池】trend 宽入口(上限300) / range 严入口(上限40)
    max_cand = TREND_MAX_CANDIDATES if g.active_mode == 'trend' else MAX_CANDIDATES
    main_board_pool = get_main_board_pool(context)
    elliott_count = 0   # 第2闸重计算次数统计（日志观察性能/信号密度）
    # 确定性修复：池满即停(break 在 max_cand)依赖遍历顺序；对
    # main_board_pool 排序，保证选取在不同运行间一致、可复现。
    for stock in sorted(main_board_pool):
        if len(g.candidate_pool) >= max_cand:
            break
        if stock in context.portfolio.positions or stock in g.blacklist_dict \
           or stock in g.position_lock_stocks or stock in PERM_HOLD_CODES:
            continue

        # --- 第1闸·廉价日线预筛（两个检查共用一次 120 根取数）---
        # 检查①"回踩"：近5日低点须触及30日低点区域（价格必须回踩下来）；
        # 检查②"位置"：现价在120日高低区间分位 ≤ 60%。
        #   仅有①时，若大拉升发生在30日窗口之外，"30日低点"本身悬在山腰平台上，
        #   回踩到它仍是高位接货；②把锚定校准到半年窗口，要求现价处于长周期区间的
        #   中下部，二者叠加才真正拒绝追高。
        pre = get_bars(stock, count=LONG_POS_WINDOW, unit='1d',
                       fields=['high', 'low', 'close'], include_now=False)
        if pre is None or len(pre) < PRESCREEN_LOW_WINDOW:
            continue
        lows_all = np.asarray(pre['low'], dtype=float)
        highs_all = np.asarray(pre['high'], dtype=float)
        closes_all = np.asarray(pre['close'], dtype=float)

        # 检查①：近5日回踩到30日低点区域（窗口取最近30根）
        lows30 = lows_all[-PRESCREEN_LOW_WINDOW:]
        if np.nanmin(lows30[-PRESCREEN_RECENT:]) > np.nanmin(lows30):
            continue  # 最近5日没回踩到30日低点区域

        # 检查②：长窗口位置分位 ≤ LONG_POS_MAX（拒绝"30日低点在山腰"的票）
        #   trend 模式不做此过滤（不拒创新高赢家，否则错过主升浪）。
        if g.active_mode != 'trend':
            lo_120 = np.nanmin(lows_all)
            hi_120 = np.nanmax(highs_all)
            rng = hi_120 - lo_120
            if rng > 1e-6:
                pos_pct = (closes_all[-1] - lo_120) / rng
                if pos_pct > LONG_POS_MAX:
                    continue  # 现价仍在半年区间上部 → 前期大拉升的山腰/高位，拒绝

        # --- 第2闸·日线波浪检测（宽松摆动 + 斐波那契）：找 浪1+浪2 结构 ---
        #   浪2 回撤比例合格 且 浪2 不破浪1起点 → 突破浪1高点=浪3启动。
        sig, trigger, stop_ref = detect_elliott_buy(stock)
        if sig != 'buy':
            continue
        elliott_count += 1
        g.candidate_pool.add(stock)
        g.elliott_setups[stock] = (trigger, stop_ref)

    log.info("📋 今日波浪候选池 %d 只 [%s模式, 上限%d]（波浪重算 %d 次）"
             % (len(g.candidate_pool), g.active_mode, max_cand, elliott_count))


# ======================================================================
# 五、开盘：跌停拦截 + 死磕池处理 + ST强制清仓
# ======================================================================

def check_perm_hold(context, current_data):
    """永久持仓检查：深度价值条件(PB/PE)满足则用固定预算买入并永久持有。
    永久持仓不参与止损/止盈/开盘跌停拦截——建仓后锁仓不动，与策略仓位独立。"""
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
            g.perm_hold_done.add(code)   # 已持仓视为完成
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
            continue   # 涨停不追
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

    # 永久持仓检查（深度价值买入，与策略仓位独立）
    check_perm_hold(context, current_data)

    # 5.1 开盘即跌停的持仓：立即挂跌停价限价单卖出（永久持仓股 / 货币ETF 跳过）
    for security in list(context.portfolio.positions.keys()):
        if security in g.perm_hold_done or security == CASH_ETF:
            continue   # 永久持仓 / 货币ETF 不卖出
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
            # 卖不掉的记录锁死价，进补卖池
            if security not in g.pending_exit_stocks:
                g.pending_exit_stocks[security] = data.low_limit

    # 5.2 历史死磕池优先处理
    if g.pending_exit_stocks:
        log.warning("🔄 [待补卖池] 检查 %d 只历史锁死标的..." % len(g.pending_exit_stocks))
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

    # 5.3 持仓突变 ST/退 → 开盘强制清仓（货币ETF 跳过）
    for security in list(context.portfolio.positions.keys()):
        if security in g.pending_exit_stocks or security == CASH_ETF:
            continue
        if current_data[security].is_st or 'ST' in current_data[security].name \
           or '退' in current_data[security].name:
            amount = context.portfolio.positions[security].total_amount
            log.error("⚠️ 持仓 %s 变为ST/退，开盘强制清仓 %d 股" % (security, amount))
            reference_price = current_data[security].day_open or current_data[security].last_price
            order_amount_in_chunks(security, -amount, reference_price)


# ======================================================================
# 六、盘中 every_bar：突破入场（廉价价格比较）+ 30分节拍止损 + 14:57 终检
# ======================================================================

def market_intraday(context):
    current_dt = context.current_dt
    current_date = current_dt.date()
    current_time = current_dt.strftime('%H:%M')
    current_data = get_current_data()

    # 6.1 每根bar检查突破入场（低成本，仅价格比较，无需重算波浪）
    _process_elliott_entries(context, current_data, current_date)

    # 6.2 风控节拍：30分钟收盘时刻做止损重检
    if current_time in BAR_CHECK_TIMES:
        _check_stop_loss(context, current_data, current_date)

    # 6.3 14:57 及以后：死磕池生死劫 + 止损终检
    if current_time < '14:57':
        return

    # 死磕池 14:57 生死劫：收复锁死价+MA20 → 留活口；否则限价清仓
    if g.pending_exit_stocks:
        for security in list(g.pending_exit_stocks.keys()):
            if security not in context.portfolio.positions:
                g.pending_exit_stocks.pop(security, None)
                continue
            if security == CASH_ETF:
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

    # 止损终检（当日最后一次机会，覆盖风控节拍之间发生的破位）
    _check_stop_loss(context, current_data, current_date)


# ----------------------------------------------------------------------
# 六·A：止损检查（结构止损为主 —— 波浪证伪替代均线止损）
#   设计逻辑：本策略买的是"浪2 洗盘后的浪3 启动突破"，此时均线（MA5/MA10
#   贴近或低于 MA20）天然处于"回调转强"形态，若沿用均线止损，买入次日
#   就会被扫地出门。买突破的正确止损是【波浪证伪】：
#     - 浪2 低点(L2)，是"回调结束、浪3 启动"的锚点；
#     - 价格若有效跌破该低点（留 STRUCT_STOP_BUFFER 缓冲防插针），
#       说明浪级失效（实际是 B 浪反弹而非推动浪3）→ 无条件离场。
#   刚性 -8% 降级为灾难兜底；并新增【利润棘轮】锁利。
#   本函数在30分收盘节拍 + 14:57 各查一次（比只在 14:57 检查响应更快）。
# ----------------------------------------------------------------------

def _ma_stop_triggered(security, current_price, total_pnl_ratio):
    """trend 模式均线止损（= V2.0）：-5% 硬 + 跌破MA20 + 均线转弱(MA20>MA5/MA10)。
    亦用作无 stop_ref 仓位的回退止损。返回 (should_sell, reason)。数据不足时仅用刚性。"""
    try:
        dbars = get_bars(security, count=30, unit='1d', fields=['close'], include_now=False)
    except Exception:
        dbars = None
    if dbars is not None and len(dbars) >= 20:
        dc = np.asarray(dbars['close'], dtype=float)
        ma5 = dc[-5:].mean()
        ma10 = dc[-10:].mean()
        ma20 = dc[-20:].mean()
        if total_pnl_ratio <= TREND_HARD_STOP:
            return True, "趋势态触及 %.0f%% 刚性止损" % (TREND_HARD_STOP * 100)
        if current_price < ma20:
            return True, "趋势态跌破MA20(%.2f)" % ma20
        if ma20 > ma5 or ma20 > ma10:
            return True, "趋势态均线转弱(MA20>MA5/MA10)"
        return False, ""
    if total_pnl_ratio <= TREND_HARD_STOP:
        return True, "趋势态触及 %.0f%% 刚性止损(数据不足)" % (TREND_HARD_STOP * 100)
    return False, ""


def _check_stop_loss(context, current_data, current_date):
    current_positions = context.portfolio.positions
    for security in list(current_positions.keys()):
        if security in g.perm_hold_done or security == CASH_ETF:
            continue   # 永久持仓 / 货币ETF 不参与止损
        if security in g.pending_exit_stocks:
            continue
        if g.trailing_stop_last_date.get(security) == current_date:
            continue
        position = current_positions[security]
        if position.closeable_amount == 0:
            continue  # T+1：当日新仓无法卖出，次日自动纳入检查

        current_price = current_data[security].last_price if security in current_data else np.nan
        if np.isnan(current_price) or current_price <= 0:
            continue
        # 跌停封死卖不出 → 交给开盘拦截/死磕机制处理
        if current_price <= current_data[security].low_limit:
            continue

        my_cost = g.buy_cost_dict.get(security, position.avg_cost)
        total_pnl_ratio = (current_price - my_cost) / my_cost

        # --- 利润棘轮·水位更新（只用已发生的盘中现价，无未来函数）---
        hwm = g.high_watermark_dict.get(security, my_cost)
        if current_price > hwm:
            hwm = current_price
            g.high_watermark_dict[security] = hwm

        should_sell = False
        reason = ""

        # ---- 止损分支：有结构止损位(stop_ref=浪2低点)→结构止损+刚性+棘轮；无→MA止损 ----
        stop_ref = g.stop_ref_dict.get(security)
        if stop_ref is not None:
            # a) 结构止损（主力）：跌破浪2低点（波浪证伪）。
            #    入场前已保证触发价距买入价 ≤ ELLIOTT_MAX_STOP_DIST，故正常破位
            #    一定是它先于刚性线触发。
            if current_price < stop_ref * (1.0 - STRUCT_STOP_BUFFER):
                should_sell = True
                reason = "跌破浪2低点 %.2f（结构止损·波浪证伪）" % stop_ref
            # b) 刚性止损：-8% 纯灾难兜底（隔夜跳空/极端行情）
            elif total_pnl_ratio <= HARD_STOP_RATIO:
                should_sell = True
                reason = "触及 %.0f%% 刚性止损线（灾难兜底）" % (HARD_STOP_RATIO * 100)
            # c) 利润棘轮：浮盈曾达激活线后，从最高水位回撤超阈值 → 锁定利润
            elif hwm >= my_cost * (1.0 + TRAIL_ACTIVATE_RATIO) \
                    and current_price <= hwm * (1.0 - TRAIL_DRAWDOWN_RATIO):
                should_sell = True
                reason = "利润棘轮止盈：最高 %.2f(+%.1f%%) 回撤%.0f%%" \
                         % (hwm, (hwm - my_cost) / my_cost * 100, TRAIL_DRAWDOWN_RATIO * 100)
        else:
            # ---- MA 均线止损（趋势市无结构止损位的仓用均线顺势止损）----
            should_sell, reason = _ma_stop_triggered(security, current_price, total_pnl_ratio)

        if should_sell:
            log.warn("🚨【风控卖出】%s 原因: %s，当前盈亏 %.2f%%"
                     % (security, reason, total_pnl_ratio * 100))
            order_amount_in_chunks(security, -position.closeable_amount, current_price)
            g.trailing_stop_last_date[security] = current_date
            _post_exit_cleanup(security, current_date, is_loss=(total_pnl_ratio < 0))


# ----------------------------------------------------------------------
# 六·B：盘中突破入场处理器（波浪买点）
#   候选池（盘前波浪检测过）的个股，盘中现价突破"浪1高点(trigger)"即视为
#   浪3 启动，按【比例仓位】直接买入，并存入浪2低点作为结构止损位。
# ----------------------------------------------------------------------

def _process_elliott_entries(context, current_data, current_date):
    # bear 状态不开新仓
    if g.active_mode == 'bear':
        return
    # 无候选则不开新仓（也避免无谓变现货币ETF）
    if not g.candidate_pool:
        return
    if g.today_buy_count >= MAX_BUYS_PER_DAY:
        return

    # 仓位预算：闲余现金 + 货币ETF市值（避免"现金全在511880导致可用现金偏低、
    # 误判无钱而永不变现永不买"的死锁）。确有买点时才变现511880转可用现金。
    etf_pos = context.portfolio.positions.get(CASH_ETF)
    etf_value = 0.0
    if etf_pos is not None and etf_pos.closeable_amount > 0:
        ep = current_data[CASH_ETF].last_price if CASH_ETF in current_data else 0.0
        etf_value = etf_pos.closeable_amount * ep
    cash_reserve = TREND_CASH_RESERVE if g.active_mode == 'trend' else CASH_RESERVE_RATIO
    buy_budget = context.portfolio.available_cash + etf_value \
                 - context.portfolio.total_value * cash_reserve
    if buy_budget <= 10000:
        return

    triggered = []
    # 确定性修复：elliott_setups 遍历顺序固定按代码序，使多只同刻触发时选取可复现。
    for stock in sorted(g.elliott_setups.keys()):
        if g.today_buy_count + len(triggered) >= MAX_BUYS_PER_DAY:
            break
        if stock in g.position_lock_stocks or stock in context.portfolio.positions \
           or stock in g.blacklist_dict or stock in PERM_HOLD_CODES:
            continue
        trigger, stop_ref = g.elliott_setups[stock]
        price = current_data[stock].last_price if stock in current_data else np.nan
        if np.isnan(price) or price <= 0:
            continue
        # 突破浪1高点 = 浪3 启动 → 买入
        if price <= trigger:
            continue
        # 涨停不追：现价已封涨停则放弃（买不进且属情绪追高）
        if price >= current_data[stock].high_limit:
            continue
        triggered.append((stock, price, stop_ref, trigger))

    if not triggered:
        return

    # 确有加仓目标 → 加仓前变现货币ETF（按需，避免无信号日白白损失隔夜利息）
    _redeem_cash_etf(context)

    # 【建仓】trend 等权(budget/n，封顶 TREND_PER_STOCK_MAX_RATIO) /
    #        range 比例仓位(总资产×PER_STOCK_MAX_RATIO，始终留30%现金)。
    #   用 spent 本地累计，规避 available_cash 在同一bar内可能滞后更新。
    is_trend = (g.active_mode == 'trend')
    n = len(triggered)
    trend_eq = buy_budget / n
    trend_cap = context.portfolio.total_value * TREND_PER_STOCK_MAX_RATIO
    per_stock_cap = min(trend_eq, trend_cap) if is_trend \
                    else (context.portfolio.total_value * PER_STOCK_MAX_RATIO)
    spent = 0.0
    for stock, price, stop_ref, trigger in triggered:
        if g.today_buy_count >= MAX_BUYS_PER_DAY:
            break
        remaining = buy_budget - spent           # 本批剩余可用预算
        if remaining <= 10000:
            break
        target_value = min(per_stock_cap, remaining)  # 上限与剩余预算取小
        ordered_amount = order_buy_once(stock, target_value, price)
        if ordered_amount >= 100:
            spent += ordered_amount * price
            g.buy_cost_dict[stock] = price
            g.position_lock_stocks.add(stock)
            g.today_buy_count += 1
            g.elliott_setups.pop(stock, None)
            # 波浪证伪位=浪2低点，结构止损（跌破=浪级失效）；并起棘轮水位。
            g.stop_ref_dict[stock] = stop_ref
            g.high_watermark_dict[stock] = price
            log.info("🛒【波浪买·浪3启动】%s 突破浪1高点 %.2f，现价 %.2f 买入 %d 股(≈%.0f元/%s%d只)，结构止损(浪2低点) %.2f（距价%.1f%%）"
                     % (stock, trigger, price, ordered_amount, ordered_amount * price,
                        ('等权' if is_trend else '上限'), n, stop_ref,
                        (price - stop_ref) / price * 100))


# ======================================================================
# 六·C：货币ETF现金管理（闲置现金赚隔夜利息，加仓前按需变现）
# ======================================================================

def _redeem_cash_etf(context):
    """加仓前变现：把持有的货币ETF(511880)全部转为可用现金，供建仓使用。
    非无条件每日变现——仅在确有加仓意图（候选池非空）时由
    _process_elliott_entries 调用，避免 bear/无信号日每天白白卖掉损失隔夜利息。"""
    pos = context.portfolio.positions.get(CASH_ETF)
    if pos is None or pos.closeable_amount <= 0:
        return
    try:
        order_target(CASH_ETF, 0)   # 清掉货币ETF，现金回到可用
        log.info("💵 [货币ETF变现] 卖出 511880 %d 份，释放现金用于加仓" % pos.closeable_amount)
    except Exception as e:
        log.warn("货币ETF变现失败: %s" % e)


def _invest_cash_etf(context, current_data):
    """盘后现金管理：把闲余现金(available_cash - 保留额)买入 511880 货币ETF赚隔夜利息。
    货币ETF不参与止损/止盈/跌停拦截；不在 perm_hold 也不在候选池。"""
    try:
        avail = context.portfolio.available_cash
        invest_cash = avail - CASH_ETF_RESERVE
        if invest_cash < 10000:   # 至少够买一手(约1万元)才做
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


# ----------------------------------------------------------------------
# 六·D：清仓善后（无熔断：仅黑名单区分盈亏 + 清缓存 + 解锁）
# ----------------------------------------------------------------------

def _post_exit_cleanup(security, current_date, is_loss):
    """清仓善后：黑名单(区分盈亏) / 清缓存 / 解锁。"""
    if is_loss:
        ban_days = BLACKLIST_LOSS_DAYS
    else:
        ban_days = BLACKLIST_PROFIT_DAYS
    g.blacklist_dict[security] = (current_date, ban_days)

    g.buy_cost_dict.pop(security, None)
    g.stop_ref_dict.pop(security, None)
    g.high_watermark_dict.pop(security, None)
    g.position_lock_stocks.discard(security)


# ======================================================================
# 七、盘后审计（15:30）：跌停废单拦截 + 货币ETF现金管理
# ======================================================================

def after_market_close(context):
    """复盘今日全部卖出单，凡因跌停流动性枯竭导致撤单/拒单且仍有实仓的，
    记录锁死价拖入补卖池，次日开盘继续死磕。最后把闲余现金投入货币ETF。"""
    todays_orders = get_orders()
    if todays_orders:
        for order_id, order_obj in todays_orders.items():
            if order_obj.action == 'close':
                if order_obj.status.name in ['canceled', 'rejected']:
                    security = order_obj.security
                    if security in context.portfolio.positions and security != CASH_ETF:
                        if security not in g.pending_exit_stocks:
                            current_data = get_current_data()
                            g.pending_exit_stocks[security] = current_data[security].low_limit
                            log.error("🚨 [死锁拦截] %s 今日卖出未成交且仍有实仓，"
                                      "锁死价 %.2f，已入死磕补卖池，明日开盘继续卖出！"
                                      % (security, current_data[security].low_limit))

    # 闲余现金买入货币ETF（T+0，次日享受隔夜利息；非永久持仓、非候选池）
    _invest_cash_etf(context, get_current_data())
