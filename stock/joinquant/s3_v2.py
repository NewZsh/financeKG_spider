# -*- coding: utf-8 -*-
"""
策略名称：
全主板"安全背驰买点"缠论分钟频策略 (s3)  —— 30分钟直买版

======================================================================
一、策略总纲
======================================================================
本策略在 s1.py（突破-洗盘-反包策略）的工程框架上，把"买卖信号"替换为
缠论的【安全背驰买卖点】，其余仓位管理 / 止损 / 跌停死磕机制沿用 s1。

★★ 本版改动（相对旧版最重要的一点）★★
    去掉"观察池 + 隔日企稳确认"两段式买入流程。
    改为：盘中每个 30 分钟 K 线收盘时刻，直接在 30 分钟级别检测
    "安全背驰买点"，一旦成立【立即下单买入】，不再等待次日、
    不再要求现价站上信号日收盘。信号更快、更贴近真实分钟频交易。

【安全背驰买点】= 30分钟背驰信号 + 两重安全过滤：
    1) 30分钟背驰信号（一类买点候选）：
       30分钟级别，最近一笔为"下降笔"，价格创出比上一同向笔更低的
       低点，但该笔区间 MACD 柱面积(绝对值之和) 比上一同向下降笔明显
       缩小 → 下跌动能衰竭，即缠论"底背驰"。信号须新鲜（发生在最近
       INTRADAY_FRESH_BARS 根30分钟K线内）。
    2) 月线大势过滤（盘前对候选池预先计算）：
       月线收盘价必须站在月线 MA10 之上（月线多头）。
       目的：过滤"下跌趋势中途的反弹背驰"（高位危险买点），
       只保留"大级别上升趋势中的回调背驰"（安全的中低位买点）。
    3) 中枢位置过滤：
       当前价不得高于最近一个 30分钟中枢的上沿 ZG。
       确保买点发生在中枢内部或下方（一/二类买点区域），
       而不是远离中枢的主升浪半山腰。

【安全背驰卖点】（用于止盈，替代 s1 的移动止盈）：
    同样改到 30分钟级别，盘中每个30分钟收盘时刻扫描持仓：
    1) 30分钟顶背驰：最近一笔为"上升笔"，价格创新高但 MACD 柱面积缩小。
    2) 中枢位置过滤：当前价高于最近30分钟中枢上沿 ZG（真正的高位）。
    满足即全部清仓止盈。

======================================================================
二、与 s1 保持一致的部分
======================================================================
1. 股票池：全主板（过滤 300/301 创业板、688 科创板、8/4 北交所、
   ST/*ST/退、停牌、上市不足180天）。
2. 仓位管理：
   - 保持 30% 现金，剩余现金对当批触发股票等权分配；
   - 单股单次建仓 ≤ 10 万元；
   - 单股一个持仓周期只买一次（position_lock）；
   - 每日最多买入 MAX_BUYS_PER_DAY 只。
3. 止损（14:57 判定，与 s1 完全一致）：
   - 刚性止损：亏损 ≥ 5% 清仓；
   - 趋势止损：14:57 价格跌破 MA20 清仓；
   - 均线转弱：MA20 > MA5 或 MA20 > MA10 清仓。
4. 跌停等特殊情况（与 s1 完全一致）：
   - 开盘即跌停 → 挂跌停价限价单"死磕"卖出；
   - 卖不掉的进 pending_exit_stocks 补卖池，次日开盘继续死磕；
   - 14:57 "锁死收复判定"：收复锁死价且突破 MA20 → 恢复持仓，
     否则限价清仓；
   - 15:30 盘后审计：撤单/拒单且仍有实仓的股票拖入补卖池。
5. 风控善后：黑名单 5 天、连续亏损 5 次熔断冻结 8 日（默认注释关闭，
   与 s1 当前状态一致）。

======================================================================
三、性能设计（分钟频回测的关键）
======================================================================
30分钟缠论计算(分型→笔→中枢→背驰)较重，绝不能在盘中每分钟对全市场算。
分层设计：
  - before_market_open（盘前，每日一次）：
      a) 廉价日线预筛：只对"近 PRESCREEN_RECENT 日回踩到
         PRESCREEN_LOW_WINDOW 日低点区域"的主板股保留；
      b) 月线多头过滤（月收盘 ≥ 月MA10），通过者进入【监控候选池】
         g.candidate_pool（上限 MAX_CANDIDATES 只）。
         注意：候选池只是"盯盘范围"，不是旧版的"信号观察池"——
         它不缓存任何已成立信号、不做隔日延迟，纯粹为性能缩小扫描面。
      c) 缓存大盘安全开关（沪深300 收盘 ≥ 60日均值）。
  - market_intraday（盘中每分钟触发）：
      * 仅在 30分钟收盘时刻（BAR_30M_TIMES）做重计算：
          - 候选池：取30分钟K线 → 检测安全背驰买点 → 【直接买入】；
          - 持仓股：取30分钟K线 → 检测安全背驰卖点 → 直接止盈。
      * 14:57 及以后：统一执行三层止损 + 跌停死磕生死劫。

======================================================================
四、策略流程 UML
======================================================================
```mermaid
flowchart TD
    A[盘前] --> B[清理黑名单/补卖池/锁仓残留]
    B --> C[全主板日线预筛: 近5日回踩30日低点区域]
    C --> D{月线在MA10上方?}
    D -- 否 --> D1[危险, 丢弃]
    D -- 是 --> E[加入监控候选池 candidate_pool]
    E --> MO[开盘: 跌停拦截/死磕池挂单/ST清仓]
    MO --> I[盘中每分钟]
    I --> T{是30分钟收盘时刻?}
    T -- 是 --> S[持仓30分钟顶背驰+中枢上方 → 止盈全清]
    S --> J{大盘安全 & 现金充足?}
    J -- 是 --> K[候选池30分钟底背驰+月多头+中枢下方 → 直接买入]
    I --> O[14:57: 死磕池生死劫 + 三层止损]
    O --> P[15:30 盘后审计: 废单入补卖池]
```
"""
from jqdata import *
import numpy as np
import pandas as pd

# ==================== 全局常量 ====================
MAX_TRADE_VALUE = 100000       # 单股单次建仓金额上限（元）
MAX_BUYS_PER_DAY = 5           # 每日最大买入只数
PRESCREEN_LOW_WINDOW = 30      # 日线预筛：回踩到 N 日低点区域（捕捉上升趋势中的回调）
PRESCREEN_RECENT = 5           # 日线预筛：低点须发生在最近 N 日内
MAX_CANDIDATES = 300           # 每日监控候选池上限（防盘中超时）
BARS_30M_FOR_CHAN = 250        # 30分钟缠论分析所用K线根数（约31个交易日）
INTRADAY_FRESH_BARS = 2        # 30分钟背驰信号新鲜度：须发生在最近 N 根30分钟K线内

# 30分钟K线收盘时刻（沪深两市）。15:00收盘无法成交，故不纳入；
# 仅在这些时刻做重量级缠论检测与下单。
BAR_30M_TIMES = {'10:00', '10:30', '11:00', '11:30', '13:30', '14:00', '14:30'}


# ======================================================================
# 〇、缠论核心算法（自包含，移植自本地 ta_calc.py，纯 numpy/pandas 实现）
#     —— 与级别无关：喂日线就是日线级别，喂30分钟线就是30分钟级别
# ======================================================================

def _fractal(high, low):
    """顶/底分型识别。
    输入: high/low 为 np.ndarray
    输出: f 数组, 1=顶分型, -1=底分型, 0=无
    定义: 第i根K线高低点同时高于左右两根 → 顶分型; 同时低于 → 底分型。"""
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
    返回 [(start_idx, end_idx, direction, start_price, end_price)]
    direction: 1=上升笔(底分型→顶分型), -1=下降笔(顶分型→底分型)"""
    f = _fractal(high, low)
    pts = [(i, v) for i, v in enumerate(f) if v != 0]
    bi = []
    last = None
    for idx, v in pts:
        if last is None:
            last = (idx, v)
            continue
        if v == last[1]:
            # 同性质分型：保留更极端者（顶取更高的、底取更低的）
            if v == 1:
                if high[idx] > high[last[0]]:
                    last = (idx, v)
            else:
                if low[idx] < low[last[0]]:
                    last = (idx, v)
            continue
        # 异性质分型：间隔足够则成笔
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
    """简化中枢：连续3笔的价格区间存在重叠 → 中枢 [ZD, ZG]。
    返回 [(start_idx, end_idx, ZG, ZD)]，取重叠区间。"""
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


def detect_beichi_signal(high, low, close, recent_bars=2):
    """检测最近是否出现"背驰"信号（缠论一类买卖点候选）。
    —— 本函数与级别无关，传入哪个周期的K线就是哪个周期的背驰。

    判定逻辑（与本地 ta_calc.bei_chi 一致）：
      取最近一笔 cur 与上一根同向笔 prev（间隔一笔），
      - 底背驰(买)：cur 是下降笔, cur 低点 < prev 低点(创新低)，
        且 cur 区间 |MACD柱面积| < prev 区间 |MACD柱面积|
      - 顶背驰(卖)：cur 是上升笔, cur 高点 > prev 高点(创新高)，
        且面积同样缩小
      信号新鲜度：cur 笔终点必须落在最近 recent_bars 根K线内。

    返回: ('buy'/'sell'/None, last_zg, last_zd)
      last_zg/last_zd 为最近一个中枢的上/下沿(无中枢则 None)，
      供上层做"中枢位置"安全过滤。"""
    bi = _bi_list(high, low)
    if len(bi) < 3:
        return None, None, None

    zsl = _zhongshu_from_bi(bi)
    last_zg = zsl[-1][2] if zsl else None
    last_zd = zsl[-1][3] if zsl else None

    hist = _macd_hist(close)
    n = len(close)

    cur, prev = bi[-1], bi[-3]
    # 信号必须"新鲜"：最近一笔的终点在最后 recent_bars 根K线内
    if cur[1] < n - recent_bars:
        return None, last_zg, last_zd
    if cur[2] != prev[2]:
        return None, last_zg, last_zd

    area_cur = abs(np.nansum(hist[cur[0]:cur[1]+1]))
    area_prev = abs(np.nansum(hist[prev[0]:prev[1]+1]))
    if area_prev <= 0:
        return None, last_zg, last_zd

    if cur[2] == -1 and cur[4] < prev[4] and area_cur < area_prev:
        return 'buy', last_zg, last_zd    # 底背驰
    if cur[2] == 1 and cur[4] > prev[4] and area_cur < area_prev:
        return 'sell', last_zg, last_zd   # 顶背驰
    return None, last_zg, last_zd


def is_monthly_trend_up(security, context):
    """月线大势过滤：月线收盘 ≥ 月线 MA10 → 视为大级别多头。
    这是"安全背驰买点"的第一重安全阀：
    月线趋势向下时出现的底背驰，多为下跌中继的反弹（危险买点），
    必须放弃。盘前对候选池预先计算，盘中直接信任（月线日内不变）。"""
    try:
        mdf = get_bars(security, count=12, unit='1M', fields=['close'], include_now=True)
        if mdf is None or len(mdf) < 10:
            return False   # 数据不足按不安全处理
        closes = np.asarray(mdf['close'], dtype=float)
        ma10 = closes[-10:].mean()
        return closes[-1] >= ma10
    except Exception as e:
        log.warn("月线数据获取失败 %s: %s" % (security, e))
        return False


# ======================================================================
# 一、下单工具函数（与 s1 完全一致）
# ======================================================================

def order_buy_once(security, total_value, reference_price):
    """单次建仓买入：金额封顶 MAX_TRADE_VALUE，按手取整。"""
    if reference_price is None or np.isnan(reference_price) or reference_price <= 0:
        return 0
    capped_value = min(float(total_value), MAX_TRADE_VALUE)
    total_amount = int(capped_value / reference_price / 100) * 100
    if total_amount < 100:
        return 0
    order(security, total_amount)
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
    log.info('【缠论安全背驰版 s3 v2.0 · 30分钟直买】启动...')

    # 主板标准佣金（最低5元）
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003,
                             close_commission=0.0003, min_commission=5), type='stock')

    # ---- 交易状态 ----
    g.buy_cost_dict = {}            # 个股真实买入成本 {stock: price}
    g.blacklist_dict = {}           # 黑名单 {stock: 加入日期}，5天后自动移出
    g.consecutive_loss_count = 0    # 连续亏损计数（熔断用）
    g.freeze_days_left = 0          # 熔断剩余冻结天数
    g.position_lock_stocks = set()  # 持仓周期锁：清仓前不允许再次买入
    g.trailing_stop_last_date = {}  # 个股当日卖出去重 {stock: date}
    g.today_buy_count = 0           # 当日已买入只数

    # ---- 监控候选池（盘前构建，仅缩小盘中扫描面，非信号观察池）----
    g.candidate_pool = set()        # {stock, ...} 月线多头 + 日线回踩到位的候选

    # ---- 高频缓存（盘前算好，盘中直读，避免分钟级限流卡死）----
    g.is_market_safe = False        # 大盘趋势开关：沪深300 收盘 ≥ 60日均值

    # ---- 跌停死磕机制（与 s1 一致）----
    g.pending_exit_stocks = {}      # {stock: 锁死价} 因跌停未能离场的补卖池

    # 四大定时任务
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    run_daily(market_open, time='open', reference_security='000300.XSHG')
    run_daily(market_intraday, time='every_bar', reference_security='000300.XSHG')
    run_daily(after_market_close, time='15:30', reference_security='000300.XSHG')


# ======================================================================
# 三、股票池过滤：全主板（非ST、非创业、非科创、非北交所）
#     —— 与 s1 完全一致
# ======================================================================

def get_main_board_pool(context):
    """动态筛出当天：纯沪深主板 + 非ST + 非停牌 + 上市满180天 的正常股。"""
    current_date = context.current_dt.date()
    all_stocks = list(get_all_securities(['stock'], date=current_date).index)
    current_data = get_current_data()

    main_board_stocks = []
    for stock in all_stocks:
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
# 四、盘前：风控清理 + 构建"监控候选池"（月线多头 + 日线回踩到位）
#     不再在盘前生成任何买卖信号——信号一律盘中30分钟级别实时判定。
# ======================================================================

def before_market_open(context):
    current_date = context.current_dt.date()
    g.today_buy_count = 0

    # 4.1 策略级熔断（与 s1 一致，默认注释关闭）
    # if g.freeze_days_left > 0:
    #     g.freeze_days_left -= 1
    #     log.warn("策略熔断保护中，剩余 %d 天不买入。" % g.freeze_days_left)
    #     return

    # 4.2 黑名单清理：满5天自动移出
    for stock in list(g.blacklist_dict.keys()):
        if (current_date - g.blacklist_dict[stock]).days > 5:
            del g.blacklist_dict[stock]

    # 4.3 死磕池 / 锁仓 / 去重表 清洗（与 s1 一致）
    for stock in list(g.pending_exit_stocks.keys()):
        if stock not in context.portfolio.positions:
            g.pending_exit_stocks.pop(stock, None)
    for stock in list(g.position_lock_stocks):
        if stock not in context.portfolio.positions and stock not in g.pending_exit_stocks:
            g.position_lock_stocks.discard(stock)
    for stock in list(g.trailing_stop_last_date.keys()):
        if stock not in context.portfolio.positions and stock not in g.pending_exit_stocks:
            g.trailing_stop_last_date.pop(stock, None)

    # 4.4 【构建监控候选池】日线廉价预筛 + 月线多头过滤
    #     candidate_pool 只决定"盘中盯哪些票"，不缓存信号、不做隔日延迟。
    g.candidate_pool = set()
    main_board_pool = get_main_board_pool(context)
    # 确定性修复：池满即停(break 在 MAX_CANDIDATES)依赖遍历顺序；排序以保证可复现。
    for stock in sorted(main_board_pool):
        if len(g.candidate_pool) >= MAX_CANDIDATES:
            break
        if stock in context.portfolio.positions or stock in g.blacklist_dict \
           or stock in g.position_lock_stocks:
            continue

        # --- 廉价日线预筛：近5日回踩到30日低点区域（上升趋势中的回调）---
        # 30分钟底背驰买点通常发生在日线级别回调到位处，先用它把全市场
        # 压缩到几百只，避免盘中对全市场取30分钟K线做缠论而超时。
        pre = get_bars(stock, count=PRESCREEN_LOW_WINDOW, unit='1d',
                       fields=['low'], include_now=False)
        if pre is None or len(pre) < PRESCREEN_LOW_WINDOW:
            continue
        lows = np.asarray(pre['low'], dtype=float)
        if np.nanmin(lows[-PRESCREEN_RECENT:]) > np.nanmin(lows):
            continue  # 最近5日没回踩到30日低点区域，暂不监控

        # --- 月线多头过滤（大势安全阀，盘前算好盘中直接信任）---
        if not is_monthly_trend_up(stock, context):
            continue

        g.candidate_pool.add(stock)

    log.info("📋 今日监控候选池 %d 只（月线多头 + 日线回踩到位）" % len(g.candidate_pool))

    # 4.5 大盘趋势开关缓存（与 s1 一致：沪深300 收盘 ≥ 60日均值）
    index_data = get_bars('000300.XSHG', count=60, unit='1d', fields=['close'], include_now=False)
    if index_data is not None and len(index_data) > 0:
        g.is_market_safe = index_data['close'][-1] >= index_data['close'].mean()
    else:
        g.is_market_safe = False


# ======================================================================
# 五、开盘：跌停拦截 + 死磕池处理 + ST强制清仓（与 s1 完全一致）
# ======================================================================

def market_open(context):
    current_data = get_current_data()
    current_date = context.current_dt.date()

    # 5.1 开盘即跌停的持仓：立即挂跌停价限价单卖出
    for security in list(context.portfolio.positions.keys()):
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

    # 5.3 持仓突变 ST/退 → 开盘强制清仓
    for security in list(context.portfolio.positions.keys()):
        if security in g.pending_exit_stocks:
            continue
        if current_data[security].is_st or 'ST' in current_data[security].name \
           or '退' in current_data[security].name:
            amount = context.portfolio.positions[security].total_amount
            log.error("⚠️ 持仓 %s 变为ST/退，开盘强制清仓 %d 股" % (security, amount))
            reference_price = current_data[security].day_open or current_data[security].last_price
            order_amount_in_chunks(security, -amount, reference_price)


# ======================================================================
# 六、盘中每分钟：30分钟收盘做缠论买卖点 + 14:57 止损/死磕
# ======================================================================

def market_intraday(context):
    current_dt = context.current_dt
    current_date = current_dt.date()
    current_time = current_dt.strftime('%H:%M')
    current_data = get_current_data()

    # 6.1 仅在30分钟K线收盘时刻做重量级缠论检测与下单
    if current_time in BAR_30M_TIMES:
        _process_30m_signals(context, current_data, current_date)

    # 6.2 持仓止损风控：仅 14:57 及以后（与 s1 完全一致）
    if current_time < '14:57':
        return

    # 死磕池 14:57 生死劫：收复锁死价+MA20 → 留活口；否则限价清仓
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

    # 普通持仓三层止损（与 s1 一致：刚性-5% / 跌破MA20 / 均线转弱）
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

        should_sell = False
        reason = ""
        if total_pnl_ratio <= -0.05:
            should_sell = True
            reason = "触及-5%刚性止损线"
        elif current_price < ma20:
            should_sell = True
            reason = "14:57价格跌破20日均线"
        elif ma20 > ma5 or ma20 > ma10:
            should_sell = True
            reason = "MA20(%.2f)>MA5(%.2f)或MA10(%.2f)，均线趋势转弱" % (ma20, ma5, ma10)

        if should_sell:
            log.warn("🚨【止损卖出】%s 原因: %s，当前盈亏 %.2f%%"
                     % (security, reason, total_pnl_ratio * 100))
            order_amount_in_chunks(security, -position.closeable_amount, current_price)
            g.trailing_stop_last_date[security] = current_date
            _post_exit_cleanup(security, current_date, is_loss=(total_pnl_ratio < 0))


# ----------------------------------------------------------------------
# 六·核心：30分钟收盘时刻的缠论买卖点处理器
#   先扫持仓卖点止盈，再扫候选池买点直接买入。
# ----------------------------------------------------------------------

def _process_30m_signals(context, current_data, current_date):
    # ============ A) 卖点止盈：持仓30分钟顶背驰 + 中枢上方 ============
    for security in list(context.portfolio.positions.keys()):
        if security in g.pending_exit_stocks:
            continue
        if g.trailing_stop_last_date.get(security) == current_date:
            continue
        pos = context.portfolio.positions[security]
        if pos.closeable_amount == 0:
            continue
        try:
            bars = get_bars(security, count=BARS_30M_FOR_CHAN, unit='30m',
                            fields=['high', 'low', 'close'], include_now=True)
        except Exception as e:
            log.warn("30分钟卖点数据获取失败 %s: %s" % (security, e))
            continue
        if bars is None or len(bars) < 60:
            continue
        high = np.asarray(bars['high'], dtype=float)
        low = np.asarray(bars['low'], dtype=float)
        close = np.asarray(bars['close'], dtype=float)

        sig, zg, zd = detect_beichi_signal(high, low, close, recent_bars=INTRADAY_FRESH_BARS)
        if sig != 'sell':
            continue
        # 中枢位置过滤：必须在30分钟中枢上方才算"安全卖点"（高位止盈）
        if zg is not None and close[-1] <= zg:
            continue

        current_price = current_data[security].last_price
        if np.isnan(current_price) or current_price <= 0:
            continue
        # 跌停封死卖不出，交给死磕机制
        if current_price <= current_data[security].low_limit:
            continue
        log.warn("💰【30分钟背驰止盈】%s 顶背驰+中枢上方，全仓清出 %d 股 @%.2f"
                 % (security, pos.closeable_amount, current_price))
        order_amount_in_chunks(security, -pos.closeable_amount, current_price)
        g.trailing_stop_last_date[security] = current_date
        # 善后：止盈属盈利卖出 → 连亏清零、黑名单、清缓存
        _post_exit_cleanup(security, current_date, is_loss=False)

    # ============ B) 买点买入：候选池30分钟底背驰 + 中枢下方，直接下单 ============
    # 大盘趋势过滤（读盘前缓存，不在盘中请求指数K线）
    if not getattr(g, 'is_market_safe', False):
        return
    if g.today_buy_count >= MAX_BUYS_PER_DAY:
        return
    # 仓位管理与 s1 一致：保留30%现金
    buy_budget = context.portfolio.available_cash - context.portfolio.total_value * 0.30
    if buy_budget <= 10000:
        return

    triggered_stocks = []
    # 确定性修复：candidate_pool 是 set，迭代顺序受 PYTHONHASHSEED 随机化；
    # 多票同刻触发 + break 截断会随机挑票→曲线漂移。sorted 固定按代码序，可复现。
    for stock in sorted(list(g.candidate_pool)):
        if g.today_buy_count + len(triggered_stocks) >= MAX_BUYS_PER_DAY:
            break
        if stock in g.position_lock_stocks or stock in context.portfolio.positions \
           or stock in g.blacklist_dict:
            continue

        try:
            bars = get_bars(stock, count=BARS_30M_FOR_CHAN, unit='30m',
                            fields=['high', 'low', 'close'], include_now=True)
        except Exception as e:
            log.warn("30分钟买点数据获取失败 %s: %s" % (stock, e))
            continue
        if bars is None or len(bars) < 60:
            continue
        high = np.asarray(bars['high'], dtype=float)
        low = np.asarray(bars['low'], dtype=float)
        close = np.asarray(bars['close'], dtype=float)

        sig, zg, zd = detect_beichi_signal(high, low, close, recent_bars=INTRADAY_FRESH_BARS)
        if sig != 'buy':
            continue
        # 中枢位置过滤：现价必须 ≤ 最近30分钟中枢上沿ZG（中枢内/下方买点）
        if zg is not None and close[-1] > zg:
            continue

        current_price = current_data[stock].last_price
        if np.isnan(current_price) or current_price <= 0:
            continue
        # 涨停不追：现价已封涨停则放弃（买不进且属情绪追高）
        if current_price >= current_data[stock].high_limit:
            continue

        triggered_stocks.append((stock, current_price))

    # 等权分配当批触发股票，单股买入金额封顶 10w —— 直接建仓，无隔日/企稳等待
    if len(triggered_stocks) > 0:
        cash_per_stock = buy_budget / len(triggered_stocks)
        for stock, current_price in triggered_stocks:
            ordered_amount = order_buy_once(stock, cash_per_stock, current_price)
            if ordered_amount >= 100:
                log.info("🛒【30分钟背驰买点·直买】%s 底背驰+月多头+中枢下方，现价 %.2f 买入 %d 股"
                         % (stock, current_price, ordered_amount))
                g.buy_cost_dict[stock] = current_price
                g.position_lock_stocks.add(stock)
                g.today_buy_count += 1
                g.candidate_pool.discard(stock)


def _post_exit_cleanup(security, current_date, is_loss):
    """清仓善后（与 s1 一致）：黑名单 / 连亏计数与熔断 / 清缓存 / 解锁。"""
    if is_loss:
        g.consecutive_loss_count += 1
        if g.consecutive_loss_count >= 5:
            g.freeze_days_left = 8
            log.error("💥 连续亏损5次，触发熔断，冻结买入8个交易日！")
    else:
        g.consecutive_loss_count = 0

    g.blacklist_dict[security] = current_date
    g.buy_cost_dict.pop(security, None)
    g.position_lock_stocks.discard(security)


# ======================================================================
# 七、盘后审计（15:30）：跌停废单拦截（与 s1 完全一致）
# ======================================================================

def after_market_close(context):
    """复盘今日全部卖出单，凡因跌停流动性枯竭导致撤单/拒单且仍有实仓的，
    记录锁死价拖入补卖池，次日开盘继续死磕。"""
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
                        log.error("🚨 [死锁拦截] %s 今日卖出未成交且仍有实仓，"
                                  "锁死价 %.2f，已入死磕补卖池，明日开盘继续卖出！"
                                  % (security, current_data[security].low_limit))
