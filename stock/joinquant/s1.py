# -*- coding: utf-8 -*-
"""
策略名称：
主板强势股“突破 - 洗盘 - 盘中上破昨开即介入”短线策略

- 策略目标：只做相对强势、趋势向上的股票，
1. 由于我不能实盘购买创业板、科创板和北交所，因此先排过滤创业板（300、301）、科创板（688）、北交所（8、4 开头）股票。，只做沪深主板。
2. 过滤 ST、*ST、名称含“退”的股票。
3. 过滤当日停牌股票。
4. 过滤上市未满 180 天的新股，避免次新股波动与回测失真。

- before_market_open 盘前选股
1. 先处理策略级风控：如果此前连续亏损达到熔断条件，则冻结买入若干天。
2. 清理个股黑名单：黑名单超过 5 天的股票自动恢复可交易。
3. 清理待补卖池：若某只股票已经不在持仓中，则从 pending_exit_stocks 中移除。
4. 维护动态观察池 g.watch_pool：
    - 如果观察池中的某个股票今天突然变成了 ST\*ST\名称含“退”，则直接淘汰。
    - 每天 age + 1。
    - age > 15 的股票直接淘汰，说明突破后迟迟未完成反包确认。
    - 若昨日出现近似主板跌停的极弱阴线，则淘汰。
    - 若阴线洗盘的成交量超过此前突破阳线最大量，视为主力异常出货，淘汰。
    - 若阳线继续上涨且放量，则更新 max_raise_vol；若阴线但量能仍健康，则更新 max_drop_vol。
5. 对全市场主板股票做新一轮突破扫描，候选条件为：
    - 最近收盘价满足多头排列：close > MA5 > MA10。
    - 最近一日价格足够强：相对当日开盘涨幅 > 4%，或相对前一日收盘涨幅 > 4%。
    - 最近一日成交量显著放大：量能 Z-Score > 1.2，且高于前一日成交量。
6. 满足条件的股票注入观察池，并记录：
    - breakout_date：突破识别日期。
    - max_raise_vol：突破阳线阶段观察到的最大阳线量能。
    - max_drop_vol：洗盘阶段观察到的最大阴线量能。
    - age：进入观察池后的存续天数。
7. 观察池最大容量限制为 500 只。

- market_open 开盘风险拦截与补卖处理：
    - 拦截开盘直接跌停的持仓：若今日开盘即跌停，立即挂跌停价卖单。
    - 处理历史待补卖池 pending_exit_stocks：
        - 若依然跌停开盘：继续挂跌停价卖单，“死磕”流动性。
        - 若已打开跌停：暂不操作，留待 14:57 进行“收复判定”。

- market_intraday 分钟级盘中买卖 & 持仓 
1. 仓位管理：保持30%现金，如果不足，则不再触发新的买入，等待下一次机会；且单个股票在单次建仓周期内最多只买一次、总买入金额不超过 10w RMB。
2. 指数趋势过滤：只有在沪深 300 最近收盘仍位于其 60 日均值上方时，才允许盘中继续触发买点。
3. 买
    - 盘中每个 bar 都会检查观察池中的候选股，而不是等到 14:57 尾盘确认后再排队到第二天买入。
    - 只检查观察池中 age >= 1 的股票，即突破当天不买，至少等待一天确认洗盘行为。
    - 个股满足以下条件即可在当天盘中直接触发买入：
        - 昨天必须是阴线：确认前一天发生了洗盘。
        - 今天盘中最新价格必须超过昨天开盘价：视为洗盘后的重新转强。
        - 必须仍然保持 MA5 > MA10
    - 一旦触发买入，策略会按当时可用现金对当批触发股票做等权分配，但单个股票实际买入金额会被限制在 10w 以内。
    - 每个股票在一次持仓周期内只允许开仓一次；买入后进入持仓锁定状态，直到全部清仓前都不允许再次买入。
    - 买入后股票会从观察池删除，避免同一天重复触发。
4. 持仓风控
    - 规则卖出：对于突然某天变成 ST、*ST、名称含“退”的股票，开盘后立即按市价卖出，当天必须完成清仓。
    - 对已经进入待补卖池的股票，在 14:57 执行“锁死收复判定”：
        - 若当前价格同时收复了“锁死价”且突破了 MA20：视为转强，移出补卖池，恢复正常持仓。
        - 否则：立即市价止损出场，以防再次锁死。
    - 对普通持仓，策略会计算：
        - 14:57 附近的盘中价格。
        - 最近 20 日均线 MA20。
        - 当前总收益率 total_pnl_ratio。
        - 历史最高浮盈 max_pnl。
    - 卖出规则分为三层，都是以14:57价格为基准进行判断，满足条件则当天盘中分单挂单：
        - 移动止盈：
            - 若最高浮盈 < 10%，不触发移动止盈，继续持有。
            - 若最高浮盈在 10% 到 30% 之间，则回撤到最高浮盈的 70% 触发止盈。
            - 若最高浮盈 > 30%，则回撤到最高浮盈的 60% 触发止盈。
            - 每次触发移动止盈时，若当前可卖仓位不少于 500 股，则先卖出 1/2；若当前可卖仓位小于 500 股，则直接全部卖出。
            - 若本次只是部分止盈而非全平，则卖出后保留原始买入成本，但将剩余仓位的“最高浮盈”重新统计。
            - 移动止盈属于“同日最多一次”的减仓动作，策略强制“个股同日卖出保护”：一旦个股在盘中触发任何卖出（止盈/止损），当日后续分钟 bar 将不再对该股执行任何重复卖出逻辑。
        - 刚性止损：若亏损达到 5%，触发挂单清仓。
        - 趋势止损：
            - 若 14:57 价格跌破 20 日均线，触发挂单清仓。
            - 若 14:57 发现MA20 > MA5 或者 MA20 > MA10，说明趋势已经转弱，也触发挂单清仓。
    - 卖出后执行善后：
        - 个股加入黑名单，避免短期反复踩雷。
        - 清理买入成本和最高收益缓存。
        - 若本次属于亏损卖出，则连续亏损计数 +1；否则清零。
        - 当连续亏损次数达到 5 次时，触发策略熔断，冻结买入 8 个交易日。

- after_market_close 盘后异常审计：复盘今日卖出单，若发现因跌停等原因导致“撤单/拒绝”且仍有持仓，则记录锁定价格并加入待补卖池。

- 策略流程 UML
```mermaid
flowchart TD
    A[盘前启动] --> B{策略是否处于熔断冻结期}
    B -- 是 --> B1[减少冻结天数并停止当日买入]
    B -- 否 --> C[清理黑名单与待补卖池]
    C --> D1{观察池个股是否变为ST/退}
    D1 -- 是 --> D1a[直接淘汰该标的]
    D1 -- 否 --> D2[age+1 / 淘汰超龄/跌停/异常砸盘标的]
    D2 --> D3[更新突破量能及洗盘量能]
    D1a --> D3
    D3 --> E[扫描全市场主板突破股]
    E --> F[满足突破条件则加入观察池]
    F --> MO[开盘：拦截新跌停股及执行历史跌停股死磕挂单]
    MO --> G[盘中每分钟执行 market_intraday]
    G --> H{沪深300趋势是否允许交易}
    H -- 否 --> H1[停止当日盘中选股]
    H -- 是 --> I{昨阴线且今价上破昨开且MA5>MA10}
    I -- 否 --> I1[继续等待或观察池自然淘汰]
    I -- 是 --> J[当日盘中单次买入且单股不超10w]
    J --> K[记录买入成本和最高浮盈]
    K --> L[14:57 检查持仓风控]
    L --> PS{补卖池是否收复锁死价与MA20}
    PS -- 是 --> PS1[移出补卖池恢复正常持有]
    PS -- 否 --> PS2[市价强行清仓]
    PS1 --> N
    PS2 --> N
    L --> N{个股是否满足常规卖出条件}
    N -- 否 --> O[继续持有]
    N -- 移动止盈 --> P1[按次/半仓止盈并重置运行状态]
    P1 --> Q[止盈/止损善后：加黑名单/计亏损/清缓存]
    N -- 跌破MA20/反向金叉/刚性止损 --> P3[全仓清出]
    P3 --> Q
    Q --> R{卖出单是否因跌停失败且仍有实仓}
    R -- 否 --> S[正常结束]
    R -- 是 --> T[记录锁死价入补卖池]
    T --> MO
```
"""
from jqdata import *
import numpy as np
import pandas as pd

MAX_TRADE_VALUE = 100000
MAX_BUYS_PER_DAY = 5


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

    # 强制取整，避免浮点数精度导致 0 股卖单
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
            # 处理不足一手碎股，一并打包卖出或买入
            if (remaining_amount - chunk_amount) > 0 and (remaining_amount - chunk_amount) < 100:
                chunk_amount = remaining_amount

        order(security, direction * chunk_amount, style=order_style)
        remaining_amount -= chunk_amount

# ==================== 1. 框架初始化函数 ====================
def initialize(context):
    # 设置沪神300为基准
    set_benchmark('000300.XSHG')
    # 开启真实价格复权模式
    set_option('use_real_price', True)
    log.info('【趋势版本v1.0】启动...')

    # 主板标准交易佣金设置（自适应主板最低5元佣金）
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    # 全局变量
    g.buy_cost_dict = {}          # 记录个股真实买入成本 {stock: price}
    g.max_pnl_dict = {}           # 记录个股持仓最高收益率（用于移动止盈）
    g.blacklist_dict = {}         # 黑名单，防止短期重复踩雷 {stock: delete_date}
    g.consecutive_loss_count = 0  # 连续亏损计数器
    g.freeze_days_left = 0        # 策略熔断剩余天数        

    g.watch_pool = {}             # 全局动态观察池字典 {stock: {info}}
    g.position_lock_stocks = set() # 已开仓未完全退出的股票，持仓周期内禁止再次买入
    g.trailing_stop_last_date = {} # 记录个股最近一次卖出触发日期，防止同日重复卖出
    g.today_buy_count = 0        # 记录当日已买入股票数量

    # 跟踪当天盘中买入但尚未获得日线确认的股票（用于次日开盘按市价强制卖出）
    g.intraday_unconfirmed_buys = set()
    # 次日一开盘需要强制卖出的股票集合（由盘后审计决定）
    g.next_open_forced_sell = set()

    g.is_market_safe = False     # 【高频缓存】记录当日大盘趋势是否安全
    g.watch_pool_static = {}     # 【高频缓存】存储观察池个股的历史截面静态相，避免盘中高频请求

    # ------------------ 🛠️ 核心修复注入：死磕防死锁全局池 ------------------
    # 记录由于跌停导致未能成功止盈止损的股票池 { 股票代码: 触发原因 }
    g.pending_exit_stocks = {} 
    # ------------------------------------------------------------------

    # 注册四大核心定时任务（无缝追加 15:30 盘后坏账审计）
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
    run_daily(market_open, time='open', reference_security='000300.XSHG')
    run_daily(market_intraday, time='every_bar', reference_security='000300.XSHG')
    run_daily(after_market_close, time='15:30', reference_security='000300.XSHG')


# ==================== 2. 🔩 核心过滤：构建非ST、非创业科创的主板池 ====================
def get_main_board_pool(context):
    """
    动态筛出当天全市场：纯沪深主板 + 非ST + 非停牌 + 上市满180天 的正常交易股票
    """
    current_date = context.current_dt.date()
    # 1. 获取当天全市场所有股票
    all_stocks = list(get_all_securities(['stock'], date=current_date).index)
    
    # 2. 获取当天所有股票的快照数据（用于过滤ST和停牌）
    current_data = get_current_data()
    
    main_board_stocks = []
    for stock in all_stocks:
        # 过滤创业板 (300, 301) 和科创板 (688) 以及北交所 (8, 4)
        if stock.startswith('300') or stock.startswith('301') or stock.startswith('688') or stock.startswith('8') or stock.startswith('4'):
            continue
            
        # 过滤 ST、*ST、退市股
        if current_data[stock].is_st or 'ST' in current_data[stock].name or '退' in current_data[stock].name:
            continue
            
        # 过滤当天停牌股
        if current_data[stock].paused:
            continue
            
        # 过滤上市不足 180 天的新股（防止次新股回测失真）
        info = get_security_info(stock)
        if info is not None:
            if (current_date - info.start_date).days < 180:
                continue
                
        main_board_stocks.append(stock)
        
    return main_board_stocks


# ==================== 3. ☀️ 盘前：全市场股票池滚动维护与突破日扫描 ====================
def before_market_open(context):
    current_date = context.current_dt.date()
    g.today_buy_count = 0

    # 3.1 策略整体风控：检查策略级别熔断状态
    if g.freeze_days_left > 0:
        g.freeze_days_left -= 1
        log.warn(f"🚨 策略处于整体亏损熔断保护中，剩余 {g.freeze_days_left} 天不进行任何买入。")
        return

    # 3.2 清理黑名单（满5天的个股自动移出黑名单）
    for stock in list(g.blacklist_dict.keys()):
        if (current_date - g.blacklist_dict[stock]).days > 5:
            del g.blacklist_dict[stock]

    # ------------------ 🛠️ 核心修复注入：盘前自动清洗死磕池 ------------------
    for stock in list(g.pending_exit_stocks.keys()):
        if stock not in context.portfolio.positions:
            g.pending_exit_stocks.pop(stock, None)

    for stock in list(g.position_lock_stocks):
        if stock not in context.portfolio.positions and stock not in g.pending_exit_stocks:
            g.position_lock_stocks.discard(stock)

    for stock in list(g.trailing_stop_last_date.keys()):
        if stock not in context.portfolio.positions and stock not in g.pending_exit_stocks:
            g.trailing_stop_last_date.pop(stock, None)
    # ------------------------------------------------------------------

    # 3.3 【池内滚动】遍历当前动态观察池中的股票，审查昨日洗盘表现
    current_data_watch = get_current_data()
    for stock in list(g.watch_pool.keys()):
        info = g.watch_pool[stock]

        # 淘汰突变 ST / *ST / 退市股
        if current_data_watch[stock].is_st or 'ST' in current_data_watch[stock].name or '退' in current_data_watch[stock].name:
            del g.watch_pool[stock]
            log.info(f"🗑️ 观察池淘汰 ST/退市股 -> {stock}")
            continue

        info['age'] += 1
        
        # 淘汰过期的股票
        if info['age'] > 15:
            del g.watch_pool[stock]
            continue
            
        # 提取昨日及前日的真实日K线数据
        hist_1d = get_bars(stock, count=2, unit='1d', fields=['open', 'close', 'volume'], include_now=False)
        if hist_1d is not None and len(hist_1d) >= 2:
            t2_close = hist_1d['close'][0] 
            t1_open, t1_close, t1_vol = hist_1d['open'][1], hist_1d['close'][1], hist_1d['volume'][1] 
            
            # 主板跌停限制判定（主板跌幅 >= 9.9% 近似跌停）
            if (t1_close <= t1_open) and ((t1_close / t2_close) <= 0.901):
                del g.watch_pool[stock]
                continue
                
            # 动态监控砸盘量能
            if t1_close >= t1_open:
                if t1_vol > info['max_raise_vol']: 
                    info['max_raise_vol'] = t1_vol
            else:
                # 阴线砸盘量超越了突破大阳线的量，属于主力异常砸盘，直接淘汰
                if t1_vol > info['max_raise_vol']:
                    del g.watch_pool[stock]
                else:
                    info['max_drop_vol'] = max(info['max_drop_vol'], t1_vol)

    # 3.4 【全市场主板扫描】寻找符合多头突破大阳线的标的注入池子
    main_board_pool = get_main_board_pool(context)
    
    # 限制池子最大承载量，防止内存泄露和过度分散
    if len(g.watch_pool) < 500:
        for stock in main_board_pool:
            # 已经在观察池或已持仓或在黑名单的，不再重复扫描
            if stock in g.watch_pool or stock in context.portfolio.positions or stock in g.blacklist_dict or stock in g.position_lock_stocks:
                continue
                
            # 批量获取个股历史K线进行技术指标计算
            df = get_bars(stock, count=40, unit='1d', fields=['open', 'close', 'volume'], include_now=False)
            if len(df) < 35: continue
            
            last_close, last_open, last_vol = df['close'][-1], df['open'][-1], df['volume'][-1]
            ma5, ma10, ma20 = df['close'][-5:].mean(), df['close'][-10:].mean(), df['close'][-20:].mean()
            
            # 计算成交量 Z-Score 爆量系数
            vol_series = df['volume'][-22:-1]
            z_score = (last_vol - vol_series.mean()) / vol_series.std() if vol_series.std() > 0 else 0
            
            # 多头排列形态 + 主板放量大阳线(涨幅>4%)
            is_ma_ok = last_close > ma5 > ma10
            is_price_ok = (last_close / last_open > 1.04) or (last_close / df['close'][-2] > 1.04) 
            is_vol_ok = z_score > 1.2 and last_vol > df['volume'][-2]
            
            if is_ma_ok and is_price_ok and is_vol_ok:
                g.watch_pool[stock] = {
                    'breakout_date': current_date,
                    'max_raise_vol': last_vol,  
                    'breakout_price': max(last_open, last_close), 
                    'age': 0,
                    'max_drop_vol': 0.0  
                }
                log.info(f"🎯 突破注入主板池 -> {stock} (Z-Score: {z_score:.2f}, 基准价: {g.watch_pool[stock]['breakout_price']:.2f})")
                if len(g.watch_pool) >= 500: # 控量保护
                    break

    # ====== 新增：盘前预计算缓存，避免盘中每分钟重复请求K线致使系统限流卡死 ======
    index_data = get_bars('000300.XSHG', count=60, unit='1d', fields=['close'], include_now=False)
    if index_data is not None and len(index_data) > 0:
        g.is_market_safe = index_data['close'][-1] >= index_data['close'].mean()
    else:
        g.is_market_safe = False

    g.watch_pool_static = {}
    for stock in list(g.watch_pool.keys()):
        hist = get_bars(stock, count=20, unit='1d', fields=['open', 'close'], include_now=False)
        if hist is not None and len(hist) >= 19:
            g.watch_pool_static[stock] = {
                't1_open': hist['open'][-1],
                't1_close': hist['close'][-1],
                # 如果要推算实时的 ma5, ma10，只需要存下昨天之前的 4天和9天求和基数即可
                'sum_4': hist['close'][-4:].sum(),
                'sum_9': hist['close'][-9:].sum()
            }


# ==================== 4. 📈 开盘补卖处理 ====================
def market_open(context):
    current_data = get_current_data()
    current_date = context.current_dt.date()
    # 次日一开盘：优先处理前一日盘中买入但日线未确认的强制卖出列表
    if getattr(g, 'next_open_forced_sell', None):
        for security in list(g.next_open_forced_sell):
            if security in context.portfolio.positions:
                pos = context.portfolio.positions[security]
                amount = pos.closeable_amount
                if amount > 0:
                    log.warn(f"🛑 次日开盘强制清仓(无日线确认) -> {security} , 卖出数量: {amount}")
                    # 市价卖出（不使用限价），尽快离场
                    order_amount_in_chunks(security, -amount, None)
                    g.trailing_stop_last_date[security] = current_date
            # 无论是否仍持有，清理该标记
            g.next_open_forced_sell.discard(security)
    
    # 1. 如果开盘跌停，直接挂跌停卖
    for security in list(context.portfolio.positions.keys()):
        data = current_data[security]
        pos = context.portfolio.positions[security]
        
        # 判断开盘是否跌停
        if data.day_open <= data.low_limit:
            # 去重：防止因为同日多次触发跌停而重复挂单
            if g.trailing_stop_last_date.get(security) == current_date:
                continue

            amount = pos.closeable_amount
            if amount > 0:
                log.error(f"⚠️ [开盘风险拦截] 股票 {security} 今日开盘即跌停！挂跌停价限价单卖出 {amount} 股...")
                order_amount_in_chunks(security, -amount, data.low_limit, is_limit=True)
                g.trailing_stop_last_date[security] = current_date
            
            # 2. 如果因为跌停卖不出去的，记录 locked_price 进入 pending_exit_stocks
            if security not in g.pending_exit_stocks:
                g.pending_exit_stocks[security] = data.low_limit

    # ------------------ 🛠️ 核心修复注入：历史死磕池优先处理 ------------------
    if g.pending_exit_stocks:
        log.warning(f"🔄 [待补卖池扫描] 正在检查 {len(g.pending_exit_stocks)} 只历史锁死标的...")
        for security in list(g.pending_exit_stocks.keys()):
            if security not in context.portfolio.positions:
                g.pending_exit_stocks.pop(security, None)
                continue
                
            data = current_data[security]
            pos = context.portfolio.positions[security]
            
            # 同样去重，如果在上方已被处理则跳过
            if g.trailing_stop_last_date.get(security) == current_date:
                continue

            # 如果依然以跌停开盘，继续尝试挂跌停价卖出
            if data.day_open <= data.low_limit:
                amount = pos.closeable_amount
                if amount > 0:
                    log.error(f"🎯 股票 {security} 今日依然跌停开盘。继续挂跌停价限价卖单，数量: {amount}")
                    order_amount_in_chunks(security, -amount, data.low_limit, is_limit=True)
                    g.trailing_stop_last_date[security] = current_date
            else:
                log.info(f"ℹ️ 股票 {security} 今日已打开跌停开盘，将观察至 14:57 决定是否留活口...")

    # 规则卖出：持仓股突变 ST / *ST / 退市时开盘强制清仓
    for security in list(context.portfolio.positions.keys()):
        if security in g.pending_exit_stocks:
            continue
        if current_data[security].is_st or 'ST' in current_data[security].name or '退' in current_data[security].name:
            amount = context.portfolio.positions[security].total_amount
            log.error(f"⚠️ 持仓股 {security} 今日变为ST/退，开盘强制清仓，数量: {amount}")
            reference_price = current_data[security].day_open or current_data[security].last_price
            order_amount_in_chunks(security, -amount, reference_price)


# ==================== 5. ⏱️ 分钟级盘中买卖与持仓风控 ====================
def market_intraday(context):
    current_dt = context.current_dt
    current_date = current_dt.date()
    current_time = current_dt.strftime('%H:%M')
    current_data = get_current_data()

    # 5.1 买入逻辑：保留 30% 现金，仅用剩余现金参与盘中触发
    buy_budget = context.portfolio.available_cash - context.portfolio.total_value * 0.30

    if len(g.watch_pool) > 0 and buy_budget > 10000 and g.today_buy_count < MAX_BUYS_PER_DAY:
        # 优化：大盘趋势缓存读取（摒弃盘中分钟级别请求极度耗时逻辑）
        if getattr(g, 'is_market_safe', False):
            triggered_stocks = []

            # 循环遍历当前动态观察池内的所有主板个股
            for stock in list(g.watch_pool.keys()):
                # 检查限额
                if g.today_buy_count + len(triggered_stocks) >= MAX_BUYS_PER_DAY:
                    break

                info = g.watch_pool[stock]
                if info['age'] < 1:
                    continue  # 突破日当天不买

                if stock in g.position_lock_stocks or stock in context.portfolio.positions:
                    continue

                # 读取昨天的静态截面数据
                static_info = getattr(g, 'watch_pool_static', {}).get(stock)
                if not static_info:
                    continue

                # 提取缓存的数据，t1_open 和 t1_close 是昨天的开盘价和收盘价
                t1_open = static_info['t1_open']
                t1_close = static_info['t1_close']
                
                # 盘中只需要获取当下的市价即可，不再每次都取几十天前的数据求 MA
                current_price = current_data[stock].last_price
                open_price = current_data[stock].day_open

                if np.isnan(current_price) or current_price <= 0:
                    continue

                # 动态计算今日的有效 MA5 / MA10
                ma5 = (static_info['sum_4'] + current_price) / 5
                ma10 = (static_info['sum_9'] + current_price) / 10

                # 🛑 检查1：昨天必须是真阴线（确认已发生洗盘）
                if t1_close >= t1_open:
                    continue

                # 🛑 检查2：今日开盘价格低于实时价格，且实时价格上破昨日开盘价（反包确认）
                if current_price < open_price or current_price < t1_open:
                    continue
                
                # 🛑 检查3：价格区间重合校验。今日价格须大于突破日基准价（突破日开/收最大值）
                # 确保反包发生在多头有效范围内，而不是在深跌后的无效反弹
                breakout_ref = info.get('breakout_price', 0)
                if current_price < breakout_ref:
                    continue
                
                # 🛑 检查4：趋势保鲜校验。买入时刻至少维持 MA5 > MA10
                if not (ma5 > ma10):
                    continue

                triggered_stocks.append((stock, current_price, t1_open))

            if len(triggered_stocks) > 0:
                cash_per_stock = buy_budget / len(triggered_stocks)

                for stock, current_price, yesterday_open in triggered_stocks:
                    ordered_amount = order_buy_once(stock, cash_per_stock, current_price)
                    if ordered_amount >= 100:
                        est_value = ordered_amount * current_price
                        log.info(f"🛒【盘中买入触发】{stock} 当前价 {current_price:.2f} 上破昨开 {yesterday_open:.2f}，计划金额: {cash_per_stock:.2f}，实际单次买入 {ordered_amount} 股，参考市值: {est_value:.2f}")
                        g.buy_cost_dict[stock] = current_price
                        g.max_pnl_dict[stock] = 0.0
                        g.position_lock_stocks.add(stock)
                        # 标记为盘中买入但尚未获得日线收盘确认的票
                        g.intraday_unconfirmed_buys.add(stock)
                        g.today_buy_count += 1
                        del g.watch_pool[stock]

    # 5.2 持仓风控：仅在 14:57 及之后用盘中价格触发卖出
    if current_time < '14:57':
        return

    # ------------------ 🛠️ 核心修复注入：历史锁死股 14:57 生死劫 ------------------
    if g.pending_exit_stocks:
        for security in list(g.pending_exit_stocks.keys()):
            if security not in context.portfolio.positions:
                g.pending_exit_stocks.pop(security, None)
                continue
            
            # 若今日早盘已挂跌停单清仓过，可跳过本处理
            if g.trailing_stop_last_date.get(security) == current_date:
                continue

            data = current_data[security]
            # 只有在目前非跌停封死的情况下才进行观察
            if data.last_price > data.low_limit:
                # 修复: 均线必须 include_now=True 才会算上今天的真实反弹价格
                hist_20d = get_bars(security, count=20, unit='1d', fields=['close'], include_now=True)
                if len(hist_20d) < 20: continue
                ma20 = hist_20d['close'].mean()
                
                locked_price = g.pending_exit_stocks[security]
                
                # 判定：收复锁死价格 并且 收复 MA20 (改进点4)
                if data.last_price >= locked_price and data.last_price >= ma20:
                    log.info(f"✅ [锁死收复] {security} 当前价 {data.last_price:.2f} 成功收复锁死价 {locked_price:.2f} 且突破 MA20({ma20:.2f})，剔除出补卖池，恢复正常持仓状态。")
                    g.pending_exit_stocks.pop(security, None)
                else:
                    log.error(f"❌ [锁死未收复] {security} 14:57 未能收复锁死价 {locked_price:.2f} 或 MA20({ma20:.2f})，立即挂限价单清仓...")
                    amount = context.portfolio.positions[security].closeable_amount
                    if amount > 0:
                        order_amount_in_chunks(security, -amount, data.last_price, is_limit=True)
                        g.trailing_stop_last_date[security] = current_date
                    g.pending_exit_stocks.pop(security, None)

    current_positions = context.portfolio.positions
    for security in list(current_positions.keys()):
        if security in g.pending_exit_stocks:
            continue
            
        # 预防同日重复卖出（同一只股票在 14:57-15:00 之间只能触发一次卖出逻辑）
        if g.trailing_stop_last_date.get(security) == current_date:
            continue

        position = current_positions[security]
        if position.closeable_amount == 0:
            continue

        # 修改为 include_now=True，因为此时为14:57，判断当下的动态均线才具有意义
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
        reason = ""
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
                    reason = f"14:57利润回撤触发阶梯止盈，剩余仓位不足500股全部卖出(最高浮盈:{max_pnl*100:.2f}%)"
                else:
                    sell_amount = int((position.closeable_amount / 2) // 100) * 100
                    if sell_amount <= 0:
                        sell_amount = position.closeable_amount
                        full_exit = True
                        reason = f"14:57利润回撤触发阶梯止盈，仓位不足一手改为全部卖出(最高浮盈:{max_pnl*100:.2f}%)"
                    else:
                        reason = f"14:57利润回撤触发阶梯止盈，先卖出半仓(最高浮盈:{max_pnl*100:.2f}%)"
        elif total_pnl_ratio <= -0.05:
            should_sell = True
            is_loss = True
            sell_amount = position.closeable_amount
            full_exit = True
            reason = "触及-5%刚性止损线"
        elif current_price < ma20:
            should_sell = True
            is_loss = True
            sell_amount = position.closeable_amount
            full_exit = True
            reason = "14:57价格跌破20日均线"
        elif ma20 > ma5 or ma20 > ma10:
            should_sell = True
            is_loss = True
            sell_amount = position.closeable_amount
            full_exit = True
            reason = f"MA20({ma20:.2f})>MA5({ma5:.2f})或MA10({ma10:.2f})，均线趋势转弱"

        if should_sell:
            log.warn(f"🚨【盘中触发卖出】股票: {security}, 原因: {reason}，当前盈亏: {total_pnl_ratio*100:.2f}%")
            order_amount_in_chunks(security, -sell_amount, current_price)

            # 标记今日已卖出
            g.trailing_stop_last_date[security] = current_date

            if should_sell and (not full_exit) and (not is_loss):
                g.max_pnl_dict[security] = max(0.0, total_pnl_ratio)

            if full_exit:
                if is_loss:
                    g.consecutive_loss_count += 1
                    if g.consecutive_loss_count >= 5:
                        g.freeze_days_left = 8
                        log.error("💥💥💥 策略整体连续亏损5次，触发熔断保护，空仓面壁8个交易日！")
                else:
                    g.consecutive_loss_count = 0

                g.blacklist_dict[security] = current_date
                if security in g.buy_cost_dict:
                    del g.buy_cost_dict[security]
                if security in g.max_pnl_dict:
                    del g.max_pnl_dict[security]
                g.trailing_stop_last_date.pop(security, None)
                g.position_lock_stocks.discard(security)


# ==================== 6. 🌆 盘后审计（15:30）：日频死锁核心拦截阀门 ====================
def after_market_close(context):
    """
    【新增防死锁逻辑】每天收盘后自动执行。
    复盘今日发出的全部减仓单，一击必杀定位由于跌停流动性枯竭导致的未成交废单。
    """
    todays_orders = get_orders()
    if not todays_orders: return
    
    for order_id, order_obj in todays_orders.items():
        # 我们只清查用于止盈止损卖出(close)的订单
        if order_obj.action == 'close':
            # 如果订单在日内被判定为已取消或被拒绝，代表市价冲击遇阻
            if order_obj.status.name in ['canceled', 'rejected']:
                security = order_obj.security
                # 铁证如山：如果收盘了这只股票居然还在实际持仓清单里
                if security in context.portfolio.positions:
                    if security not in g.pending_exit_stocks:
                        current_data = get_current_data()
                        g.pending_exit_stocks[security] = current_data[security].low_limit
                        log.error(f"🚨 [日频死锁拦截] 发现持仓股 {security} 今日触发常规减仓却未能离场！收盘依然有实仓。跌停锁死价: {current_data[security].low_limit}")
                        log.error(f"该股已被强行拖入【每日死磕补单池】，明日（包含下周一）开盘第一分钟自动按市价继续卖出！")
    # ====== 盘后审计：评估当天盘中买入但未获日线确认的个股，若收盘日线未能收复前日则次日强制开盘卖出 ======
    if getattr(g, 'intraday_unconfirmed_buys', None):
        for stock in list(g.intraday_unconfirmed_buys):
            hist = get_bars(stock, count=1, unit='1d', fields=['open', 'close'], include_now=True)
            # 若今日收盘价格低于开盘价格，则认为盘中未获确认，次日开盘以市价清仓
            today_open = hist['open'][-1]
            today_close = hist['close'][-1]
            if today_close < today_open:
                g.next_open_forced_sell.add(stock)
                log.warn(f"📝 盘后判定：{stock} 今日收盘 {today_close:.2f} 低于开盘 {today_open:.2f}，次日开盘将强制市价清仓(因盘中买入未获确认)。")
        
        # 当天的盘后审计完成后，必须清空该集合，避免影响次日逻辑
        g.intraday_unconfirmed_buys.clear()