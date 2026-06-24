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
    - 每天 age + 1。
    - age > 15 的股票直接淘汰，说明突破后迟迟未完成反包确认。
    - 若昨日出现近似主板跌停的极弱阴线，则淘汰。
    - 若阴线洗盘的成交量超过此前突破阳线最大量，视为主力异常出货，淘汰。
    - 若阳线继续上涨且放量，则更新 max_raise_vol；若阴线但量能仍健康，则更新 max_drop_vol。
5. 对全市场主板股票做新一轮突破扫描，候选条件为：
    - 最近收盘价满足多头排列：close > MA5 > MA10 > MA20。
    - 最近一日价格足够强：相对当日开盘涨幅 > 4%，或相对前一日收盘涨幅 > 4%。
    - 最近一日成交量显著放大：量能 Z-Score > 1.2，且高于前一日成交量。
6. 满足条件的股票注入观察池，并记录：
    - breakout_date：突破识别日期。
    - max_raise_vol：突破阳线阶段观察到的最大阳线量能。
    - max_drop_vol：洗盘阶段观察到的最大阴线量能。
    - age：进入观察池后的存续天数。
7. 观察池最大容量限制为 50 只，避免资金和注意力过度分散。

- market_open 开盘先优先处理 pending_exit_stocks：
    - 这些股票是前一日触发卖出，但因为跌停或流动性枯竭未卖掉的持仓。
    - 策略会在次日开盘直接按市价分单挂单，优先解决风险暴露，而不是继续做新交易。

- market_intraday 分钟级盘中买卖 & 持仓 
1. 仓位管理：保持30%现金，如果不足，则不再触发新的买入，等待下一次机会；且单个股票在单次建仓周期内最多只买一次、总买入金额不超过 10w RMB。
2. 指数趋势过滤：只有在沪深 300 最近收盘仍位于其 60 日均值上方时，才允许盘中继续触发买点。
3. 买
    - 盘中每个 bar 都会检查观察池中的候选股，而不是等到 14:57 尾盘确认后再排队到第二天买入。
    - 只检查观察池中 age >= 1 的股票，即突破当天不买，至少等待一天确认洗盘行为。
    - 个股满足以下条件即可在当天盘中直接触发买入：
        - 昨天必须是阴线：确认前一天发生了洗盘。
        - 今天盘中最新价格必须超过昨天开盘价：视为洗盘后的重新转强。
    - 一旦触发买入，策略会按当时可用现金对当批触发股票做等权分配，但单个股票实际买入金额会被限制在 10w 以内。
    - 每个股票在一次持仓周期内只允许开仓一次；买入后进入持仓锁定状态，直到全部清仓前都不允许再次买入。
    - 买入后股票会从观察池删除，避免同一天重复触发。
4. 持仓风控
    - 对已经进入 pending_exit_stocks 的股票，不再重复执行常规止盈止损，避免重复报单。
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
            - 策略强制“个股同日卖出保护”：一旦个股在盘中触发任何卖出（止盈/止损），当日后续分钟 bar 将不再对该股执行任何重复卖出逻辑。
            - 刚性止损：若亏损达到 5%，触发挂单卖出。
            - 趋势止损：若 14:57 价格跌破 20 日均线，触发挂单卖出。
    - 卖出后执行善后：
        - 个股加入黑名单，避免短期反复踩雷。
        - 清理买入成本和最高收益缓存。
        - 若本次属于亏损卖出，则连续亏损计数 +1；否则清零。
        - 当连续亏损次数达到 5 次时，触发策略熔断，冻结买入 8 个交易日。
    - 额外约束：移动止盈属于“同日最多一次”的减仓动作；若当天已经触发过移动止盈，则当天后续分钟 bar 不再重复触发，需等到下一交易日才允许再次因为移动止盈而减仓。

- after_market_close 盘后异常审计 
1. 策略会复盘当天全部订单，重点检查卖出单（action == 'close'）。
2. 如果卖出单状态为 canceled 或 rejected，且收盘后该股票仍在实际持仓中，则认定为“卖出死锁”。
3. 死锁股票会被加入 g.pending_exit_stocks，等待下一交易日开盘继续按市价排队卖出。
4. 这一机制主要解决日内卖出指令发出后，由于跌停封单或流动性枯竭造成的未成交问题。

- 策略流程 UML
```mermaid
flowchart TD
    A[盘前启动] --> B{策略是否处于熔断冻结期}
    B -- 是 --> B1[减少冻结天数并停止当日买入]
    B -- 否 --> C[清理黑名单与待补卖池]
    C --> D[维护观察池已有标的]
    D --> E[扫描全市场主板突破股]
    E --> F[满足突破条件则加入观察池]
    F --> G[盘中每分钟执行 market_intraday]
    G --> H{沪深300趋势是否允许交易}
    H -- 否 --> H1[停止当日盘中选股]
    H -- 是 --> I{昨阴线且今价上破昨开}
    I -- 否 --> I1[继续保留或淘汰观察池标的]
    I -- 是 --> J[当日盘中单次买入且单股不超10w]
    J --> K[记录买入成本和最高浮盈]
    K --> L[14:57 检查持仓止盈止损]
    L --> N{是否触发卖出}
     N -- 否 --> O[继续持有]
    N -- 是 --> P[分单提交卖出单]
    P --> P1{是否为移动止盈}
    P1 -- 是 --> P2[记录当日已触发移动止盈]
    P2 --> P3[当天剩余分钟不再重复止盈]
    P3 --> Q
    P1 -- 否 --> Q
     Q --> R{卖出是否失败且仍有持仓}
     R -- 否 --> S[正常结束]
    R -- 是 --> T[加入待补卖池]
    T --> U[次日开盘按市价分单继续卖出]
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


def order_amount_in_chunks(security, total_amount, reference_price):
    if total_amount == 0:
        return

    if reference_price is None or np.isnan(reference_price) or reference_price <= 0:
        order(security, total_amount)
        return

    max_chunk_amount = int(MAX_TRADE_VALUE / reference_price / 100) * 100
    if max_chunk_amount <= 0:
        order(security, total_amount)
        return

    direction = 1 if total_amount > 0 else -1
    remaining_amount = abs(int(total_amount))

    while remaining_amount > 0:
        if remaining_amount < 100:
            chunk_amount = remaining_amount
        else:
            chunk_amount = min(remaining_amount, max_chunk_amount)
            if chunk_amount < 100:
                order(security, direction * remaining_amount)
                return

        order(security, direction * chunk_amount)
        remaining_amount -= chunk_amount

# ==================== 1. 框架初始化函数 ====================
def initialize(context):
    # 设置沪神300为基准
    set_benchmark('000300.XSHG')
    # 开启真实价格复权模式
    set_option('use_real_price', True)
    log.info('【V6.5-全市场沪深主板·跌停修复全闭环版】启动...')

    # 主板标准交易佣金设置（自适应主板最低5元佣金）
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    # 全局变量初始化
    g.buy_cost_dict = {}          # 记录个股真实买入成本 {stock: price}
    g.max_pnl_dict = {}           # 记录个股持仓最高收益率（用于移动止盈）
    g.blacklist_dict = {}         # 黑名单，防止短期重复踩雷 {stock: delete_date}
    g.consecutive_loss_count = 0  # 连续亏损计数器
    g.freeze_days_left = 0        # 策略熔断剩余天数        

    g.watch_pool = {}             # 全局动态观察池字典 {stock: {info}}
    g.position_lock_stocks = set() # 已开仓未完全退出的股票，持仓周期内禁止再次买入
    g.trailing_stop_last_date = {} # 记录个股最近一次卖出触发日期，防止同日重复卖出
    g.today_buy_count = 0        # 记录当日已买入股票数量

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
    for stock in list(g.watch_pool.keys()):
        info = g.watch_pool[stock]
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
    
    # 限制池子最大承载量，防止内存泄泄露和过度分散（V6.5原版50只控制）
    if len(g.watch_pool) >= 50:
        return

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
        is_ma_ok = last_close > ma5 > ma10 > ma20
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
            if len(g.watch_pool) >= 50: # 控量保护
                break


# ==================== 4. 📈 开盘补卖处理 ====================
def market_open(context):
    current_data = get_current_data()
    
    # ------------------ 🛠️ 核心修复注入：开盘死磕排队优先处理 ------------------
    if g.pending_exit_stocks:
        log.warning(f"🔄 [开盘死磕激活] 发现有由于跌停未出货成功标的，今日开盘强行排队...")
        for security in list(g.pending_exit_stocks.keys()):
            if security not in context.portfolio.positions:
                g.pending_exit_stocks.pop(security, None)
                continue
                
            # 直接按市价重新卖出，优先处理未完成离场的风险暴露
            if security in current_data:
                amount = context.portfolio.positions[security].total_amount
                log.error(f"🎯 股票 {security} 上日清仓未成功。今日开盘按市价重新提交卖出单，数量: {amount}")
                reference_price = current_data[security].day_open or current_data[security].last_price
                order_amount_in_chunks(security, -amount, reference_price)
    # ----------------------------------------------------------------------


# ==================== 5. ⏱️ 分钟级盘中买卖与持仓风控 ====================
def market_intraday(context):
    current_dt = context.current_dt
    current_date = current_dt.date()
    current_time = current_dt.strftime('%H:%M')
    current_data = get_current_data()

    # 5.1 买入逻辑：保留 30% 现金，仅用剩余现金参与盘中触发
    buy_budget = context.portfolio.available_cash - context.portfolio.total_value * 0.30

    if len(g.watch_pool) > 0 and buy_budget > 10000 and g.today_buy_count < MAX_BUYS_PER_DAY:
        # 大盘中期趋势防守开关
        index_data = get_bars('000300.XSHG', count=60, unit='1d', fields=['close'], include_now=False)['close']
        if len(index_data) > 0 and index_data[-1] >= index_data.mean():
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

                # 获取近期日线数据（用于反包判定及实时均线校验）
                # 需 20+ 天数据计算 MA20，include_now=True 包含今日实时 bar
                hist_1d = get_bars(stock, count=25, unit='1d', fields=['open', 'close'], include_now=True)
                if len(hist_1d) < 21:
                    continue

                # 昨天的数据 (T-1)
                t1_open, t1_close = hist_1d['open'][-2], hist_1d['close'][-2]
                # 今日实时价格 (T)
                current_price = current_data[stock].last_price if stock in current_data else hist_1d['close'][-1]
                
                # 计算今日实时均线 (MA5/10/20)
                ma5_now = hist_1d['close'][-5:].mean()
                ma10_now = hist_1d['close'][-10:].mean()
                ma20_now = hist_1d['close'][-20:].mean()

                if np.isnan(current_price):
                    continue

                # 🛑 检查1：昨天必须是真阴线（确认已发生洗盘）
                if t1_close >= t1_open:
                    continue

                # 🛑 检查2：今日实时价格上破昨日开盘价（反包确认）
                if current_price <= t1_open:
                    continue
                
                # 🛑 检查3：价格区间重合校验。今日价格须大于突破日基准价（突破日开/收最大值）
                # 确保反包发生在多头有效范围内，而不是在深跌后的无效反弹
                breakout_ref = info.get('breakout_price', 0)
                if current_price < breakout_ref:
                    continue
                
                # 🛑 检查4：趋势保鲜校验。买入时刻均线必须依然维持多头排列
                if not (ma5_now > ma10_now > ma20_now):
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
                        g.today_buy_count += 1
                        del g.watch_pool[stock]

    # 5.2 持仓风控：仅在 14:57 及之后用盘中价格触发卖出
    if current_time < '14:57':
        return

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

        hist_data = get_bars(security, count=20, unit='1d', fields=['close'], include_now=False)
        if len(hist_data) < 20:
            continue

        current_price = current_data[security].last_price if security in current_data else np.nan
        if np.isnan(current_price):
            continue

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
                        g.pending_exit_stocks[security] = "V6.5风控单内遭遇极端跌停死锁"
                        log.error(f"🚨 [日频死锁拦截] 发现持仓股 {security} 今日触发常规减仓却未能离场！收盘依然有实仓。")
                        log.error(f"该股已被强行拖入【每日死磕补单池】，明日（包含下周一）开盘第一分钟自动按市价继续卖出！")