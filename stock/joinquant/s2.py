# -*- coding: utf-8 -*-
"""
策略名称：
箱体底部吸筹 + 突破上沿盘中介入短线策略

- 策略目标：寻找在箱体底部出现震荡吸筹特征的股票，并在盘中突破箱体上沿时直接买入。
1. 只做沪深主板，过滤创业板（300、301）、科创板（688）、北交所（8、4 开头）股票。
2. 过滤 ST、*ST、名称含“退”的股票。
3. 过滤当日停牌股票。
4. 过滤上市未满 180 天的新股，避免次新股波动与回测失真。

- before_market_open 盘前选股
1. 趋势背景判定：20日均线大于60日均线。
2. 箱体震荡判定：过去 20 天内最高价和最低价的波动振幅小于 15%。
3. 满足条件的股票记下其箱体上沿（high_max）并加入观察池（age最大为15天）。

- market_intraday 分钟级盘中买卖 & 持仓 
1. 买入条件（唯一买入条件）：盘中最新价格突破记录的箱体上缘（high_max）时，即刻触发买入，单股最高限额 10w。
2. 持仓风控：
    - 规则卖出：个股突变ST等，开盘强制清仓。跌停死磕补卖机制不变。
    - 移动止盈（14:57触发验证）：
        - 浮盈 10%~30% 之间，回撤至最高浮盈 70% 时触发（大于500股先平一半）。
        - 浮盈 > 30% 时，回撤至最高浮盈 60% 时触发。
    - 刚性止损：亏损达到 5% 挂单清仓。
    - 趋势止损：14:57 跌破 20日均线，或者 MA20 > MA5/MA10 时挂单清仓。
3. 善后防死锁：盘后审计若仍有实仓未走掉的跌停股，转入次日死磕池。
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
    由于开启单股调试，现在强制返回 000878.XSHE
    """
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
    # if g.freeze_days_left > 0:
    #     g.freeze_days_left -= 1
    #     log.warn(f"🚨 策略处于整体亏损熔断保护中，剩余 {g.freeze_days_left} 天不进行任何买入。")
    #     return

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
        hist_1d = get_bars(stock, count=2, unit='1d', fields=['open', 'close'], include_now=False)
        if hist_1d is not None and len(hist_1d) >= 2:
            t2_close = hist_1d['close'][0] 
            t1_open, t1_close = hist_1d['open'][1], hist_1d['close'][1]
            
            # 主板跌停限制判定（主板跌幅 >= 9.9% 近似跌停）
            if (t1_close <= t1_open) and ((t1_close / t2_close) <= 0.901):
                del g.watch_pool[stock]
                continue

    # 3.4 【全市场主板扫描】寻找符合箱体内震荡吸筹的标的注入池子
    main_board_pool = get_main_board_pool(context)
    
    # 限制池子最大承载量，防止内存泄露和过度分散
    if len(g.watch_pool) < 500:
        for stock in main_board_pool:
            if stock in g.watch_pool or stock in context.portfolio.positions or stock in g.blacklist_dict or stock in g.position_lock_stocks:
                continue
                
            df = get_bars(stock, count=65, unit='1d', fields=['open', 'close', 'high', 'low', 'volume'], include_now=False)
            if len(df) < 60: continue
            
            # 转换为 DataFrame 支持 pandas 方法
            df_pad = pd.DataFrame(df)
            
            # 趋势背景：20日均线上大于60日均线
            ma20_curr = df_pad['close'].iloc[-20:].mean()
            ma60_curr = df_pad['close'].iloc[-60:].mean()
            
            if ma20_curr <= ma60_curr:
                continue
                
            # 1. 箱体震荡判定 (过去 20 天内，最高价和最低价的波动小于 15%)
            lookback = 20
            high_max = df_pad['high'].iloc[-lookback:].max()
            low_min = df_pad['low'].iloc[-lookback:].min()
            
            if low_min <= 0: continue
            amplitude = (high_max - low_min) / low_min
            is_in_box = amplitude < 0.15 
            
            if is_in_box:
                g.watch_pool[stock] = {
                    'breakout_date': current_date,
                    'high_max': high_max,
                    'low_min': low_min,
                    'age': 0
                }
                log.info(f"🎯 确认箱体震荡吸筹 -> {stock} (箱体振幅: {amplitude*100:.2f}%, 上沿: {high_max:.2f})")
                if len(g.watch_pool) >= 500:
                    break

    # ====== 新增：盘前预计算缓存，避免盘中每分钟重复请求K线致使系统限流卡死 ======
    index_data = get_bars('000300.XSHG', count=60, unit='1d', fields=['close'], include_now=False)
    if index_data is not None and len(index_data) > 0:
        g.is_market_safe = index_data['close'][-1] >= index_data['close'].mean()
    else:
        g.is_market_safe = False

    g.watch_pool_static = {}
    for stock in list(g.watch_pool.keys()):
        hist = get_bars(stock, count=25, unit='1d', fields=['open', 'close'], include_now=False)
        if hist is not None and len(hist) >= 19:
            # 聚宽的 get_bars 返回的是 np.ndarray 或自定义对象，不一定是 pandas DataFrame
            # 为了使用 pandas 的强大序列运算，强制转换为 pd.Series
            closes = pd.Series(hist['close'].flatten() if hasattr(hist['close'], 'flatten') else hist['close'])
            
            diff = closes.diff()
            gain = diff.where(diff > 0, 0.0)
            loss = -diff.where(diff < 0, 0.0)
            
            g.watch_pool_static[stock] = {
                't1_open': hist['open'][-1],
                't1_close': closes.iloc[-1],
                # 如果要推算实时的 ma5, ma10，只需要存下昨天之前的 4天和9天求和基数即可
                'sum_4': closes.iloc[-4:].sum(),
                'sum_9': closes.iloc[-9:].sum(),
                'gain_sum_14': gain.iloc[-14:].sum(),
                'loss_sum_14': loss.iloc[-14:].sum()
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

                # 盘中获取当下的市价
                current_price = current_data[stock].last_price

                if np.isnan(current_price) or current_price <= 0:
                    continue

                # 🛑 检查：突破箱体上边缘的时候直接买入
                box_top = info.get('high_max', 0)
                if current_price <= box_top:
                    continue

                # ==========================
                # 进入此阶段代表个股已向上突破箱体上沿，直接触发买入信号。
                # ==========================

                # 顺利成为触发买入候选
                triggered_stocks.append((stock, current_price, box_top, 0.0))

            if len(triggered_stocks) > 0:
                cash_per_stock = buy_budget / len(triggered_stocks)

                for stock, current_price, box_top, _ in triggered_stocks:
                    ordered_amount = order_buy_once(stock, cash_per_stock, current_price)
                    if ordered_amount >= 100:
                        est_value = ordered_amount * current_price
                        log.info(f"🛒【盘中买入触发】{stock} 当前价 {current_price:.2f} 突破箱体上缘 {box_top:.2f}，计划金额: {cash_per_stock:.2f}，实际买入 {ordered_amount} 股。")
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