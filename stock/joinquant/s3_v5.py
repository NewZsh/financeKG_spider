# -*- coding: utf-8 -*-
"""
策略名称：
全主板"安全背驰买点"缠论分钟频策略 (s3)  —— v5.0 市场状态自适应·进取版

======================================================================
★ v5.0 核心思想：在 v4 三分状态基础上，把 trend 模式的买点从"底背驰抄底"
   升级为"缠论二买/三买(顺势)+周线中枢上移"——从"抄底弱势"翻转为"买强势股回调"
======================================================================
v4 数据发现：四版本选股全是负 alpha（胜率<50%、平仓全亏），超额只来自熊市避跌。
"底背驰直买"本质是抄底反转（接弱势股飞刀），在强牛段能跟涨但整体负 edge。
缠论真正的"进取"工具是二买(回调不破一类低点)与三买(突破后回踩不进中枢)——
都是已确认强势股的回踩买点。v5 在 trend 模式用这两个顺势信号，并要求候选股
处于周线"中枢上移"的上升结构（多级别联立），实现"只在强势股的主升回踩上车"。
  trend 模式：宽入口(2闸+周线中枢上移,上限300) / 等权近满仓 / 30分二买·三买 /
            结构止损(二买/三买低点) / 利润棘轮 / 不熔断
  range 模式：严入口(3闸+120日位置,上限40) / 比例轻仓留30%现金 / 30分底背驰 /
            -8%+结构止损+利润棘轮 / 中枢fail-closed / 启用熔断   （同 v4）
  bear 模式：不开新仓，只管持仓（同 v4）
开关 AGGRESSIVE_BUY=False 可整体退化回 v4 行为，便于 A/B 对照。

======================================================================
★ v4.0 核心思想：市场状态自适应（V2 与 V3.3 风格互补，非二选一）
======================================================================
分时段回测发现：V2.0(30分直买版) 在趋势多头市多收益少回撤，V3.3(三硬伤修复版)
在弱势震荡市少挨打——同一参数在不同风格下优劣反转。v4 不再固定一套参数，
而是盘前用沪深300判断市场状态，按状态切换两套参数集：
  trend (趋势多头: 沪深300>MA60 且 MA20上行) → V2.0 风格：
      宽入口(2闸,上限300) / 等权近满仓 / -5%+MA20均线止损 / 中枢fail-open / 不熔断
  range (弱势震荡: 其余) → V3.3 风格：
      严入口(3闸+120日位置,上限40) / 比例轻仓留30%现金 / -8%+结构止损+利润棘轮 / 中枢fail-closed / 启用熔断
切换由 g.active_mode 驱动，贯穿 before_market_open / _check_stop_loss /
_process_30m_signals / _post_exit_cleanup。跨模式遗留仓位(trend建仓后次日转range)
在 range 日无 stop_ref 时回退 MA 止损兜底，避免保护缺失。

======================================================================
〇·AAA、v3.3 修改清单（相对 v3.2）—— 修复 review 指出的三处逻辑硬伤
======================================================================
【硬伤1. 利润回撤黑洞 → 新增"移动止盈利润棘轮"】
    v3.2 的止盈唯一出口 = "30分钟顶背驰 + 现价>中枢ZG"，条件很严；
    而全部止损（刚性/结构）都锚定在【成本价下方】。后果：一只票浮盈
    +30% 后若凑不齐严格卖点（如缓慢阴跌、无新高背驰结构），利润无人
    看守，一路回吐到成本下方的止损线才离场——"盈利单坐电梯"。
    v3.3 新增利润棘轮（trailing）：
      - 持仓期间维护最高价水位 g.high_watermark_dict（仅用已发生的
        盘中现价更新，无未来函数）；
      - 浮盈曾达 TRAIL_ACTIVATE_RATIO(10%) 即激活保护；
      - 激活后现价从最高水位回撤 ≥ TRAIL_DRAWDOWN_RATIO(8%) → 止盈
        离场。（例：最高+10%回撤8%≈保本出，最高+30%≈锁住+19.6%）
      背驰卖点仍是首选出口（更早、更精确），棘轮只是"利润的最后防线"。

【硬伤2. 结构止损被刚性止损架空 → 止损体系重构】
    v3.2 的结构止损位取"30分信号笔低点 与 日线信号笔低点 的较低者"。
    日线笔低点距买入价常有 10%~20%，远低于 -5% 刚性线 → 刚性止损
    永远先触发，结构止损沦为死代码；且 -5% 刚性砍在结构位之前，等于
    v3.0 修掉的"止损与入场方向打架"换了个形式回归（背驰反转还没
    走出来就被固定百分比扫出）。v3.3 重构为：
      a) 结构止损位只用【30分钟信号笔低点】（不再并入日线笔低点）；
      b) 新增入场前风险过滤 MAX_STOP_DISTANCE(5%)：若结构止损触发价
         距买入价超过 5% → 该笔交易风险过大，直接放弃信号（止损太远
         的交易不做，从源头保证"结构止损永远先于刚性止损触发"）；
      c) 刚性止损放宽 -5% → -8%，降级为纯灾难兜底（隔夜跳空/极端
         行情），正常破位一律由结构止损（≤5%+1%缓冲）先行处理。
    三层出口自此各司其职：背驰卖点(利润最大化) > 利润棘轮(保盈利)
    > 结构止损(证伪离场) > -8%刚性(灾难兜底)。

【硬伤3. 中枢过滤 fail-open → 买入端改为 fail-closed】
    v3.2 买入端写的是 `if zg is not None and close > zg: 拒绝`——
    当 zg 为 None（没形成中枢 / 中枢已过期=走势不明）时【无条件放行】。
    "不追中枢上方"这道安全闸在最需要它的场景（无结构参照的单边行情、
    长期无中枢的乱走势）反而完全失效。v3.3 改为 fail-closed：
      - 买入端（日线确认 + 30分入场）：无有效中枢 → 直接拒绝
        （看不清结构就不买，宁可错过）；
      - 卖出端（止盈）保持 fail-open：无中枢时只要顶背驰即可止盈
        （出口从宽是安全方向，且已有棘轮兜底）——不对称设计是有意的。

======================================================================
〇·AA、v3.2 修改清单（相对 v3.1）
======================================================================
本版按实盘/回测的三点反馈 + review 遗留细节做收口：

【1. 修复"等权分配失效" → 改为账户总资产比例仓位】
    v3.1 的买入用 cash_per_stock = buy_budget / len(triggered)。问题：
    同一个30分钟收盘时刻，多只股票"同时"成立背驰买点的概率极低，
    triggered 几乎恒为 1 → 等权退化成"把留足30%现金后的全部预算
    砸给这唯一一只"，再被绝对值 MAX_TRADE_VALUE=10万 截断。
    后果：账户越小，单只占比越离谱（如20万账户，留30%后14万全给
    一只 → 单票 50%+ 仓位，完全没有分散）。
    v3.2 改为【固定比例仓位】：单只建仓市值上限 = 账户总资产 ×
    PER_STOCK_MAX_RATIO（默认0.20），并对当批触发的每只逐单实时
    重算剩余可用预算（始终留30%现金），既保证分散，也让后续
    30分钟时刻/后续交易日仍有钱继续买（改善资金闲置）。
    MAX_TRADE_VALUE 退居为"单笔分单抗冲击阈值"，不再充当仓位上限。

【2. 移除"沪深300 < 60日线不买"大盘闸】
    该闸与"买入已看月线多头(第2闸)"在大势维度部分重叠，且属于
    择时性质的系统性开关。按需求移除：initialize / before_market_open /
    盘中买入 三处的 g.is_market_safe 逻辑全部删除。
    注：熔断(连亏5次冻结8天)与黑名单/结构止损等风控保留不变。

【3. 黑名单区分盈亏（止盈不再长期误杀二次买点）】
    v3.1 卖出后一律黑名单5天，会把"止盈出场后回踩再出二次背驰买点"
    的强势票也封5天而错过。v3.2 改为：亏损出场黑 BLACKLIST_LOSS_DAYS
    (5天)，止盈出场只黑 BLACKLIST_PROFIT_DAYS(2天)。
    blacklist_dict 值改存 (加入日期, 冷静天数)。

【4. review 遗留细节收口】
    - 资金利用：逐单实时重算预算(见第1条)，避免"只触发1只时预算
      被一次性吃光/截断、后续不再补"。
    - 陈旧中枢：ZS_MAX_AGE 时效过滤已在 detect_beichi_signal 生效。
    - 熔断死代码：连亏5次熔断检查已真正启用(v3.0起)。

======================================================================
〇·A、v3.1 修复：预筛"山腰陷阱"（长窗口位置过滤）
======================================================================
v3.0 的第1闸预筛只要求"近5日回踩到30日低点区域"。漏洞：若大拉升
发生在 30 日窗口之外（例：两个月前 10→30 元，之后 25~30 横盘），
则"30日低点"本身就悬在山腰平台（25元），回踩到它依旧是 +150% 的
高位接货——很可能正是主力出货区。
v3.1 在第1闸叠加【长窗口位置过滤】：
    现价在 120 日高低区间中的分位 = (现价-120日低)/(120日高-120日低)
    必须 ≤ 0.60（LONG_POS_MAX），否则视为仍在山腰/高位，拒绝入池。
取数窗口顺势从 30 根拉到 120 根（仍是单次廉价请求，两检查共用）。
"回踩"锚定短窗口（30日），"位置"锚定长窗口（120日），二者叠加
才构成完整的"拒绝追高"：既要回踩下来，也要本来就处在半年区间的
中下部。

======================================================================
〇、v3.0 版本说明（相对 v2.0 的修复清单）
======================================================================
本版按策略 review 的问题清单做了 5 处关键修复：

【P0-1 修复：止损与入场逻辑打架】
    v2.0 沿用 s1 的"均线止损"（跌破MA20 / MA20>MA5或MA10 转弱即卖）。
    但本策略入场 = 30分钟底背驰 = 买在"价格刚回调下来"的位置，
    此时日线 MA5 往往已拐头贴近 MA20 → 买入次日就被均线止损扫地出门，
    持仓周期被压到 1~2 天，背驰反转来不及走出来，变成高换手磨损机。
    v3.0 改为【结构止损】——买回调策略的正确止损方式：
        a) 刚性止损：亏损 ≥ 5% 清仓（保留，兜底）；
        b) 结构止损：现价跌破"买点信号笔的低点"（即背驰那一笔创出的
           新低）再留 1% 缓冲 → 背驰假设被证伪，立即离场。
        均线类止损全部移除（与买回调的方向天然矛盾）。
    结构止损在每个30分钟收盘时刻 + 14:57 都会检查（比 v2.0 只在
    14:57 查更快响应盘中破位，部分缓解 T+1 隔夜跳空的敞口）。

【P0-2 修复：回测性能不可运行】
    v2.0 候选池上限 300 只 × 每日 7 个30分钟时刻全量缠论 ≈ 2100 次
    30分钟K线请求/日，分钟回测必然超时。
    v3.0：候选池上限降到 40 只；且盘前先用"日线级别买点确认"
    （见 P1）把候选池压到极小，盘中扫描量 ≤ 40×7=280 次/日，可运行。

【P1 修复：级别错配 → 多级别联立】
    v2.0 直接拿 30分钟背驰当决策级别，一天噪声信号极多。
    v3.0 恢复缠论正统的"多级别联立"：
        月线定势(多头) → 日线定买点(日线底背驰近5日内成立+中枢下方)
        → 30分钟定入场(30分钟底背驰触发才扣扳机)。
    日线买点确认放在盘前（每日一次重计算），30分钟仅做精确入场。

【P1 修复：信号新鲜度过严】
    INTRADAY_FRESH_BARS 2 → 3（分型需右侧确认，笔端点天然滞后一根，
    =2 时绝大多数有效背驰因"确认晚一根"被拒）。

【P2 修复：熔断死代码 / 陈旧中枢】
    a) 连亏5次熔断的检查代码正式启用（v2.0 中设置计数却注释了检查，
       计数器空转并不真正停手）；
    b) 中枢时效过滤：最近一个中枢若"太老"（终点距今超过 ZS_MAX_AGE
       根K线），不再拿它做位置过滤（陈旧区间会使 现价vsZG 判断失真）。

======================================================================
一、策略总纲（多级别联立版）
======================================================================
本策略在 s1.py（突破-洗盘-反包策略）的工程框架上，把"买卖信号"替换为
缠论的【安全背驰买卖点】，仓位管理 / 跌停死磕机制沿用 s1，止损改为
适配"买回调"的结构止损。

【安全背驰买点】= 三级联立，逐级收敛：
    第1级·月线定势（盘前）：月收盘 ≥ 月MA10（月线多头）。
        过滤"下跌趋势中途的反弹背驰"（下跌中继危险买点）。
    第2级·日线定买点（盘前）：日线级别底背驰在最近 DAILY_FRESH_BARS
        个交易日内成立，且现价 ≤ 日线最近中枢上沿ZG（不追中枢上方
        半山腰）。→ 通过者进入当日【监控候选池】（≤40只）。
    第3级·30分钟定入场（盘中）：候选池个股在30分钟级别出现底背驰
        （最近 INTRADAY_FRESH_BARS 根30分钟K线内成立），且现价 ≤
        30分钟最近中枢ZG → 立即按【比例仓位】下单买入（单只 ≤ 账户
        总资产 × PER_STOCK_MAX_RATIO，留 CASH_RESERVE_RATIO 现金）。

【安全背驰卖点】（止盈首选出口）：
    持仓股 30分钟顶背驰 + (无有效中枢 或 现价 > 30分钟中枢ZG)
    → 全仓清出。卖出端 fail-open：无中枢时顶背驰即可走（出口从宽）。

【利润棘轮】（v3.3 新增，浮盈的最后防线）：
    浮盈曾达 TRAIL_ACTIVATE_RATIO(10%) 激活；激活后现价从最高水位
    回撤 ≥ TRAIL_DRAWDOWN_RATIO(8%) → 止盈离场。

【止损】（每个30分钟收盘时刻 + 14:57 检查，v3.3 重构）：
    a) 结构：现价 < 30分钟信号笔低点 × (1 - STRUCT_STOP_BUFFER)，
       且入场前已保证该触发价距买入价 ≤ MAX_STOP_DISTANCE(5%)；
    b) 刚性：亏损 ≥ 8%（纯灾难兜底：隔夜跳空/极端行情）。

======================================================================
二、与 s1 保持一致的部分
======================================================================
1. 股票池：全主板（过滤 300/301 创业板、688 科创板、8/4 北交所、
   ST/*ST/退、停牌、上市不足180天）。
2. 仓位管理（v3.2 调整）：保持30%现金、单股上限=账户总资产×
   PER_STOCK_MAX_RATIO(比例仓位，替代已失效的"等权分配")、单周期
   一次、每日最多 MAX_BUYS_PER_DAY 只；单笔按 MAX_TRADE_VALUE 分单抗冲击。
3. 跌停等特殊情况：开盘跌停挂跌停价死磕 / 补卖池 / 14:57 锁死收复
   判定 / 15:30 盘后废单审计 —— 完全一致。
4. 风控善后（v3.2 调整）：黑名单——亏损出场5天/止盈出场2天；
   连亏5次熔断冻结8日（v3.0 起真正生效）。
   注：v3.2 已移除"沪深300<60日线不买"的大盘闸。

======================================================================
三、性能设计（分钟频回测的关键）
======================================================================
  - before_market_open（盘前，每日一次，重计算集中在此）：
      a) 廉价日线预筛（单次120根K线取数，两检查共用）：
         ① 近 PRESCREEN_RECENT 日回踩到 PRESCREEN_LOW_WINDOW 日低点区域；
         ② 现价 120 日分位 ≤ LONG_POS_MAX（v3.1 修山腰陷阱）；
      b) 月线多头过滤（12根月K线）；
      c) 日线缠论买点确认（250根日K线，最重，放最后、且池满即停）；
      → 三道闸后进入 g.candidate_pool（上限 MAX_CANDIDATES=40）。
  - market_intraday（盘中每分钟触发）：
      * 仅在 30分钟收盘时刻（BAR_30M_TIMES）做重计算：
          - 持仓：30分钟卖点止盈 + 结构/刚性止损检查；
          - 候选池（≤40只）：30分钟买点 → 直接买入。
      * 14:57 及以后：止损终检 + 跌停死磕生死劫。

======================================================================
四、策略流程 UML
======================================================================
```mermaid
flowchart TD
    A[盘前] --> B[熔断检查/黑名单/补卖池清理]
    B --> C[日线廉价预筛: 近5日回踩30日低点 且 现价120日分位≤60%]
    C --> D{月线在MA10上方?}
    D -- 否 --> D1[丢弃: 下跌中继危险]
    D -- 是 --> E{日线底背驰近5日内成立 且 现价≤日线中枢ZG?}
    E -- 否 --> E1[丢弃: 日线级别买点未确认]
    E -- 是 --> F[进监控候选池 ≤40只]
    F --> MO[开盘: 跌停拦截/死磕池挂单/ST清仓]
    MO --> I[盘中每分钟]
    I --> T{是30分钟收盘时刻?}
    T -- 是 --> S1[持仓: 30分顶背驰→止盈 无中枢也可走]
    S1 --> S0[持仓: 浮盈曾达10%后回撤8%→棘轮止盈]
    S0 --> S2[持仓: 跌破30分信号笔低点→结构止损 / -8%灾难兜底]
    S2 --> K[候选池: 30分底背驰+有效中枢下方+止损距离≤5%→买入]
    I --> O[14:57: 死磕池生死劫 + 止损终检]
    O --> P[15:30 盘后审计: 废单入补卖池]
```
"""
from jqdata import *
import numpy as np
import pandas as pd

# ==================== 全局常量 ====================
# v3.2：MAX_TRADE_VALUE 由"仓位上限"降级为"单笔分单抗冲击阈值"——
#   单笔下单金额超过它就拆成多笔，避免大单冲击；真正的单只仓位上限
#   由下面的 PER_STOCK_MAX_RATIO（按账户总资产比例）决定。
MAX_TRADE_VALUE = 100000       # 单笔分单抗冲击阈值（元），超过则拆单
MAX_BUYS_PER_DAY = 5           # 每日最大买入只数

# ---- 仓位管理（v3.2：比例仓位，替代已失效的"等权分配"）----
# 单只个股建仓市值上限 = 账户总资产 × PER_STOCK_MAX_RATIO。
#   0.20 = 单只最多占总资产20% → 配合"留30%现金"，最多约3~4只并存，
#   兼顾分散与单笔有效性。想更分散调小(如0.15)，想更集中调大(如0.25)。
PER_STOCK_MAX_RATIO = 0.20
CASH_RESERVE_RATIO = 0.30      # 强制保留现金比例（不动用总资产的30%）

# ---- 黑名单冷静期（v3.2：区分盈亏）----
BLACKLIST_LOSS_DAYS = 5        # 亏损出场：黑名单 5 天（防割肉后手痒接飞刀）
BLACKLIST_PROFIT_DAYS = 2      # 止盈出场：只黑 2 天（保留回踩二次背驰买点机会）

# ---- 盘前三道闸参数 ----
PRESCREEN_LOW_WINDOW = 30      # 日线预筛：回踩到 N 日低点区域
PRESCREEN_RECENT = 5           # 日线预筛：低点须发生在最近 N 日内
# v3.1 新增·长窗口位置过滤（修"山腰陷阱"）：
#   若一只股票在 30 日窗口之外刚经历过大拉升（如两个月前 10→30 元，之后
#   25~30 横盘），"30日低点"本身就悬在山腰（25元平台）。仅靠"回踩30日
#   低点"会在 +150% 的高位入池接货。因此再加一道锚定长窗口的位置约束：
#   现价在 120 日高低区间中的分位 ≤ 60% 才允许入池。
#   分位 = (现价 - 120日最低) / (120日最高 - 120日最低)
LONG_POS_WINDOW = 120          # 长窗口位置过滤的观察窗口（约半年）
LONG_POS_MAX = 0.60            # 现价分位上限：> 0.60 视为仍在山腰/高位，拒绝
DAILY_FRESH_BARS = 5           # 日线背驰信号新鲜度：须在最近 N 个交易日内成立
MAX_CANDIDATES = 40            # 监控候选池上限（P0性能修复：300→40）
BARS_DAY_FOR_CHAN = 250        # 日线缠论分析所用K线根数
BARS_WEEK_FOR_CHAN = 120        # 周线缠论分析所用K线根数（约2.3年，足够识别周线中枢上移）

# ---- 盘中30分钟入场参数 ----
BARS_30M_FOR_CHAN = 250        # 30分钟缠论分析所用K线根数（约31个交易日）
INTRADAY_FRESH_BARS = 3        # 30分钟背驰新鲜度（P1修复：2→3，分型右侧确认天然滞后）

# ---- 止损参数（v3.3 重构：结构止损为主，刚性降级为灾难兜底）----
# 设计要点：入场前用 MAX_STOP_DISTANCE 保证结构止损触发价距买入价 ≤5%，
#   刚性线放到 -8% → 正常破位永远是结构止损先触发（修复 v3.2 中
#   "刚性-5%永远先于10%+外的结构位触发、结构止损沦为死代码"的硬伤）。
HARD_STOP_RATIO = -0.08        # 刚性止损线：纯灾难兜底（隔夜跳空/极端行情）
STRUCT_STOP_BUFFER = 0.01      # 结构止损缓冲：跌破信号笔低点 1% 才触发（防插针）
MAX_STOP_DISTANCE = 0.05       # 入场前风险过滤：结构止损触发价距买入价 >5% 则放弃该信号

# ---- 利润棘轮（v3.3 新增：修"利润回撤黑洞"）----
# 背驰卖点是首选出口（更早更精确），棘轮是浮盈的最后防线：
#   浮盈曾达 10% 即激活；激活后现价从最高水位回撤 8% → 止盈离场。
#   例：最高 +10% 回撤 8% ≈ 保本出；最高 +30% ≈ 锁住 +19.6%。
TRAIL_ACTIVATE_RATIO = 0.10    # 激活阈值：最高价 ≥ 成本 ×(1+10%)
TRAIL_DRAWDOWN_RATIO = 0.08    # 触发阈值：现价 ≤ 最高水位 ×(1-8%)

# ---- 中枢时效（P2修复：陈旧中枢不做位置过滤）----
ZS_MAX_AGE = 60                # 最近中枢终点距今超过 N 根K线视为过期（日线/30分钟通用）

# ---- v5：进取信号开关 ----
# True → trend 模式启用"缠论二买/三买(顺势)+周线中枢上移"进取买点（买强势股回调），
#   替代原 v4 的"30分底背驰抄底"。False → 退化为 v4 行为，便于 A/B 对照。
AGGRESSIVE_BUY = True

# 30分钟K线收盘时刻（沪深两市）。15:00收盘无法成交，故不纳入；
# 仅在这些时刻做重量级缠论检测与下单/止损。
BAR_30M_TIMES = {'10:00', '10:30', '11:00', '11:30', '13:30', '14:00', '14:30'}

# ==================== v4：市场状态自适应参数 ====================
# 盘前用沪深300判断市场状态，按状态切换两套参数集：
#   trend (趋势多头) → v2.0 风格：宽入口/等权满仓/-5%+MA20止损/不熔断/fail-open
#   range (弱势震荡) → v3.3 风格：严入口/比例轻仓/-8%+结构+棘轮止损/启用熔断/fail-closed
# range 模式参数即上方现有常量(MAX_CANDIDATES=40 / INTRADAY_FRESH_BARS=3 /
#   HARD_STOP_RATIO=-0.08 / PER_STOCK_MAX_RATIO=0.20 / CASH_RESERVE_RATIO=0.30 /
#   LONG_POS_MAX=0.60 / TRAIL_* / STRUCT_*)。下方仅声明 trend 模式覆盖值与状态分类器参数。
REGIME_INDEX = '000300.XSHG'      # 大盘状态判断基准（沪深300）
REGIME_TREND_BARS = 70            # 取大盘近 N 日K线判断状态
REGIME_MA_LONG = 60               # 中期均线：收盘 > MA60 视为中期多头
REGIME_MA_SHORT = 20              # 短期均线：MA20 上行视为短期强势

# ---- trend 模式参数（= v2.0 风格）----
TREND_MAX_CANDIDATES = 300        # 宽入口：候选池上限放大到 300
TREND_INTRADAY_FRESH = 2          # 30分背驰新鲜度（v2.0 原值，更严）
TREND_HARD_STOP = -0.05           # -5% 紧止损（v2.0 原值）
TREND_CASH_RESERVE = 0.0          # 不强留现金（等权近满仓）
TREND_BLACKLIST_DAYS = 5          # 平铺 5 天黑名单（不分盈亏）
TREND_PER_STOCK_MAX_RATIO = 0.30  # trend 模式单票上限(总资产×30%，防等权退化全仓1只)
# trend 模式另有（在分支里体现，无需常量）：不做日线确认第3闸 / 不做120日位置过滤 /
#   中枢 fail-open / 不用结构止损与利润棘轮(改用 MA20 均线止损) / 不启用熔断。

# ---- v4.1 三分状态：bear 状态参数 + trend 确认缓冲 ----
REGIME_CONFIRM_DAYS = 3           # trend 须连续 N 日满足条件才切换(防熊市反弹误判)
REGIME_MA_COMPARE_GAP = 5         # MA60 与 N 日前的 MA60 比较以判断上行/下行(更稳，抗插针)
# bear(熊市/下行)：沪深300<MA60 且 MA60 下行 → 不开新仓，只管理持仓止盈止损。
#   2022-2023 熊市反弹段曾因二分判定被误归 trend(满仓进攻)致大回撤；
#   三分把"下行"独立出来空仓防御，是回撤控制的关键。

# ---- v4.3 永久持仓（深度价值买入并长期持有，不参与止损/止盈/跌停拦截）----
# 触发条件满足时用固定预算买入后永久锁仓，与 trend/range/bear 策略仓位独立。
PERM_HOLD_STOCKS = [
    {'code': '601988.XSHG', 'name': '中国银行', 'metric': 'pb', 'thresh': 0.8, 'budget': 5000},
    {'code': '600900.XSHG', 'name': '长江电力', 'metric': 'pe', 'thresh': 20, 'budget': 5000},
]
PERM_HOLD_CODES = {c['code'] for c in PERM_HOLD_STOCKS}

# ---- 货币ETF现金管理（闲置现金赚隔夜利息，加仓前按需变现）----
CASH_ETF = '511880.XSHG'        # 华宝添益货币ETF（T+0，年化~2%）
CASH_ETF_RESERVE = 10000        # 保留至少 1 万元现金应对临时需求


# ======================================================================
# 〇、缠论核心算法（自包含，移植自本地 ta_calc.py，纯 numpy/pandas 实现）
#     —— 与级别无关：喂日线就是日线级别，喂30分钟线就是30分钟级别
# ======================================================================

def _fractal(high, low):
    """顶/底分型识别。
    输入: high/low 为 np.ndarray
    输出: f 数组, 1=顶分型, -1=底分型, 0=无
    定义: 第i根K线高低点同时高于左右两根 → 顶分型; 同时低于 → 底分型。
    注意 range(1, n-1) 不取最后一根 → 分型靠已收盘的右侧K线确认，无未来函数。"""
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


def detect_beichi_signal(high, low, close, recent_bars=3, zs_max_age=ZS_MAX_AGE):
    """检测最近是否出现"背驰"信号（缠论一类买卖点候选）。
    —— 本函数与级别无关，传入哪个周期的K线就是哪个周期的背驰。

    判定逻辑（与本地 ta_calc.bei_chi 一致）：
      取最近一笔 cur 与上一根同向笔 prev（间隔一笔），
      - 底背驰(买)：cur 是下降笔, cur 低点 < prev 低点(创新低)，
        且 cur 区间 |MACD柱面积| < prev 区间 |MACD柱面积|
      - 顶背驰(卖)：cur 是上升笔, cur 高点 > prev 高点(创新高)，
        且面积同样缩小
      信号新鲜度：cur 笔终点必须落在最近 recent_bars 根K线内。

    P2修复·中枢时效：最近中枢的终点若距离现在超过 zs_max_age 根K线，
      视为"陈旧中枢"，zg/zd 返回 None（不拿过期区间做位置过滤）。

    返回: (signal, last_zg, last_zd, sig_low)
      signal  : 'buy' / 'sell' / None
      last_zg : 最近【未过期】中枢上沿（无则 None）
      last_zd : 最近【未过期】中枢下沿（无则 None）
      sig_low : 信号笔的极值价（底背驰=新低价, 顶背驰=新高价），
                买入后作为【结构止损参考位】——跌破它即背驰证伪。"""
    bi = _bi_list(high, low)
    if len(bi) < 3:
        return None, None, None, None

    n = len(close)
    zsl = _zhongshu_from_bi(bi)
    last_zg, last_zd = None, None
    if zsl:
        zs = zsl[-1]
        # 中枢时效检查：终点距今 ≤ zs_max_age 根K线才有效
        if (n - 1 - zs[1]) <= zs_max_age:
            last_zg, last_zd = zs[2], zs[3]

    hist = _macd_hist(close)

    cur, prev = bi[-1], bi[-3]
    # 信号必须"新鲜"：最近一笔的终点在最后 recent_bars 根K线内
    if cur[1] < n - recent_bars:
        return None, last_zg, last_zd, None
    if cur[2] != prev[2]:
        return None, last_zg, last_zd, None

    area_cur = abs(np.nansum(hist[cur[0]:cur[1]+1]))
    area_prev = abs(np.nansum(hist[prev[0]:prev[1]+1]))
    if area_prev <= 0:
        return None, last_zg, last_zd, None

    if cur[2] == -1 and cur[4] < prev[4] and area_cur < area_prev:
        return 'buy', last_zg, last_zd, cur[4]    # 底背驰, sig_low=信号笔新低
    if cur[2] == 1 and cur[4] > prev[4] and area_cur < area_prev:
        return 'sell', last_zg, last_zd, cur[4]   # 顶背驰, sig_low=信号笔新高
    return None, last_zg, last_zd, None


def is_monthly_trend_up(security, context):
    """月线大势过滤（第1级）：月线收盘 ≥ 月线 MA10 → 视为大级别多头。
    月线趋势向下时出现的底背驰，多为下跌中继的反弹（危险买点），必须放弃。
    盘前对候选池预先计算，盘中直接信任（月线日内不变）。"""
    try:
        mdf = get_bars(security, count=12, unit='1M', fields=['close'], include_now=False)
        if mdf is None or len(mdf) < 10:
            return False   # 数据不足按不安全处理
        closes = np.asarray(mdf['close'], dtype=float)
        ma10 = closes[-10:].mean()
        return closes[-1] >= ma10
    except Exception as e:
        log.warn("月线数据获取失败 %s: %s" % (security, e))
        return False


def daily_buy_point_confirmed(security):
    """日线级别买点确认（第2级，P1修复的核心）：
    要求日线底背驰在最近 DAILY_FRESH_BARS 个交易日内成立，
    且最新收盘价 ≤ 日线最近中枢上沿ZG（不在中枢上方半山腰）。
    通过 → 该股具备"日线级别的一类买点背景"，才允许降级到
    30分钟找精确入场。这是缠论"多级别联立"的正确用法：
    大级别定买卖，小级别定买卖点位置。
    返回: (True/False, day_sig_low)
    注（v3.3）：day_sig_low 仅存入 candidate_info 备查/日志用，
    不再并入结构止损位（避免把止损推远、被刚性线架空，见硬伤2）。"""
    try:
        bars = get_bars(security, count=BARS_DAY_FOR_CHAN, unit='1d',
                        fields=['high', 'low', 'close'], include_now=False)
    except Exception as e:
        log.warn("日线数据获取失败 %s: %s" % (security, e))
        return False, None
    if bars is None or len(bars) < 100:
        return False, None
    high = np.asarray(bars['high'], dtype=float)
    low = np.asarray(bars['low'], dtype=float)
    close = np.asarray(bars['close'], dtype=float)

    sig, zg, zd, sig_low = detect_beichi_signal(
        high, low, close, recent_bars=DAILY_FRESH_BARS, zs_max_age=ZS_MAX_AGE)
    if sig != 'buy':
        return False, None
    # 中枢位置过滤（v3.3 改 fail-closed）：必须存在【有效且未过期】的日线
    # 中枢，且收盘价 ≤ 中枢上沿ZG（中枢内/下方 = 一/二类买点区域）。
    # 无中枢 = 走势结构不明 → 直接拒绝，宁可错过（旧版 zg=None 时无条件
    # 放行，"不追中枢上方"这道闸在最需要时反而失效）。
    if zg is None or close[-1] > zg:
        return False, None
    return True, sig_low


def is_weekly_rising(security, context):
    """v5 进取版·周线"中枢上移"判定（多级别上升结构过滤）：
    取周线K线→笔→中枢，要求：① 存在≥2个中枢；② 最近中枢ZG > 前一中枢ZG
    （中枢上移，缠论"走势必完美"中的上升结构）；③ 现价站在最近中枢ZG上方
    （上升段进行中，非已跌破）。满足→该股处于周线级上升走势，才允许用30分钟
    二买/三买"买强势回调"。这是把 v4 的"抄底弱势股"翻转为"买强势股回踩"的关键闸。
    盘前对 trend 候选池计算，盘中信任（周线日内不变）。"""
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
        return False          # 中枢未上移 → 非上升结构
    if close[-1] <= zg_last:
        return False          # 现价未站上最近中枢上沿 → 不在上升段
    return True


def detect_aggressive_buy(high, low, close, recent_bars=3, zs_max_age=ZS_MAX_AGE):
    """v5 进取版·缠论二买/三买(顺势)检测。返回 (signal, kind, zg, zd, sig_low)。
    kind ∈ {'2nd' 二类买点, '3rd' 三类买点}。与 detect_beichi_signal(底背驰=一类,
    抄底反转)互补：这里是"顺势/回调买"，抓的是已确认强势股的回踩，而非弱势股接刀。
      - 三买(3rd)：存在有效中枢[ZD,ZG]；中枢后一笔向上突破(ZG上方新高)，随后一笔
            向下回踩但低点≥ZG(不进中枢)，且该回踩下降笔新鲜→顺势追涨买点。
      - 二买(2nd)：最近一类买点(底背驰)低点L1后回升一笔，再回落一笔低点>L1
            (不破一类低点，确认反转有效)且回落笔新鲜→更安全再入场买点。
    无信号返回 (None,None,None,None,None)。sig_low 作为结构止损参考位
    （跌破二买/三买低点=结构证伪）。"""
    bi = _bi_list(high, low)
    if len(bi) < 5:
        return None, None, None, None, None
    n = len(close)
    zsl = _zhongshu_from_bi(bi)
    last_zg, last_zd = None, None
    if zsl:
        zs = zsl[-1]
        if (n - 1 - zs[1]) <= zs_max_age:
            last_zg, last_zd = zs[2], zs[3]

    # ---- 三买：中枢后突破→回踩不进中枢 ----
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
                    down = b   # 回踩低点≥ZG(不进中枢) → 三买
            if down is not None and down[1] >= n - recent_bars:
                return 'buy', '3rd', last_zg, last_zd, down[4]

    # ---- 二买：一类低点L1后回升→回落不破L1 ----
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
                return 'buy', '2nd', last_zg, last_zd, down2[4]
    return None, None, None, None, None


# ======================================================================
# 一、下单工具函数（与 s1 完全一致）
# ======================================================================

def order_buy_once(security, target_value, reference_price):
    """单次建仓买入（v3.2）：
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
    log.info('【缠论安全背驰版 s3 v4.0 · 市场状态自适应(trend=V2风格/range=V3.3风格)】启动...')

    # 主板标准佣金（最低5元）
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003,
                             close_commission=0.0003, min_commission=5), type='stock')

    # ---- 交易状态 ----
    g.buy_cost_dict = {}            # 个股真实买入成本 {stock: price}
    g.stop_ref_dict = {}            # 【结构止损参考位】{stock: 30分钟买点信号笔低点}
    g.high_watermark_dict = {}      # 【利润棘轮】持仓期最高价水位 {stock: price}（v3.3）
    g.blacklist_dict = {}           # 黑名单 {stock: (加入日期, 冷静天数)}，到期自动移出（v3.2：亏损5天/止盈2天）
    g.consecutive_loss_count = 0    # 连续亏损计数（熔断用）
    g.freeze_days_left = 0          # 熔断剩余冻结天数（v3.0 起真正生效）
    g.position_lock_stocks = set()  # 持仓周期锁：清仓前不允许再次买入
    g.trailing_stop_last_date = {}  # 个股当日卖出去重 {stock: date}
    g.today_buy_count = 0           # 当日已买入只数
    g.frozen_today = False          # 当日是否处于熔断冻结（盘中买入检查用）
    g.active_mode = 'range'         # v4：当日市场状态('trend'/'range'/'bear')，盘前由 classify_market_regime 设定
    g.trend_confirm_count = 0      # v4.1：trend 连续确认计数(防熊市反弹误判)
    g.perm_hold_done = set()       # v4.3：已完成永久建仓的股票(建仓后不再止损/止盈/重复买入)

    # ---- 监控候选池（盘前构建：range 三道闸 / trend 两道闸）----
    g.candidate_pool = set()        # {stock, ...}
    g.candidate_info = {}           # {stock: {'day_sig_low': float}} 日线信号笔低点备查

    # v3.2：已移除"沪深300<60日线不买"大盘闸，不再维护 g.is_market_safe

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
        if stock == CASH_ETF:
            continue  # 排除货币ETF，避免被当成股票候选
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
# 三·B、v4 市场状态分类器（盘前调用，决定当日用 trend / range 哪套参数）
# ======================================================================

def classify_market_regime(context):
    """v4.2 三分状态·慢进快出：用沪深300判断 trend / range / bear。
      进入trend(慢进)：价格>MA60 且 MA60上行，连续 REGIME_CONFIRM_DAYS 日确认
            → 防 2022-2023 熊市反弹段误判为 trend 满仓进攻。
      维持trend(快出)：一旦 价格<MA20 或 MA20 下行 → 立即退出至 range/bear
            → 防 2021 年初核心资产见顶时 MA60 滞后仍上行、trend 不退而满仓吃急跌。
      bear  (熊市/下行)：价格<MA60 且 MA60 下行 → 不开新仓。
      range (震荡)：其余。
    设计依据：
      ① 进出非对称——进入用 MA60(慢、稳，抗反弹插针)，退出用 MA20(快，防见顶滞后)；
      ② v4.1 用 MA60 判退出(慢进慢出)，2021 见顶时 MA60 仍在上行不退出→满仓吃跌；
         改用 MA20 做快速退出信号，MA60 严格条件+确认做慢进，兼顾两端。
      ③ bear 立即生效(下行市快速空仓防御)。"""
    try:
        bars = get_bars(REGIME_INDEX, count=REGIME_TREND_BARS, unit='1d',
                        fields=['close'], include_now=False)
        if bars is None or len(bars) < REGIME_MA_LONG + REGIME_MA_COMPARE_GAP + 2:
            return 'range'
        closes = np.asarray(bars['close'], dtype=float)
        ma60_now = closes[-REGIME_MA_LONG:].mean()
        # N 日前的 MA60(同样60日窗口，整体前移 N 日)
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
# 四、盘前：熔断/风控清理 + 三道闸构建"监控候选池"
#     第1闸: 日线廉价预筛(最便宜, 先砍大头)
#     第2闸: 月线多头(次便宜)
#     第3闸: 日线缠论买点确认(最贵, 放最后, 池满即停)
# ======================================================================

def before_market_open(context):
    current_date = context.current_dt.date()
    g.today_buy_count = 0
    g.frozen_today = False
    g.candidate_pool = set()
    g.candidate_info = {}

    # v4.1：盘前先判市场状态(三分：trend/range/bear)
    g.active_mode = classify_market_regime(context)
    is_trend = (g.active_mode == 'trend')
    mode_desc = {'trend': '趋势多头(trend)→V2进攻风格', 'range': '弱势震荡(range)→V3.3防御风格',
                 'bear': '熊市下行(bear)→不开新仓'}[g.active_mode]
    log.info("🌐 今日市场状态: %s" % mode_desc)

    # 4.1 策略级熔断（P2修复：v3.0 起真正生效。连亏5次冻结买入8个交易日，
    #     冻结期内仍正常执行持仓的止盈/止损/死磕，只是不开新仓）
    # v4.1：仅 range 模式启用熔断；trend/bear 不冻结(trend连亏多为系统回调；bear本就不开新仓)。
    if g.active_mode != 'range':
        g.freeze_days_left = 0       # 非range态强制不冻结
    elif g.freeze_days_left > 0:
        g.freeze_days_left -= 1
        g.frozen_today = True
        log.warn("🧊 策略熔断保护中，剩余 %d 天不开新仓（持仓风控照常）。" % g.freeze_days_left)

    # 4.2 黑名单清理（v3.2：按各自冷静天数到期移出——亏损5天/止盈2天）
    for stock in list(g.blacklist_dict.keys()):
        entry = g.blacklist_dict[stock]
        # 兼容旧格式（若曾存单一日期，按亏损天数处理）
        if isinstance(entry, tuple):
            entry_date, ban_days = entry
        else:
            entry_date, ban_days = entry, BLACKLIST_LOSS_DAYS
        if (current_date - entry_date).days > ban_days:
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

    # 4.4 熔断中 → 今天不会开新仓，直接跳过重量级候选池构建（省回测时间）
    #     （v3.2 已移除"沪深300<60日线"大盘闸，不再因大盘方向跳过建池）
    if g.frozen_today:
        log.info("📋 今日不构建候选池（熔断冻结中）")
        return
    # v4.1：bear(熊市下行)状态不开新仓，跳过候选池构建（持仓止盈止损照常由盘中 _check_stop_loss 处理）
    if g.active_mode == 'bear':
        log.info("🐻 熊市状态，今日不开新仓（持仓风控照常）")
        return

    # 4.5 【构建监控候选池】v4：trend 两道闸(回踩+月多头, 上限300) / range 三道闸(+日线确认+120日位置, 上限40)
    max_cand = TREND_MAX_CANDIDATES if is_trend else MAX_CANDIDATES
    main_board_pool = get_main_board_pool(context)
    checked_heavy = 0   # 第3闸重计算次数统计（日志观察性能用）
    # 确定性修复：池满即停(break 在 max_cand)依赖遍历顺序；对
    # main_board_pool 排序，保证选取在不同运行间一致、可复现。
    for stock in sorted(main_board_pool):
        if len(g.candidate_pool) >= max_cand:
            break
        if stock in context.portfolio.positions or stock in g.blacklist_dict \
           or stock in g.position_lock_stocks:
            continue

        # --- 第1闸·廉价日线预筛（两个检查共用一次 120 根取数）---
        # 检查①"回踩"：近5日低点须触及30日低点区域（价格必须回踩下来）；
        # 检查②"位置"（v3.1 修山腰陷阱）：现价在120日高低区间分位 ≤ 60%。
        #   仅有①时，若大拉升发生在30日窗口之外，"30日低点"本身悬在
        #   山腰平台上，回踩到它仍是高位接货；②把锚定校准到半年窗口，
        #   要求现价处于长周期区间的中下部，二者叠加才真正拒绝追高。
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
        # v4：仅 range 模式做此过滤（trend 模式不拒创新高赢家，否则错过主升浪）。
        # 数据不足120根时用实际长度（次新股已被上市180天过滤，一般够用）
        if not is_trend:
            lo_120 = np.nanmin(lows_all)
            hi_120 = np.nanmax(highs_all)
            rng = hi_120 - lo_120
            if rng > 1e-6:
                pos_pct = (closes_all[-1] - lo_120) / rng
                if pos_pct > LONG_POS_MAX:
                    continue  # 现价仍在半年区间上部 → 前期大拉升的山腰/高位，拒绝

        # --- 第2闸·月线多头过滤（大势安全阀）---
        if not is_monthly_trend_up(stock, context):
            continue

        # --- v5 进取闸：trend 模式额外要求周线"中枢上移"（多级别上升结构）---
        # 把 trend 候选从"任意月多头"收窄到"周线级上升走势的强势股"，使盘中
        # 30分二买/三买变成"买强势回调"，而非"抄底弱势"（即 v4 底背驰的缺陷）。
        # range 模式不做此闸（range 用原防御逻辑，不追强势）。
        if is_trend and AGGRESSIVE_BUY and not is_weekly_rising(stock, context):
            continue

        # --- 第3闸·日线缠论买点确认（P1修复核心：日线定买点）---
        # 只有日线级别底背驰近5日内成立 + 中枢下方，才值得盘中用
        # 30分钟去找精确入场。这一道闸把候选池压到极小（性能关键）。
        # v4：仅 range 模式做（trend 模式宽入口，直接用30分背驰抓早期，不等日线确认）。
        day_sig_low = None
        if not is_trend:
            ok, day_sig_low = daily_buy_point_confirmed(stock)
            checked_heavy += 1
            if not ok:
                continue

        g.candidate_pool.add(stock)
        g.candidate_info[stock] = {'day_sig_low': day_sig_low}

    log.info("📋 今日监控候选池 %d 只 [%s模式, 上限%d]（重计算 %d 次）"
             % (len(g.candidate_pool), g.active_mode, max_cand, checked_heavy))


# ======================================================================
# 五、开盘：跌停拦截 + 死磕池处理 + ST强制清仓（与 s1 完全一致）
# ======================================================================

def check_perm_hold(context, current_data):
    """v4.3 永久持仓检查：深度价值条件(PB/PE)满足则用固定预算买入并永久持有。
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

    # v4.3 永久持仓检查（深度价值买入，与策略仓位独立）
    check_perm_hold(context, current_data)

    # 5.1 开盘即跌停的持仓：立即挂跌停价限价单卖出（永久持仓股跳过）
    for security in list(context.portfolio.positions.keys()):
        if security == CASH_ETF:
            continue   # 货币ETF不参与持仓风控
        if security in g.perm_hold_done:
            continue   # 永久持仓不卖出
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
        if security == CASH_ETF:
            continue   # 货币ETF不参与持仓风控
        if security in g.pending_exit_stocks:
            continue
        if current_data[security].is_st or 'ST' in current_data[security].name \
           or '退' in current_data[security].name:
            amount = context.portfolio.positions[security].total_amount
            log.error("⚠️ 持仓 %s 变为ST/退，开盘强制清仓 %d 股" % (security, amount))
            reference_price = current_data[security].day_open or current_data[security].last_price
            order_amount_in_chunks(security, -amount, reference_price)


# ======================================================================
# 六、盘中每分钟：30分钟收盘做缠论买卖点+止损 / 14:57 终检+死磕
# ======================================================================

def market_intraday(context):
    current_dt = context.current_dt
    current_date = current_dt.date()
    current_time = current_dt.strftime('%H:%M')
    current_data = get_current_data()

    # 6.1 仅在30分钟K线收盘时刻做重量级缠论检测、止损检查与下单
    if current_time in BAR_30M_TIMES:
        _check_stop_loss(context, current_data, current_date)      # 先风控
        _process_30m_signals(context, current_data, current_date)  # 再买卖点

    # 6.2 14:57 及以后：死磕池生死劫 + 止损终检（与 s1 时点一致）
    if current_time < '14:57':
        return

    # 死磕池 14:57 生死劫：收复锁死价+MA20 → 留活口；否则限价清仓
    # （MA20 在这里只用于"跌停股是否恢复持仓"的判定，与止损无关，保留 s1 原样）
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

    # 止损终检（当日最后一次机会，覆盖 30分钟时刻之间发生的破位）
    _check_stop_loss(context, current_data, current_date)


# ----------------------------------------------------------------------
# 六·A：止损检查（P0修复核心 —— 结构止损替代均线止损）
#   设计逻辑：本策略买的是"回调中的背驰反转"，此时均线（MA5/MA10 贴近
#   或低于 MA20）天然处于"转弱"形态，若沿用 s1 的均线止损，买入次日
#   就会被扫地出门。买回调的正确止损是【背驰假设证伪】：
#     - 背驰笔创出的那个新低，是"下跌动能衰竭"的证据锚点；
#     - 价格若有效跌破该低点（留 STRUCT_STOP_BUFFER 缓冲防插针），
#       说明下跌动能并未衰竭、背驰失败 → 无条件离场。
#   v3.3 重构：结构止损为主（入场前 MAX_STOP_DISTANCE 已保证它 ≤5%，
#   必先于刚性线触发）；刚性 -8% 降级为灾难兜底；并新增【利润棘轮】
#   （浮盈曾达10%后从最高水位回撤8%即锁利离场，修"利润回撤黑洞"）。
#   本函数在每个30分钟收盘时刻 + 14:57 各查一次：比 v2.0 只在 14:57
#   检查响应更快，部分缓解 T+1 新仓的隔夜跳空敞口（T+1 当日新仓
#   closeable_amount=0 天然跳过，次日起生效）。
# ----------------------------------------------------------------------

def _ma_stop_triggered(security, current_price, total_pnl_ratio):
    """trend 模式均线止损（= v2.0）：-5% 硬 + 跌破MA20 + 均线转弱(MA20>MA5或MA10)。
    亦用作 range 模式下"跨日遗留的 trend 仓位"(无 stop_ref) 的回退止损。
    返回 (should_sell, reason)。数据不足时仅用刚性。"""
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
        if security == CASH_ETF:
            continue   # 货币ETF不参与止损
        if security in g.perm_hold_done:
            continue   # v4.3 永久持仓不参与止损
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

        # --- v3.3 利润棘轮·水位更新（只用已发生的盘中现价，无未来函数）---
        hwm = g.high_watermark_dict.get(security, my_cost)
        if current_price > hwm:
            hwm = current_price
            g.high_watermark_dict[security] = hwm

        should_sell = False
        reason = ""

        # ---- 止损分支：有结构止损位(stop_ref)→结构止损+刚性+棘轮；无→MA止损 ----
        # v5：stop_ref 在两种情况下存在——① range 模式底背驰买；② trend 模式进取买
        #   (二买/三买低点)。两者都走"结构止损"路径（跌破买入信号低点=结构证伪），
        #   这是缠论最自然的退出，也是对"进取"仓位的硬约束。
        #   无 stop_ref 的情况——trend 模式底背驰买(AGGRESSIVE_BUY=False 退化)或
        #   跨模式遗留仓→回退 MA 均线止损兜底（v2 风格）。
        stop_ref = g.stop_ref_dict.get(security)
        if stop_ref is not None:
            # a) 结构止损（主力）：跌破30分钟买点信号笔低点（背驰/二买/三买证伪）。
            #    入场前已保证触发价距买入价 ≤ MAX_STOP_DISTANCE，故正常破位
            #    一定是它先于刚性线触发。
            if current_price < stop_ref * (1.0 - STRUCT_STOP_BUFFER):
                should_sell = True
                reason = "跌破信号笔低点 %.2f（结构止损·证伪）" % stop_ref
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
            # ---- MA 均线止损（= v2.0：-5%硬 + 跌破MA20 + 均线转弱）----
            # 趋势市无结构止损位的仓（底背驰买/跨模式遗留）用均线顺势止损：
            # 趋势一弱即走、躲深调。
            should_sell, reason = _ma_stop_triggered(security, current_price, total_pnl_ratio)

        if should_sell:
            log.warn("🚨【风控卖出】%s 原因: %s，当前盈亏 %.2f%%"
                     % (security, reason, total_pnl_ratio * 100))
            order_amount_in_chunks(security, -position.closeable_amount, current_price)
            g.trailing_stop_last_date[security] = current_date
            _post_exit_cleanup(security, current_date, is_loss=(total_pnl_ratio < 0))


# ----------------------------------------------------------------------
# 六·B：30分钟收盘时刻的缠论买卖点处理器
#   先扫持仓卖点止盈，再扫候选池买点直接买入。
#   候选池 ≤40 只（盘前日线买点确认过），单日30分钟重计算 ≤ 40×7=280 次。
# ----------------------------------------------------------------------

def _process_30m_signals(context, current_data, current_date):
    # ============ A) 卖点止盈：持仓30分钟顶背驰 + 中枢上方 ============
    for security in list(context.portfolio.positions.keys()):
        if security == CASH_ETF:
            continue   # 货币ETF不止盈/不卖出
        if security in g.perm_hold_done:
            continue   # v4.3 永久持仓不止盈
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

        fresh_sell = TREND_INTRADAY_FRESH if g.active_mode == 'trend' else INTRADAY_FRESH_BARS
        sig, zg, zd, _ = detect_beichi_signal(high, low, close,
                                              recent_bars=fresh_sell)
        if sig != 'sell':
            continue
        # 中枢位置过滤：现价在30分钟【未过期】中枢上方才算"高位止盈"。
        # 注意（v3.3）：卖出端有意保持 fail-open —— zg=None（无中枢/过期）
        # 时只要顶背驰成立即放行止盈。出口从宽是安全方向，与买入端的
        # fail-closed 不对称是设计使然（且利润棘轮已兜底）。
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
    # 熔断冻结期不开新仓（P2修复：真正生效）
    if g.frozen_today:
        return
    # v4.1：bear(熊市下行)状态不开新仓
    if g.active_mode == 'bear':
        return
    if g.today_buy_count >= MAX_BUYS_PER_DAY:
        return
    # 仓位管理：v4 trend 模式等权近满仓(不留现金)；range 模式留 CASH_RESERVE_RATIO 现金
    cash_reserve = TREND_CASH_RESERVE if g.active_mode == 'trend' else CASH_RESERVE_RATIO
    # 货币ETF市值并入预算，避免"现金全在511880→可用现金偏低→误判无钱→永不变现永不买"死锁
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
    # 确定性修复：candidate_pool 是 set，迭代顺序受 PYTHONHASHSEED 随机化。
    # 当多只票同刻触发 30 分钟底背驰、break 在 MAX_BUYS_PER_DAY 处截断时，
    # 随机序会导致每次回测挑中的票不同、资金曲线漂移。sorted 固定按代码序，
    # 使结果可复现（取舍：同刻超 5 只时取代码字典序最小者，而非随机子集）。
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

        fresh_buy = TREND_INTRADAY_FRESH if g.active_mode == 'trend' else INTRADAY_FRESH_BARS
        # v5：trend 模式且开启进取 → 用二买/三买(顺势回调)；否则用底背驰(一类，抄底)
        if g.active_mode == 'trend' and AGGRESSIVE_BUY:
            sig, kind, zg, zd, sig_low = detect_aggressive_buy(high, low, close,
                                                               recent_bars=fresh_buy)
            use_aggr = True
        else:
            sig, zg, zd, sig_low = detect_beichi_signal(high, low, close,
                                                        recent_bars=fresh_buy)
            use_aggr = False
            kind = '1st'
        if sig != 'buy':
            continue
        # 中枢位置过滤：trend 模式 fail-open（无中枢放行；有中枢且价>ZG才拒）；
        #   range 模式 fail-closed（无中枢或价>ZG都拒，看不清结构不买）。
        if g.active_mode == 'trend':
            if zg is not None and close[-1] > zg:
                continue
        else:
            if zg is None or close[-1] > zg:
                continue

        current_price = current_data[stock].last_price
        if np.isnan(current_price) or current_price <= 0:
            continue
        # 涨停不追：现价已封涨停则放弃（买不进且属情绪追高）
        if current_price >= current_data[stock].high_limit:
            continue

        # 入场前风险过滤：结构止损触发价距买入价 ≤ MAX_STOP_DISTANCE（控制单笔风险）。
        #   range 买 与 trend 进取买(二买/三买)都须做（保证结构止损先于刚性触发，
        #   且二买/三买低点可能离现价较远，需确认风险可控）；trend 底背驰买
        #   (退化/无stop_ref)跳过，改用 MA 止损。
        if use_aggr or g.active_mode != 'trend':
            if sig_low is None:
                continue
            struct_trigger = sig_low * (1.0 - STRUCT_STOP_BUFFER)
            if struct_trigger < current_price * (1.0 - MAX_STOP_DISTANCE):
                continue  # 结构位距现价 >5%，单笔风险过大，放弃该信号

        triggered_stocks.append((stock, current_price, sig_low, use_aggr, kind))

    # 【建仓】v4：trend 模式等权(budget/n，封顶 MAX_TRADE_VALUE 分单) / range 模式比例仓位(总资产×PER_STOCK_MAX_RATIO)
    #   用 spent 本地累计，规避 available_cash 在同一bar内可能滞后更新。
    if len(triggered_stocks) > 0:
        # 确有加仓目标 → 加仓前变现货币ETF（按需，避免无信号日白白损失隔夜利息）
        _redeem_cash_etf(context)
        is_trend = (g.active_mode == 'trend')
        n = len(triggered_stocks)
        # trend 等权：当批预算均分，但单票不超总资产×TREND_PER_STOCK_MAX_RATIO(防退化1只时全仓)；
        # range 比例：单只上限=总资产×PER_STOCK_MAX_RATIO，逐单实时重算剩余(始终留30%现金)。
        trend_eq = buy_budget / n
        trend_cap = context.portfolio.total_value * TREND_PER_STOCK_MAX_RATIO
        per_stock_cap = min(trend_eq, trend_cap) if is_trend \
                        else (context.portfolio.total_value * PER_STOCK_MAX_RATIO)
        spent = 0.0
        for stock, current_price, sig_low, use_aggr, kind in triggered_stocks:
            if g.today_buy_count >= MAX_BUYS_PER_DAY:
                break
            remaining = buy_budget - spent           # 本批剩余可用预算
            if remaining <= 10000:
                break
            target_value = min(per_stock_cap, remaining)  # 上限与剩余预算取小
            ordered_amount = order_buy_once(stock, target_value, current_price)
            if ordered_amount >= 100:
                spent += ordered_amount * current_price
                g.buy_cost_dict[stock] = current_price
                g.position_lock_stocks.add(stock)
                g.today_buy_count += 1
                g.candidate_pool.discard(stock)
                if use_aggr:
                    # v5 进取买(二买/三买)：存结构止损位(二买/三买低点)+棘轮水位起点。
                    #   stop_ref 存在 → _check_stop_loss 走结构止损路径（跌破=结构证伪）。
                    g.stop_ref_dict[stock] = sig_low
                    g.high_watermark_dict[stock] = current_price
                    log.info("🛒【trend·进取·%s买】%s 周线中枢上移+月多头+30分%s，现价 %.2f 买入 %d 股(≈%.0f元/等权%d只)，结构止损位 %.2f（距价%.1f%%）"
                             % (kind, stock, ('三买' if kind == '3rd' else '二买'),
                                current_price, ordered_amount,
                                ordered_amount * current_price, n,
                                sig_low, (current_price - sig_low) / current_price * 100))
                elif is_trend:
                    # trend 退化买(AGGRESSIVE_BUY=False)：不存结构止损位（止损改用 MA 均线）
                    log.info("🛒【trend·30分背驰直买】%s 月多头+30分底背驰，现价 %.2f 买入 %d 股(≈%.0f元/等权%d只)"
                             % (stock, current_price, ordered_amount,
                                ordered_amount * current_price, n))
                else:
                    # range 模式：存结构止损位(30分信号笔低点) + 利润棘轮水位起点
                    g.stop_ref_dict[stock] = sig_low
                    g.high_watermark_dict[stock] = current_price
                    log.info("🛒【range·30分背驰直买】%s 月多头+日线买点+30分底背驰+有效中枢下方，"
                             "现价 %.2f 买入 %d 股(≈%.0f元/上限%.0f)，结构止损位 %.2f（距价%.1f%%）"
                             % (stock, current_price, ordered_amount,
                                ordered_amount * current_price, per_stock_cap,
                                sig_low, (current_price - sig_low) / current_price * 100))


def _post_exit_cleanup(security, current_date, is_loss):
    """清仓善后：黑名单(v3.2区分盈亏) / 连亏计数与熔断 / 清缓存 / 解锁。"""
    if is_loss:
        g.consecutive_loss_count += 1
        # v4.1：仅 range 模式启用熔断；trend/bear 不冻结(trend连亏多为系统回调；bear本就不开新仓)。
        if g.active_mode == 'range' and g.consecutive_loss_count >= 5:
            g.freeze_days_left = 8
            g.consecutive_loss_count = 0   # 触发后清零，避免解冻当天立即复触
            log.error("💥 连续亏损5次，触发熔断，冻结买入8个交易日！")
        # 亏损出场黑名单：trend 平铺5天 / range 用 BLACKLIST_LOSS_DAYS(5)
        ban_days = TREND_BLACKLIST_DAYS if g.active_mode == 'trend' else BLACKLIST_LOSS_DAYS
        g.blacklist_dict[security] = (current_date, ban_days)
    else:
        g.consecutive_loss_count = 0
        # 止盈出场黑名单：trend 平铺5天 / range 只黑2天(保留二次背驰买点)
        ban_days = TREND_BLACKLIST_DAYS if g.active_mode == 'trend' else BLACKLIST_PROFIT_DAYS
        g.blacklist_dict[security] = (current_date, ban_days)

    g.buy_cost_dict.pop(security, None)
    g.stop_ref_dict.pop(security, None)
    g.high_watermark_dict.pop(security, None)   # v3.3 利润棘轮水位一并清理
    g.position_lock_stocks.discard(security)


# ======================================================================
# 七、盘后审计（15:30）：跌停废单拦截（与 s1 完全一致）
# ======================================================================

def _redeem_cash_etf(context):
    """加仓前变现：把持有的货币ETF(511880)全部转为可用现金，供建仓使用。
    非无条件每日变现——仅在确有加仓意图（triggered_stocks 非空）时由
    _process_30m_signals 调用，避免 bear/无信号日每天白白卖掉损失隔夜利息。"""
    pos = context.portfolio.positions.get(CASH_ETF)
    if pos is None or pos.closeable_amount <= 0:
        return
    try:
        order(CASH_ETF, -pos.closeable_amount)   # 清掉货币ETF，现金回到可用
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


def after_market_close(context):
    """复盘今日全部卖出单，凡因跌停流动性枯竭导致撤单/拒单且仍有实仓的，
    记录锁死价拖入补卖池，次日开盘继续死磕。"""
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
    # 盘后现金管理：闲余现金买入 511880 货币ETF 赚隔夜利息（不参与止损/止盈/跌停拦截）
    _invest_cash_etf(context, get_current_data())
