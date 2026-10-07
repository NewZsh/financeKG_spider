# 交易策略与回测优化计划

> 修订记录：2026-09-27，从"规则堆叠"范式升级为"因子 + 模型 + 规则执行"范式。

## 一、问题诊断：为什么"规则堆叠"不可持续

### 1.1 当前策略 (`s.py` / `s1.py`) 的本质

| 模块 | 代码位置 | 拍脑袋参数 | 客观依据 |
|---|---|---|---|
| 突破日阳线涨幅阈值 | `s1.py:351` | `> 4%` | ❌ 无 |
| 量能 Z-Score 阈值 | `s1.py:352` | `> 1.2` | ❌ 经验值 |
| 多头排列 | `s1.py:350` | `close > MA5 > MA10` | ❌ 未做 IC 检验 |
| 反包触发 | `s1.py:514` | `current_price > t1_open` | ❌ 未做 IC 检验 |
| 止盈线 10%/30% | `s1.py:622-626` | `max_pnl * 0.70 / 0.60` | ❌ 经验值 |
| 止损 -5% | `s1.py:641` | 硬编码 | ❌ 经验值 |
| 14:57 结算 | `s1.py:546` | 时间点 | ❌ 不在因子框架内 |
| 观察池容量 50/500 | `s.py:188` / `s1.py:332` | 硬编码 | ❌ 无敏感性分析 |
| 池内最大存活期 age 15 | `s1.py:302` | 硬编码 | ❌ 无半衰期数据支撑 |

这 8 个阈值都在代码里被硬编码，每改一个都靠人脑猜"这个值更合理"，**本质是规则引擎，不是因子引擎**。

### 1.2 现状的核心缺陷

1. **参数未经客观检验**：`Z-Score > 1.2` 还是 `> 1.5` 更好？没有 IC 数据支撑。
2. **指标间关系靠人脑拼接**：`is_ma_ok and is_price_ok and is_vol_ok` 是布尔逻辑硬连接,而不是因子加权合成。无法判断哪个因子的边际贡献最大。
3. **止盈止损一刀切**：固定回撤 70%/60% 在不同 Regime 下未必最优。
4. **缺失效监测**：策略上线后无 IC 滚动监控,失效了只能等亏损才知道。
5. **无过拟合防护**:每个阈值都在样本内反复调,选定后回报偏乐观。

### 1.3 但工程底座是优秀的

`s1.py` 的几个机制必须保留,**不要推倒重来**：
- 主板池过滤（非 ST/非创业科创北交所/上市≥180 天）
- `watch_pool` 观察池 + `age` 衰减机制
- `pending_exit_stocks` 死磕池（跌停锁死的"被动持仓"处理）
- `trailing_stop_last_date` 同日卖出保护
- 持仓锁、黑名单、连续亏损熔断

**这些是"执行层"的工程能力,与因子无关,无需重写。**重写的只是"信号层"——把"哪只股买、什么时候卖"的决策来源,从人工规则替换为**因子打分 + 模型合成**。

## 二、范式转换:从"规则"到"因子 + 模型 + 规则执行"

### 2.1 范式对照

| 维度 | 当前范式（规则堆叠） | 目标范式（因子+模型+规则执行） |
|---|---|---|
| 入场信号 | `MA多头 AND 涨幅>4% AND Z>1.2` | 多因子打分 Top-N,其中每个因子都通过 IC 检验 |
| 出场信号 | 价格上破昨开 + 止盈/止损线 | 因子失效（IC 衰减）+ 触发风控规则（计分衰减+行情反向） |
| 参数确定 | 人工经验调整 | 数据驱动 + 敏感性扫描 + walk-forward 选定 |
| 性能验证 | 单次回测年化收益 | IC/ICIR/分层夏普/PBO/DSR 多维指标 |
| 风险控制 | 规则化止损 | 信号层 + 风控层双重,模型不参与下单只产生方向 |

### 2.2 三层架构

```
┌────────────────────────────────────────────────┐
│  信号层 (Factor + Model)                         │
│  ──────────────────────────                      │
│  因子库 → 单因子检验 → 多因子合成 → Top-N 信号 │
│  ↓ 输出: 每日每只股票的"买入分"                 │
├────────────────────────────────────────────────┤
│  执行层 (现有 s1.py 工程底座)                    │
│  ──────────────────────────                      │
│  主板池 → 观察池 → 反包触发 → 持仓锁定          │
│  ↓ 输出: 实际下单动作                            │
├────────────────────────────────────────────────┤
│  风控层 (现有死磕池/熔断器等保留)                │
│  ──────────────────────────                      │
│  跌停防御 / 连亏熔断 / 黑名单 / 移动止盈        │
└────────────────────────────────────────────────┘
```

## 三、第一步:把现有代码拆解为因子池

### 3.1 现有规则 → 因子清单映射

直接对 `s1.py` 中的硬编码条件做因子化重命名(用现有代码语义):

| 原规则 | 因子名 | 因子公式(草稿) | 维度 |
|---|---|---|---|
| `close > MA5` | `trend_above_ma5` | `close / MA5(close, 5) - 1` | 趋势 |
| `MA5 > MA10 > MA20` | `ma_aligned_bull` | 多头排列强度(0/1/2/3 计分) | 趋势 |
| `last_close/last_open > 1.04` | `day_return_intraday` | `(close - open) / open` | 动量 |
| `last_close/df['close'][-2] > 1.04` | `day_return_overnight` | `(close - prev_close) / prev_close` | 动量 |
| `vol_zscore > 1.2` | `vol_zscore_22d` | `(vol - mean(vol,22)) / std(vol,22)` | 量能 |
| `t1_close < t1_open`(昨阴) | `yesterday_bearish_body` | `1 if close[−1] < open[−1] else 0` | 形态 |
| `current_price > t1_open`(今价上破昨开) | `reclaim_open_up` | `1 if last_price > open[−1] else 0` | 反包 |
| 阴线砸盘量 vs 突破阳线量 | `shake_vs_break_vol` | `max(drop_vol_window) / max(raise_vol_window)` | 量价 |
| `MA20 > MA5` | `trend_breakdown` | `1 if MA20 > MA5` | 趋势反转 |
| 持仓盈亏率 | `pnl_ratio` | `(price - cost) / cost` | 风控 |
| `max_pnl`(历史最高浮盈) | `max_pnl` | `running_max(pnl_ratio)` | 风控 |
| 大盘趋势开关 | `market_regime` | `1 if hs300_close > MA60` | Regime |
| `age`(突破后天数) | `breakout_age` | 距突破日交易天数 | 时效 |

### 3.2 补充的因子家族(全市场基础因子库参考)

补齐现有代码缺失的常用因子家族(参考 Qlib Alpha158 的覆盖范围):

| 因子家族 | 代表因子 | 与现有策略的关系 |
|---|---|---|
| 量价 KDE | KMID = (close-open)/open; KLEN = (high-low)/open | 替代当前的"涨幅>4%"离散判定,改成连续分值 |
| 量能分布 | `OPEN0/MA5`、`STD25`、`CORR5/STD25` | 比单一 Z-Score 更完整的量能画像 |
| 反包因子 | `close vs max(open[-3:])` | 替代"上破昨开",改成多日休整后的反包 |
| 价量相关性(Alpha#1 思路) | `corr(close, log(volume+1), 5)` | 捕捉放量上涨的健康趋势 |
| 距前高空间 | `(close - max(high,20)) / max(high,20)` | 替代"距离 60 日高点空间"作为观察指标 |
| RSI + 量能修正 | `RSI + MFI` | 比纯 RSI 阈值更稳健 |
| 波动率结构 | realizesd_vol_5d / realized_vol_60d | 判断 Regime 切换 |

## 四、第二步:单因子检验流程

### 4.1 检验维度(参考 jqfactor_analyzer 与石川《因子投资》)

| 指标 | 计算方式 | 及格线 |
|---|---|---|
| Rank IC 均值 | 截面 rank 相关 | `|IC| > 0.03` |
| ICIR | `mean(IC) / std(IC)` | `|ICIR| > 0.5` |
| t 统计量 | IC 序列的 t 检验 | `|t| > 2` |
| 分层单调性 | Q5-Q1 收益显著、5 层严格单调 |omorphic |
| 多空年化收益 | Q5 减 Q1 的累计收益年化 | >10% |
| 换手率 | 因子值变化频率 | 越低越好,建议 <20% |
| IC 半衰期 | IC 衰减到初始值一半所需天数 | >5 日(短线策略可放宽) |

### 4.2 时间区间要求

- **覆盖完整牛熊周期**:至少 5 年,必须包括 2018 年、2022 年、2024 年等下行段,以避免动量因子在牛市段误判。
- **每个因子输出专门的检验报告**(月度 IC 时序图、分层累计收益、IC 衰减曲线),保留在 `doc/factor_lab/<factor_name>.html` 下。

### 4.3 工具选择

| 平台 | 用途 | 备注 |
|---|---|---|
| `jqfactor_analyzer`(本地) | 离线检验,出 IC/ICIR/分层收益报告 | pip 安装 |
| `alphalens`(Quantopian) | 国外原版,功能近似 jqfactor_analyzer | 已停维但仍可用 |
| `vnpy`/`hikyuu` | 与现有 SQLite 行情库对齐的本地回测 | 兼容 `sync_market_data.py` 输出 |

### 4.4 关键陷阱

1. **前视偏差**:年报数据必须按发布日对齐,而非报告期末。聚宽的 `get_fundamentals(date=context.previous_date)` 处理此问题。
2. **生存偏差**:股票池必须用 point-in-time,`get_index_stocks(date=t)`,否则会把已退市股票带进回测。
3. **未来函数**:因子计算只能取 `context.current_dt` 之前已收盘的数据。当前 `s1.py` 中 `include_now=True` 是为了结算而非选股,选股时必须 `include_now=False`。

## 五、第三步:多因子合成方案对比

### 5.1 候选合成方法

| 方法 | 描述 | 优点 | 缺点 | 适用场景 |
|---|---|---|---|---|
| 等权合成 | 所有有效因子值标准化后简单相加 | 简单、稳健、不易过拟合 | 没区分因子强弱 | 因子少(≤5)且相关性低 |
| IC 加权 | 按 IC 均值给每个因子赋权 | 自适应预测力差异 | 受 IC 噪声影响 | 基线方案 |
| ICIR 加权 | 按 ICIR 赋权 | 兼顾预测力和稳定 | 滚动窗口选择敏感 | 推荐 |
| 截面回归 | `next_return = β·F + ε`,用回归系数赋权 | 显著性可检验 | 线性假设 | 多因子模型基线 |
| LightGBM 树模型 | 用 Alpha158 因子做特征,下期收益做标签 | 自动捕捉非线性 | 黑箱、易过拟合 | 因子数 >20 时考虑 |

### 5.2 该项目推荐路线

**先做 ICIR 加权(线性,可解释) → 再尝试 LightGBM stacking 验证增量。**

理由:
1. 当前策略经验上已经有 5-6 个有效条件(多头排列、放量、反包等),先做线性合成可快速验证方向。
2. 树模型需要更多因子(至少 20+)和更长历史,当前因子数量还不足以支撑。
3. 线性合成可解释,能定位因子失效;树模型黑箱,在因子不足时容易记噪声。

## 六、第四步:防过拟合工程方案

### 6.1 验证方法对比

| 方法 | 描述 | 问题 |
|---|---|---|
| 标准 K-Fold | 随机打乱分折 | ❌ 假设 IID,金融时序违反 |
| Walk-Forward | 滚动训练(过去12月→预测下1月→滑动) | ❌ 单一路径,易受单段 regime 影响 |
| **Purged K-Fold** | 按时序分折 + 删除训练集中标签窗口与测试集重叠的样本 | ✅ 推荐,处理重叠标签泄漏 |
| **Combinatorial Purged CV (CPCV)** | 多组合路径,每条路径都 purge+embargo | ✅ 业界最佳,可统计 PBO |

### 6.2 过拟合量化指标

| 指标 | 含义 | 阈值 |
|---|---|---|
| **PBO (Probability of Backtest Overfitting)** | 回测最优策略样本外汇率 | <50% |
| **DSR (Deflated Sharpe Ratio)** | 考虑试验次数后的夏普修正 | >0.5 (实际可用线) |
| **CSCV (Combinatorially Symmetric Cross-Validation)** | 模型在不同路径上的稳定性 | 高为佳 |

### 6.3 关键纪律

1. **样本划分**:60% 训练 / 20% 验证 / 20% 测试,**测试集只用一次**,不可反复调参。
2. **训练窗口**:12-24 个月滚动,覆盖一个完整风格周期,避免用 5 年陈旧数据训练。
3. **超参数搜索**:只能在训练集内做;验证集调参=已经泄漏测试信息。
4. **最终保留期**(hold-out):最后一年的数据,模型和模型调整都不允许使用,作为生产部署前的最终验证。

## 七、第五步:失效监测与 Regime 切换

### 7.1 因子失效监测

| 指标 | 预警线 | 离线线 |
|---|---|---|
| 滚动 60 日 IC 均值 | <0.02 | <0.01 持续 30 天 |
| 滚动 ICIR | <0.3 | <0 |
| 分层单调性破缺率 | >30% | >50% |

实现方式:每月末更新每个因子的滚动 IC 报告,若连续 3 个月触发预警线,降权 50%;触发离线线则因子下线。

### 7.2 Regime 识别

| Regime | 识别信号 | 推荐因子组合 |
|---|---|---|
| 牛市 | HS300 > MA60 且 MA60 上行 | 动量、突破、量能 |
| 熊市 | HS300 < MA60 且 MA60 下行 | 反转、低波动、估值 |
| 震荡 | HS300 在 MA20 上下窄幅震荡 | 质量、换手率、特质波动率 |

**核心提示**:当前 `s1.py` 的"HS300 > MA60 才允许买入"本质是 Regime 开关。但只有"牛/非牛"两档,粒度太粗。建议扩展到三档并因子配比动态切换。

## 八、保留并升级现有的工程底座

**保留(s1.py 已有):**
- `get_main_board_pool()`:主板池过滤
- `watch_pool` + `age` 衰减:观察池机制,可复用
- `pending_exit_stocks`:跌停死磕池,工程价值高
- `blacklist_dict` + `consecutive_loss_count`:辅助风控
- 大单分块下单 `order_amount_in_chunks()`

**升级:**
- `before_market_open` 中的"突破扫描"部分要重写为因子计算模块
- `market_intraday` 中的"反包触发"要重写为因子分模型输出
- 风控参数(止盈线、止损线)从硬编码改为可配置,且支持按 Regime 切换

## 九、业界借鉴清单

### 9.1 论文(必读)

| 论文 | 作者/年份 | 核心价值 | URL |
|---|---|---|---|
| 101 Formulaic Alphas | Kakushadze, 2016 | 101 个纯量价因子公式,所有因子库的起点 | https://arxiv.org/abs/1601.00991 |
| Advances in Financial Machine Learning | Marcos López de Prado, 2018 | Purged K-Fold / CPCV / Triple Barrier / Meta-Labeling,防过拟合圣经 | 书 + https://quantrading.space/post.php?slug=purged-cross-validation |
| The Probability of Backtest Overfitting | Bailey et al., 2017 | PBO 定义与计算,判断回测是否可靠 | https://arxiv.org/abs/1409.3201 |
| Deflated Sharpe Ratio | Bailey & López de Prado, 2014 | 多次试验后夏普的修正 | https://arxiv.org/abs/1412.5062 |
| Fama-French 五因子模型 | Fama & French, 2015 | 市值/估值/盈利/投资/市场因子,A 股实证基础 | 华泰 2017 复现报告 |

### 9.2 开源项目(可直接复用)

| 项目 | 团队 | 核心能力 | URL |
|---|---|---|---|
| Qlib | 微软 | Alpha158 / Alpha360 因子集,完整研究流水线,LightGBM/LSTM/Transformer 训练框架 | https://github.com/microsoft/qlib |
| jqfactor_analyzer | 聚宽 | IC/ICIR/分层/换手离线分析,`create_full_tear_sheet()` 全景报告 | https://github.com/JoinQuant/jqfactor_analyzer |
| Alphalens(pypi) | Quantopian | 单因子检验经典工具,jqfactor_analyzer 就是基于它改的 | https://github.com/quantopian/alphalens |
| WorldQuant Alpha101 Python 实现 | yli188/WorldQuant_alpha101 | 101 个 alpha 的 Python 实现 | https://github.com/yli188/WorldQuant_alpha101 |
| hikyuu | fasionhong | C++ 内核 Python 接口的本地量化框架,适合与 SQLite 行情库结合 | https://github.com/fasiondog/hikyuu |
| Zipline-cn | 社区 | Quantopian Zipline 的中文社区维护分支 | https://github.com/zhang-yu-wei/zipline-cn |

### 9.3 国内券商研报系列

| 系列 | 团队 | 代表篇 |
|---|---|---|
| **华泰金工** | 林晓明团队 | 《多因子模型系列》《Fama-French 五因子模型 A 股实证》《因子择时与因子分配》;每月《因子有效性跟踪》(沪深300/中证500/中证1000/全A) |
| **东方证券金融工程** | 朱剑涛团队 | 《因子选股系列》《动态情景多因子 Alpha 模型 (DCA)》 |
| **广发金工** | 安宁宁/张超团队 | 《多因子 Alpha 模型》《因子衰减与因子合成》 |
| **国信金工** | 张欣慰/杨怡玲团队 | 《多因子选股体系》《特质波动率与特异度》 |

**研报使用建议**:华泰因子有效性月报可直接当作"当前市场哪些因子有效"的参考,对当前 `s1.py` 的 regime 开关是非常好的补充。

### 9.4 社区博客与教程

| 来源 | 内容 | URL |
|---|---|---|
| 聚宽社区《单因子检验全流程》 | 从换手率波动率因子入手,完整跑通 IC/分层回测 | https://www.joinquant.com/post/62185 |
| 聚宽社区《多因子模型(二)-因子检验》 | IC 均值/ICIR 阈值设定实战 | https://joinquant.com/post/13994 |
| FindTruman《JoinQuant 单因子分析教程》 | `analyze_factor` 函数详解与 IC 解读 | https://iris.findtruman.io/ai/tool/ai-quantitative-trading/e/joinquant/factor-ic-analysis-guide |
| 仓满量化《Alpha101 是什么》 | Alpha101 因子结构系统介绍 | https://www.quantfull.com/playbook/alpha101/foundations/alpha101 |
| QuantCraft《Factor & Alpha Trading Strategies》 | Qlib Alpha158 与 TopkDropoutStrategy 工程解析 | https://stratcraft.ai/it/algorithms/alpha-factor-strategies |
| CSDN《微软开源 AI 量化平台 Qlib 技术指南》 | Qlib 从数据到策略的全流程中文教程 | https://blog.csdn.net/weixin_30872577/article/details/164617917 |
| 大象 Tahoe《因子投资》(石川著) | 国内因子投资体系理论入门必读 | 书籍 |

### 9.5 直接可用的因子库参考

| 因子库 | 因子数量 | 特征 |
|---|---|---|
| Qlib Alpha158 | 158 | 价格/成交量/滚动统计,K线形态(KMID/KLEN/KMID2/KNHUP等),标准化窗口 |
| Qlib Alpha360 | 360 | 长窗口(360日),为CNN/LSTM设计 |
| WorldQuant Alpha101 | 101 | 纯公式化量价因子,pairwise corr ≈ 15.9%,适度分散 |
| 聚宽内置因子库 | 300+ | `jqfactor.get_all_factors()`,分style/basics/growth/quality/technical大类 |
| Alpha101 的 Qlib DSL 映射 | 101 | GitHub `seuzxh/quant-factor-skill/blob/main/references/alpha101-factors.md` |

## 十、落地路线图

### 阶段 1:因子化重构(1-2 周)

1. 在 `joinquant/` 下新建 `factors.py`,把上表 13 个候选因子统一实现为 `Factor` 子类
2. 用 jqfactor_analyzer 对每个因子做 5 年 IC 检验
3. 设定 ICIR > 0.5 / IC > 0.03 的及格线,过滤入库
4. 输出报告到 `doc/factor_lab/<factor>.html`

### 阶段 2:多因子合成(1 周)

5. 实现 ICIR 加权合成,与原规则版本对比回测
6. 测试集只跑一次,记录超额收益、最大回撤、Sharpe、换手率
7. 若 ICIR 加权超额收益为正,保留线性合成方案作为基线

### 阶段 3:防过拟合

8. 引入 Purged K-Fold (`tools.csv.PurgedKFold`),调整训练窗口至 12 个月
9. 用 PBO 量化指标判定是否过拟合(阈值 50%)
10. 加 DSR 修正(根据试验次数缩放夏普)

### 阶段 4:执行层接管

10. 把现有 `before_market_open/market_intraday` 的入场逻辑替换为"取因子打分 Top-N"
11. 移动止盈/止损参数提取为配置文件,支持 Regime 切换
12. 保留死磕池、黑名单、持仓锁等工程能力

### 阶段 5:失效监测

11. 每月末生成因子滚动 IC 报告
12. 触发预警线降权,触发离线下线
13. 加入 regime 识别模块,动态切换因子组合

## 附:关于现有"规则参数"的快赢验证建议

在完成完整因子化重构之前,可以先做以下两个低成本验证(在聚宽研究环境 1 天可跑完):

1. **阈值敏感性扫描**:固定其他参数,单独让 `Z-Score` 阈值在 [0.8, 1.0, 1.2, 1.5, 2.0] 间扫描,看年化收益曲线是否平滑。如果 1.2 是局部峰值,说明这个阈值可能是运气。
2. **止盈线对比**:现版 10%→70% / 30%→60% 的阶梯,与"高点回撤 5% 立即出"和"破MA20 出"三种出场规则,在同一因子池下对比夏普与最大回撤。