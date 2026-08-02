# -*- coding: utf-8 -*-
"""拉沪深300 2021-2026 日K，画日线(含MA60)+周线图，标注各年末。"""
import urllib.request, json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import rcParams

rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial']
rcParams['axes.unicode_minus'] = False

URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh000300,day,2021-01-01,2026-08-01,2000,qfq"
req = urllib.request.Request(URL, headers={'User-Agent': 'Mozilla/5.0'})
data = json.loads(urllib.request.urlopen(req, timeout=20).read().decode('utf-8'))
klines = data['data']['sh000300'].get('qfqday') or data['data']['sh000300'].get('day')
dates = [k[0] for k in klines]
closes = [float(k[2]) for k in klines]
print("拉取 %d 根日K, %s ~ %s" % (len(closes), dates[0], dates[-1]))

# MA60
ma60 = []
for i in range(len(closes)):
    s = closes[max(0, i-59):i+1]
    ma60.append(sum(s)/len(s))

# 涨红跌绿（中国惯例）：收盘价上穿MA60标红、下穿标绿背景
x = list(range(len(closes)))

# ===== 日线图 =====
fig, ax = plt.subplots(figsize=(15, 6.5))
ax.plot(x, closes, color='#333333', linewidth=1.0, label='沪深300 收盘价')
ax.plot(x, ma60, color='#d9534f', linewidth=1.3, label='MA60')
# 标注各年末竖线
for y in [2021, 2022, 2023, 2024, 2025]:
    target = "%d-12-3" % y
    for i, d in enumerate(dates):
        if d.startswith(target):
            ax.axvline(i, color='#888888', linestyle='--', linewidth=0.6)
            ax.annotate("%d末\n%s\n基准累计%.1f%%" % (y, d[:10], closes[i]/closes[0]*100-100),
                        (i, closes[i]), fontsize=8, color='#444444',
                        xytext=(5, 10), textcoords='offset points')
            break
ax.set_title('沪深300 日线 2021-01 ~ 2026-07 (MA60) — 验证缠论策略各年市场环境', fontsize=13)
ax.set_xlabel('交易日序号')
ax.set_ylabel('点位')
ax.legend(loc='upper right')
ax.grid(alpha=0.3)
fig.tight_layout()
fig.savefig('D:/work/financeKG_spider/stock/doc/hs300_day.png', dpi=120)
print("日线图已保存: hs300_day.png")

# ===== 周线图（每5个交易日降采样）=====
wk_x = x[::5]
wk_c = closes[::5]
wk_d = dates[::5]
fig2, ax2 = plt.subplots(figsize=(15, 5.5))
ax2.plot(wk_x, wk_c, color='#333333', linewidth=1.2, label='沪深300 周收盘')
# 周线MA20
wk_ma20 = []
for i in range(len(wk_c)):
    s = wk_c[max(0, i-19):i+1]
    wk_ma20.append(sum(s)/len(s))
ax2.plot(wk_x, wk_ma20, color='#d9534f', linewidth=1.3, label='周MA20')
for y in [2021, 2022, 2023, 2024, 2025]:
    target = "%d-12-3" % y
    for i, d in enumerate(wk_d):
        if d.startswith(target):
            ax2.axvline(wk_x[i], color='#888888', linestyle='--', linewidth=0.6)
            ax2.annotate("%d末" % y, (wk_x[i], wk_c[i]), fontsize=9, color='#444444',
                         xytext=(5, 8), textcoords='offset points')
            break
ax2.set_title('沪深300 周线 2021-01 ~ 2026-07 (周MA20)', fontsize=13)
ax2.set_xlabel('交易周序号')
ax2.set_ylabel('点位')
ax2.legend(loc='upper right')
ax2.grid(alpha=0.3)
fig2.tight_layout()
fig2.savefig('D:/work/financeKG_spider/stock/doc/hs300_week.png', dpi=120)
print("周线图已保存: hs300_week.png")
