import sqlite3
import pandas as pd
import sys
sys.path.append(r"d:\work\financeKG_spider\web_app\backend\services")
import ta_calc
import numpy as np

db=sqlite3.connect(r"d:\work\financeKG_spider\data\stock.db")
df = pd.read_sql("SELECT trade_date, trade_timestamp as datetime, open, high, low, close, volume FROM intraday_bars WHERE code='000001' ORDER BY trade_timestamp ASC", db)
df["datetime"] = pd.to_datetime(df["datetime"])
df.set_index("datetime", inplace=True)
df = df.groupby("trade_date").resample("5min").agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"}).dropna().reset_index()

print("Rows:", len(df))
df = ta_calc.macd(df.copy())
df.fillna(0, inplace=True)
bi = ta_calc.bi_list(df)
du = ta_calc.duan_list(bi)
zs = ta_calc.zhongshu_list(du if du else bi)
pts = ta_calc.buy_sell_points(df, bi, du, zs)
print("Points:", len(pts))

