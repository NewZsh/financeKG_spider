import sqlite3
import pandas as pd
db=sqlite3.connect(r"d:\work\financeKG_spider\data\stock.db")
daily=pd.read_sql("SELECT trade_date, close FROM daily_bars WHERE code='000001' ORDER BY trade_date DESC LIMIT 5", db)
intra=pd.read_sql("SELECT trade_date, close FROM intraday_bars WHERE code='000001' AND trade_time='15:00:00' ORDER BY trade_date DESC LIMIT 5", db)
print("Daily:\n", daily)
print("Intra:\n", intra)

