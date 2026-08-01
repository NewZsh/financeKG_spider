import pandas as pd
data = {
    "trade_date": ["2023-01-01", "2023-01-01", "2023-01-02", "2023-01-02"],
    "trade_timestamp": ["2023-01-01 09:30:00", "2023-01-01 09:31:00", "2023-01-02 09:30:00", "2023-01-02 09:31:00"],
    "open": [1,2,3,4], "high": [1,2,3,4], "low": [1,2,3,4], "close": [1,2,3,4], "volume": [10,20,30,40]
}
df = pd.DataFrame(data)
df["datetime"] = pd.to_datetime(df["trade_timestamp"])
df.set_index("datetime", inplace=True)
res = df.groupby("trade_date").resample("5min").agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}).dropna().reset_index()
print(res.columns)

