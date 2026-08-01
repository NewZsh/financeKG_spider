from fastapi import APIRouter, HTTPException, Query
import sqlite3
import pandas as pd
from typing import Optional
import os
from ..services import ta_calc
import numpy as np

router = APIRouter()

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data', 'stock.db'))

def apply_intraday_adjustment(df: pd.DataFrame, code: str, conn: sqlite3.Connection):
    # Fetch daily close to align intraday scale (¸´È¨)
    daily_query = "SELECT trade_date, close as daily_close FROM daily_bars WHERE code=? ORDER BY trade_date ASC"
    daily_df = pd.read_sql(daily_query, conn, params=(code,))
    if daily_df.empty: return df

    # Find last close of each day in intraday
    last_intra = df.groupby('trade_date')['close'].last().reset_index()
    last_intra.rename(columns={'close': 'intra_close'}, inplace=True)
    
    # Merge and compute daily factor
    merged = pd.merge(last_intra, daily_df, on='trade_date', how='left')
    merged['factor'] = merged['daily_close'] / merged['intra_close']
    
    # Fill missing factors backward then forward
    merged['factor'] = merged['factor'].bfill().ffill().fillna(1.0)
    
    factor_map = dict(zip(merged['trade_date'], merged['factor']))
    df['factor'] = df['trade_date'].map(factor_map).fillna(1.0)
    
    for col in ['open', 'high', 'low', 'close']:
        df[col] = df[col] * df['factor']
        df[col] = df[col].round(3)
        
    return df

@router.get('/{code}')
def get_ta_data(code: str, period: str = Query('day', pattern='^(day|week|month|m1|m5|m15|m30|m60)$')):
    try:
        conn = sqlite3.connect(DB_PATH)
        
        if period in ['day', 'week', 'month']:
            query = "SELECT trade_date as datetime, open, high, low, close, volume FROM daily_bars WHERE code = ? ORDER BY trade_date ASC"
            df = pd.read_sql(query, conn, params=(code,))
            conn.close()
            
            if df.empty:
                return {"error": "no data"}
                
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            
            if period == "week":
                df = df.resample('W-FRI').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
                df.reset_index(inplace=True)
                df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d')
            elif period == "month":
                df = df.resample('ME').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
                df.reset_index(inplace=True)
                df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d')
            else:
                df.reset_index(inplace=True)
                df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d')
        else:
            # Intraday (m1, m5, m15, m30, m60)
            query = "SELECT trade_date, trade_timestamp, open, high, low, close, volume FROM intraday_bars WHERE code = ? ORDER BY trade_timestamp ASC"
            df = pd.read_sql(query, conn, params=(code,))
            
            if df.empty:
                conn.close()
                return {"error": "no data"}
                
            # Handle split/dividend adjust
            df = apply_intraday_adjustment(df, code, conn)
            conn.close()
            
            df['datetime'] = pd.to_datetime(df['trade_timestamp'])
            df.set_index('datetime', inplace=True)
            
            resample_map = {"m1": "1min", "m5": "5min", "m15": "15min", "m30": "30min", "m60": "60min"}
            freq = resample_map[period]
            
            # Custom resample to group within each trading day safely
            if period != 'm1':
                # Group by trade_date and resample inside each day to avoid cross-day night gaps
                df = df.groupby('trade_date').resample(freq).agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna().reset_index()
                # reset_index makes 'trade_date' and 'datetime' as columns
            else:
                df.reset_index(inplace=True)
                
            df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M')
            
        # Calculate indicators
        df = ta_calc.macd(df.copy())
        df['ma5'] = ta_calc.ma(df, 5)
        df['ma20'] = ta_calc.ma(df, 20)
        df['ma60'] = ta_calc.ma(df, 60)
        df['rsi14'] = ta_calc.rsi(df, 14)
        df['k'], df['d'], df['j'] = ta_calc.kdj(df)
        df['boll_mid'], df['boll_up'], df['boll_dn'] = ta_calc.boll(df)
        
        # Chanlun requires index to be reset nicely
        bi = ta_calc.bi_list(df)
        du = ta_calc.duan_list(bi)
        zs = ta_calc.zhongshu_list(du if du else bi)
        bc = ta_calc.bei_chi(df, bi)
        pts = ta_calc.buy_sell_points(df, bi, du, zs)
        
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.fillna(0, inplace=True)
        
        return {
            "code": code,
            "period": period,
            "ohlcv": df[['datetime', 'open', 'high', 'low', 'close', 'volume']].to_dict(orient='records'),
            "indicators": df[['datetime', 'dif', 'dea', 'macd', 'ma5', 'ma20', 'ma60', 'rsi14', 'k', 'd', 'j', 'boll_mid', 'boll_up', 'boll_dn']].to_dict(orient='records'),
            "chanlun": {
                "bi": bi,
                "duan": du,
                "zhongshu": zs,
                "beichi": bc,
                "points": pts
            }
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
