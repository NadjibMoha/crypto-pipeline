import os
import logging
import pandas as pd
import numpy as np
import ta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class IndicatorCalculator:
    def __init__(self):
        # Allow passing engine connection for dynamic connection config (e.g., from Airflow)
        conn_str = os.getenv("CRYPTO_DB_CONN", "postgresql+psycopg2://crypto_user:crypto_pass@localhost:5432/crypto_db")
        # In airflow it might use an airflow connection, but we can just use the ENV variable
        self.engine = create_engine(conn_str)

    def fetch_data(self) -> pd.DataFrame:
        """Fetch daily OHLCV and prices from the analytics.fact_prices table."""
        query = """
        SELECT 
            coin_id, 
            price_date as date, 
            open_price, 
            high_price, 
            low_price, 
            close_price, 
            volume 
        FROM analytics.fact_prices
        ORDER BY coin_id, date ASC;
        """
        try:
            logger.info("Fetching price data from the database...")
            df = pd.read_sql(query, self.engine)
            if df.empty:
                logger.warning("No data found in fact_prices.")
            return df
        except Exception as e:
            logger.error(f"Failed to fetch data: {e}")
            return pd.DataFrame()

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df['date'] = pd.to_datetime(df['date']).dt.date
        df = df.sort_values(by=['coin_id', 'date'])

        # Store results
        results = []

        for coin_id, group in df.groupby('coin_id'):
            group = group.copy()
            
            # Simple Moving Average (SMA)
            group['sma_7'] = ta.trend.sma_indicator(group['close_price'], window=7, fillna=True)
            
            # Exponential Moving Average (EMA)
            group['ema_7'] = ta.trend.ema_indicator(group['close_price'], window=7, fillna=True)
            
            # Relative Strength Index (RSI)
            group['rsi_14'] = ta.momentum.rsi(group['close_price'], window=14, fillna=True)
            
            # Bollinger Bands
            indicator_bb = ta.volatility.BollingerBands(close=group['close_price'], window=20, window_dev=2, fillna=True)
            group['bb_upper'] = indicator_bb.bollinger_hband()
            group['bb_middle'] = indicator_bb.bollinger_mavg()
            group['bb_lower'] = indicator_bb.bollinger_lband()
            
            # Volume Weighted Average Price (VWAP) - requires cumulative summation over a period (e.g., daily is just typical price)
            # Here we just use ta's VWAP, which usually expects high, low, close, volume. ta.volume.VolumeWeightedAveragePrice
            try:
                # ta library VWAP requires a window.
                indicator_vwap = ta.volume.VolumeWeightedAveragePrice(
                    high=group['high_price'], 
                    low=group['low_price'], 
                    close=group['close_price'], 
                    volume=group['volume'], 
                    window=14, 
                    fillna=True
                )
                group['vwap'] = indicator_vwap.volume_weighted_average_price()
            except Exception as e:
                # Fallback if ta version does not support vwap easily
                logger.warning(f"Error computing VWAP for {coin_id}: {e}")
                group['vwap'] = group['close_price']
            
            # Volatility (Rolling standard deviation of returns)
            returns = group['close_price'].pct_change()
            group['volatility'] = returns.rolling(window=24, min_periods=1).std().fillna(0)
            
            # Volume Spike (current vol / 7-day avg vol)
            avg_vol_7d = group['volume'].rolling(window=7, min_periods=1).mean()
            # Prevent division by zero
            avg_vol_7d = avg_vol_7d.replace(0, np.nan)
            group['volume_spike'] = (group['volume'] / avg_vol_7d).fillna(1.0)
            
            # Price change in percentage
            group['price_change_1d_pct'] = group['close_price'].pct_change(periods=1).fillna(0) * 100
            group['price_change_7d_pct'] = group['close_price'].pct_change(periods=7).fillna(0) * 100
            group['price_change_30d_pct'] = group['close_price'].pct_change(periods=30).fillna(0) * 100
            
            # Max/Min over period or just day
            # Since data is daily, we'll assign daily max/min natively from OHLC
            group['avg_price'] = (group['high_price'] + group['low_price'] + group['close_price']) / 3
            group['max_price'] = group['high_price']
            group['min_price'] = group['low_price']
            group['avg_volume'] = avg_vol_7d # Representing short-term volume avg
            
            results.append(group)
            
        if not results:
            return pd.DataFrame()
            
        final_df = pd.concat(results, ignore_index=True)
        return final_df

    def save_to_db(self, df: pd.DataFrame):
        if df.empty:
            logger.info("Empty DataFrame, nothing to save.")
            return

        # Prepare records for UPSERT
        records = df.to_dict(orient='records')
        
        # Build raw SQL upsert statement
        upsert_query = text("""
            INSERT INTO analytics.agg_daily (
                coin_id, date, avg_price, max_price, min_price, avg_volume, volatility, 
                sma_7, ema_7, rsi_14, bb_upper, bb_middle, bb_lower, vwap, volume_spike, 
                price_change_1d_pct, price_change_7d_pct, price_change_30d_pct
            ) VALUES (
                :coin_id, :date, :avg_price, :max_price, :min_price, :avg_volume, :volatility, 
                :sma_7, :ema_7, :rsi_14, :bb_upper, :bb_middle, :bb_lower, :vwap, :volume_spike, 
                :price_change_1d_pct, :price_change_7d_pct, :price_change_30d_pct
            )
            ON CONFLICT (coin_id, date) DO UPDATE SET
                avg_price = EXCLUDED.avg_price,
                max_price = EXCLUDED.max_price,
                min_price = EXCLUDED.min_price,
                avg_volume = EXCLUDED.avg_volume,
                volatility = EXCLUDED.volatility,
                sma_7 = EXCLUDED.sma_7,
                ema_7 = EXCLUDED.ema_7,
                rsi_14 = EXCLUDED.rsi_14,
                bb_upper = EXCLUDED.bb_upper,
                bb_middle = EXCLUDED.bb_middle,
                bb_lower = EXCLUDED.bb_lower,
                vwap = EXCLUDED.vwap,
                volume_spike = EXCLUDED.volume_spike,
                price_change_1d_pct = EXCLUDED.price_change_1d_pct,
                price_change_7d_pct = EXCLUDED.price_change_7d_pct,
                price_change_30d_pct = EXCLUDED.price_change_30d_pct;
        """)

        try:
            with self.engine.begin() as conn:
                for row in records:
                    conn.execute(upsert_query, row)
            logger.info(f"Successfully upserted {len(records)} indicator records into analytics.agg_daily.")
        except Exception as e:
            logger.error(f"Failed to save indicators: {e}")

if __name__ == "__main__":
    calc = IndicatorCalculator()
    data = calc.fetch_data()
    if not data.empty:
        logger.info(f"Fetched {len(data)} rows.")
        calc_data = calc.calculate_indicators(data)
        logger.info(f"Calculated indicators for {len(calc_data)} rows.")
        calc.save_to_db(calc_data)
    else:
        logger.info("No data fetched to process.")
