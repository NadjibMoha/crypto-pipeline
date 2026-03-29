import os
import json
from datetime import datetime, timedelta
import logging
from typing import Dict, Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from sqlalchemy import create_engine, text

# Import extractors and transforms from PYTHONPATH mounted at /opt/airflow
from extractors.coingecko import CoinGeckoExtractor
from extractors.binance import BinanceExtractor
from transforms.indicators import IndicatorCalculator

# DB Connection
DB_CONN = os.getenv("CRYPTO_DB_CONN")
engine = create_engine(DB_CONN) if DB_CONN else None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'crypto_pipeline',
    default_args=default_args,
    description='A complete ELT pipeline for crypto data',
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
)

def extract_prices_func(**kwargs) -> int:
    """Fetch prices and fear/greed, save to raw schema, return record counts."""
    ti = kwargs['ti']
    if not engine:
        raise ValueError("Database connection CRYPTO_DB_CONN not found.")

    cg = CoinGeckoExtractor()
    prices = cg.fetch_prices()
    metadata = cg.fetch_metadata()
    fng = cg.fetch_fear_greed()
    
    records_fetched = 0
    with engine.begin() as conn:
        # Insert prices
        for p in prices:
            stmt = text("""
                INSERT INTO raw.raw_prices 
                (coin_id, symbol, name, price_usd, market_cap_usd, volume_24h_usd, price_change_24h_pct, circulating_supply)
                VALUES (:coin_id, :symbol, :name, :price_usd, :market_cap_usd, :volume_24h_usd, :price_change_24h_pct, :circulating_supply)
            """)
            # Find matching metadata to get symbol, name, supply
            meta = next((m for m in metadata if m['coin_id'] == p['coin_id']), {})
            p['symbol'] = meta.get('symbol', p['coin_id'][:3].upper())
            p['name'] = meta.get('name', p['coin_id'].capitalize())
            p['circulating_supply'] = meta.get('circulating_supply')
            
            conn.execute(stmt, p)
            records_fetched += 1
            
        # Insert Fear and Greed
        if fng:
            stmt = text("""
                INSERT INTO raw.raw_sentiment (value, value_classification)
                VALUES (:value, :value_classification)
            """)
            conn.execute(stmt, fng)
            records_fetched += 1
            
    # Push metrics
    ti.xcom_push(key='extract_prices_count', value=records_fetched)
    return records_fetched

def extract_ohlcv_func(**kwargs) -> int:
    """Fetch OHLCV data, save to raw schema."""
    ti = kwargs['ti']
    binance = BinanceExtractor()
    # 1h interval, up to 2 candles (current and last closed)
    data = binance.fetch_all_ohlcv(interval="1h", limit=2)
    
    records_fetched = 0
    with engine.begin() as conn:
        for row in data:
            stmt = text("""
                INSERT INTO raw.raw_ohlcv
                (coin_id, exchange, interval, open_price, high_price, low_price, close_price, volume, candle_ts)
                VALUES (:coin_id, :exchange, :interval, :open_price, :high_price, :low_price, :close_price, :volume, :candle_ts)
            """)
            conn.execute(stmt, row)
            records_fetched += 1
            
    ti.xcom_push(key='extract_ohlcv_count', value=records_fetched)
    return records_fetched

def calc_indicators_func(**kwargs):
    """Fetch stg/fact data and calculate technical indicators."""
    calc = IndicatorCalculator()
    data = calc.fetch_data()
    if not data.empty:
        calc_data = calc.calculate_indicators(data)
        calc.save_to_db(calc_data)

def log_run_func(**kwargs):
    """Log the DAG run to pipeline_audit table."""
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    run_id = dag_run.run_id
    run_at = dag_run.start_date
    duration = (datetime.now(run_at.tzinfo) - run_at).total_seconds()
    status = "SUCCESS"
    
    p_count = ti.xcom_pull(task_ids='extract_prices', key='extract_prices_count') or 0
    c_count = ti.xcom_pull(task_ids='extract_ohlcv', key='extract_ohlcv_count') or 0
    total_records = p_count + c_count
    
    with engine.begin() as conn:
        stmt = text("""
            INSERT INTO analytics.pipeline_audit (run_id, run_at, status, records_fetched, duration_seconds)
            VALUES (:run_id, :run_at, :status, :records_fetched, :duration_seconds)
            ON CONFLICT (run_id) 
            DO UPDATE SET 
                status = EXCLUDED.status, 
                records_fetched = EXCLUDED.records_fetched, 
                duration_seconds = EXCLUDED.duration_seconds
        """)
        conn.execute(stmt, {
            "run_id": run_id, 
            "run_at": run_at, 
            "status": status, 
            "records_fetched": total_records, 
            "duration_seconds": duration
        })

start_task = EmptyOperator(task_id='start', dag=dag)
end_task = EmptyOperator(task_id='end', dag=dag)

t_extract_prices = PythonOperator(
    task_id='extract_prices',
    python_callable=extract_prices_func,
    dag=dag,
)

t_extract_ohlcv = PythonOperator(
    task_id='extract_ohlcv',
    python_callable=extract_ohlcv_func,
    dag=dag,
)

# Load Staging: Deduplicate data and move to staging
sql_load_staging = """
    -- Clean Prices
    INSERT INTO staging.stg_prices (
        coin_id, symbol, name, price_usd, market_cap_usd, volume_24h_usd, price_change_24h_pct, circulating_supply, fetched_at
    )
    SELECT DISTINCT ON (coin_id, DATE(fetched_at))
        coin_id, symbol, name, price_usd, market_cap_usd, volume_24h_usd, price_change_24h_pct, circulating_supply, fetched_at
    FROM raw.raw_prices
    ORDER BY coin_id, DATE(fetched_at), fetched_at DESC
    ON CONFLICT (coin_id, fetched_date) DO UPDATE
    SET 
        price_usd = EXCLUDED.price_usd,
        market_cap_usd = EXCLUDED.market_cap_usd,
        volume_24h_usd = EXCLUDED.volume_24h_usd;

    -- Clean OHLCV
    INSERT INTO staging.stg_ohlcv (
        coin_id, exchange, interval, open_price, high_price, low_price, close_price, volume, candle_ts, fetched_at
    )
    SELECT DISTINCT ON (coin_id, exchange, interval, candle_ts)
        coin_id, exchange, interval, open_price, high_price, low_price, close_price, volume, candle_ts, fetched_at
    FROM raw.raw_ohlcv
    ORDER BY coin_id, exchange, interval, candle_ts, fetched_at DESC
    ON CONFLICT (coin_id, exchange, interval, candle_ts) DO UPDATE
    SET 
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume;
"""

t_load_staging = PythonOperator(
    task_id='load_staging',
    python_callable=lambda: engine.execute(text(sql_load_staging)) if False else None, 
    # Use python to execute SQL via SQLAlchemy engine directly to ensure it uses the env conn correctly
)

def run_sql_staging():
    with engine.begin() as conn:
        # SQLAlchemy requires separating statements unless multi=True. We can execute multiple texts
        conn.execute(text("""
            INSERT INTO staging.stg_prices (coin_id, symbol, name, price_usd, market_cap_usd, volume_24h_usd, price_change_24h_pct, circulating_supply, fetched_at)
            SELECT DISTINCT ON (coin_id, DATE(fetched_at)) coin_id, symbol, name, price_usd, market_cap_usd, volume_24h_usd, price_change_24h_pct, circulating_supply, fetched_at
            FROM raw.raw_prices
            ORDER BY coin_id, DATE(fetched_at), fetched_at DESC
            ON CONFLICT (coin_id, fetched_date) DO UPDATE SET price_usd = EXCLUDED.price_usd, market_cap_usd = EXCLUDED.market_cap_usd, volume_24h_usd = EXCLUDED.volume_24h_usd;
        """))
        conn.execute(text("""
            INSERT INTO staging.stg_ohlcv (coin_id, exchange, interval, open_price, high_price, low_price, close_price, volume, candle_ts, fetched_at)
            SELECT DISTINCT ON (coin_id, exchange, interval, candle_ts) coin_id, exchange, interval, open_price, high_price, low_price, close_price, volume, candle_ts, fetched_at
            FROM raw.raw_ohlcv
            ORDER BY coin_id, exchange, interval, candle_ts, fetched_at DESC
            ON CONFLICT (coin_id, exchange, interval, candle_ts) DO UPDATE SET close_price = EXCLUDED.close_price, volume = EXCLUDED.volume;
        """))

t_load_staging.python_callable = run_sql_staging

def run_sql_analytics():
    with engine.begin() as conn:
        # Populate dim_coins
        conn.execute(text("""
            INSERT INTO analytics.dim_coins (coin_id, symbol, name, rank, last_updated)
            SELECT DISTINCT ON (coin_id) coin_id, symbol, name, 0, fetched_at
            FROM staging.stg_prices
            ORDER BY coin_id, fetched_at DESC
            ON CONFLICT (coin_id) DO UPDATE SET last_updated = EXCLUDED.last_updated;
        """))
        # Populate fact_prices from OHLCV
        # Rollup 1h into daily
        conn.execute(text("""
            INSERT INTO analytics.fact_prices (
                coin_id, price_date, open_price, close_price, high_price, low_price, avg_price, volume
            )
            SELECT 
                coin_id,
                DATE(candle_ts) as price_date,
                (array_agg(open_price ORDER BY candle_ts ASC))[1] as open_price,
                (array_agg(close_price ORDER BY candle_ts DESC))[1] as close_price,
                MAX(high_price) as high_price,
                MIN(low_price) as low_price,
                AVG(close_price) as avg_price,
                SUM(volume) as volume
            FROM staging.stg_ohlcv
            GROUP BY coin_id, DATE(candle_ts)
            ON CONFLICT (coin_id, price_date) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                close_price = EXCLUDED.close_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                avg_price = EXCLUDED.avg_price,
                volume = EXCLUDED.volume;
        """))

t_load_analytics = PythonOperator(
    task_id='load_analytics',
    python_callable=run_sql_analytics,
    dag=dag,
)

t_calc_indicators = PythonOperator(
    task_id='calculate_indicators',
    python_callable=calc_indicators_func,
    dag=dag,
)

def run_sql_reports():
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE reports.rpt_top_movers;"))
        conn.execute(text("""
            INSERT INTO reports.rpt_top_movers (coin_id, symbol, price_change_24h_pct, volume_spike_ratio, calculated_at)
            SELECT p.coin_id, c.symbol, a.price_change_1d_pct, a.volume_spike, NOW()
            FROM analytics.agg_daily a
            JOIN analytics.dim_coins c ON a.coin_id = c.coin_id
            JOIN (
                SELECT coin_id, MAX(date) as max_date FROM analytics.agg_daily GROUP BY coin_id
            ) p ON a.coin_id = p.coin_id AND a.date = p.max_date
            ORDER BY a.price_change_1d_pct DESC LIMIT 10;
        """))
        
        conn.execute(text("TRUNCATE TABLE reports.rpt_market_summary;"))
        conn.execute(text("""
            INSERT INTO reports.rpt_market_summary (total_market_cap, btc_dominance_pct, total_volume_24h, fear_greed_score, calculated_at)
            SELECT 
                SUM(p.market_cap_usd),
                100.0 * MAX(CASE WHEN p.coin_id='bitcoin' THEN p.market_cap_usd ELSE 0 END) / NULLIF(SUM(p.market_cap_usd), 0),
                SUM(p.volume_24h_usd),
                (SELECT value FROM raw.raw_sentiment ORDER BY fetched_at DESC LIMIT 1),
                NOW()
            FROM (
                SELECT DISTINCT ON (coin_id) coin_id, market_cap_usd, volume_24h_usd
                FROM staging.stg_prices
                ORDER BY coin_id, fetched_at DESC
            ) p;
        """))

t_refresh_reports = PythonOperator(
    task_id='refresh_reports',
    python_callable=run_sql_reports,
    dag=dag,
)

t_log_run = PythonOperator(
    task_id='log_run',
    python_callable=log_run_func,
    dag=dag,
)

# Set dependencies
start_task >> [t_extract_prices, t_extract_ohlcv] >> t_load_staging
t_load_staging >> t_load_analytics >> t_calc_indicators >> t_refresh_reports
t_refresh_reports >> t_log_run >> end_task
