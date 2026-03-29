-- scripts/init_db.sql

-- 1. Create Data Warehouse Role and Database
CREATE USER crypto_user WITH PASSWORD 'crypto_pass';
CREATE DATABASE crypto_db OWNER crypto_user;

-- Connect to the newly created database
\c crypto_db

-- 2. Create Schemas
CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION crypto_user;
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION crypto_user;
CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION crypto_user;
CREATE SCHEMA IF NOT EXISTS reports AUTHORIZATION crypto_user;

-- ==========================================================
-- RAW SCHEMA
-- ==========================================================
CREATE TABLE IF NOT EXISTS raw.raw_prices (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(100),
    symbol VARCHAR(50),
    name VARCHAR(100),
    price_usd NUMERIC,
    market_cap_usd NUMERIC,
    volume_24h_usd NUMERIC,
    price_change_24h_pct NUMERIC,
    circulating_supply NUMERIC,
    fetched_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.raw_ohlcv (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(100),
    exchange VARCHAR(50),
    interval VARCHAR(10),
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    volume NUMERIC,
    candle_ts TIMESTAMP,
    fetched_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.raw_sentiment (
    id SERIAL PRIMARY KEY,
    value INTEGER,
    value_classification VARCHAR(50),
    fetched_at TIMESTAMP DEFAULT NOW()
);

-- ==========================================================
-- STAGING SCHEMA
-- ==========================================================
CREATE TABLE IF NOT EXISTS staging.stg_prices (
    coin_id VARCHAR(100),
    symbol VARCHAR(50),
    name VARCHAR(100),
    price_usd NUMERIC,
    market_cap_usd NUMERIC,
    volume_24h_usd NUMERIC,
    price_change_24h_pct NUMERIC,
    circulating_supply NUMERIC,
    fetched_at TIMESTAMP,
    UNIQUE (coin_id, fetched_at)
);
-- We use unique (coin_id, fetched_at) or cast fetched_at to date for uniqueness.
-- The user request: unique on (coin_id, DATE(fetched_at))
-- We can add a calculated column or constraint or just use fetched_date as a column.
ALTER TABLE staging.stg_prices ADD COLUMN fetched_date DATE GENERATED ALWAYS AS (DATE(fetched_at)) STORED;
ALTER TABLE staging.stg_prices ADD CONSTRAINT stg_prices_unique_coin_date UNIQUE (coin_id, fetched_date);

CREATE TABLE IF NOT EXISTS staging.stg_ohlcv (
    coin_id VARCHAR(100),
    exchange VARCHAR(50),
    interval VARCHAR(10),
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    volume NUMERIC,
    candle_ts TIMESTAMP,
    fetched_at TIMESTAMP,
    UNIQUE (coin_id, exchange, interval, candle_ts)
);

-- ==========================================================
-- ANALYTICS SCHEMA
-- ==========================================================
CREATE TABLE IF NOT EXISTS analytics.fact_prices (
    coin_id VARCHAR(100),
    price_date DATE,
    open_price NUMERIC,
    close_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    avg_price NUMERIC,
    volume NUMERIC,
    market_cap NUMERIC,
    price_change_pct NUMERIC,
    UNIQUE (coin_id, price_date)
);

CREATE TABLE IF NOT EXISTS analytics.dim_coins (
    coin_id VARCHAR(100) PRIMARY KEY,
    symbol VARCHAR(50),
    name VARCHAR(100),
    category VARCHAR(100),
    rank INTEGER,
    last_updated TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics.agg_daily (
    coin_id VARCHAR(100),
    date DATE,
    avg_price NUMERIC,
    max_price NUMERIC,
    min_price NUMERIC,
    avg_volume NUMERIC,
    volatility NUMERIC,
    market_cap_dominance_pct NUMERIC,
    fear_greed_score INTEGER,
    sma_7 NUMERIC,
    ema_7 NUMERIC,
    rsi_14 NUMERIC,
    bb_upper NUMERIC,
    bb_middle NUMERIC,
    bb_lower NUMERIC,
    vwap NUMERIC,
    volume_spike NUMERIC,
    price_change_1d_pct NUMERIC,
    price_change_7d_pct NUMERIC,
    price_change_30d_pct NUMERIC,
    UNIQUE (coin_id, date)
);

-- ==========================================================
-- REPORTS SCHEMA
-- ==========================================================
CREATE TABLE IF NOT EXISTS reports.rpt_top_movers (
    coin_id VARCHAR(100),
    symbol VARCHAR(50),
    price_change_24h_pct NUMERIC,
    volume_spike_ratio NUMERIC,
    calculated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS reports.rpt_market_summary (
    total_market_cap NUMERIC,
    btc_dominance_pct NUMERIC,
    total_volume_24h NUMERIC,
    fear_greed_score INTEGER,
    calculated_at TIMESTAMP
);

-- Pipeline Audit
CREATE TABLE IF NOT EXISTS analytics.pipeline_audit (
    run_id VARCHAR(200) PRIMARY KEY,
    run_at TIMESTAMP,
    status VARCHAR(50),
    records_fetched INTEGER,
    duration_seconds NUMERIC
);

-- ==========================================================
-- INDEXES
-- ==========================================================
CREATE INDEX IF NOT EXISTS idx_raw_prices_coin ON raw.raw_prices(coin_id, fetched_at);
CREATE INDEX IF NOT EXISTS idx_raw_ohlcv_coin ON raw.raw_ohlcv(coin_id, candle_ts);
CREATE INDEX IF NOT EXISTS IDX_stg_prices_coin ON staging.stg_prices(coin_id, fetched_date);
CREATE INDEX IF NOT EXISTS IDX_stg_ohlcv_coin ON staging.stg_ohlcv(coin_id, candle_ts);
CREATE INDEX IF NOT EXISTS IDX_fact_prices_coin ON analytics.fact_prices(coin_id, price_date);
CREATE INDEX IF NOT EXISTS IDX_agg_daily_coin ON analytics.agg_daily(coin_id, date);

-- ==========================================================
-- PRIVILEGES
-- ==========================================================
GRANT ALL PRIVILEGES ON DATABASE crypto_db TO crypto_user;
GRANT ALL PRIVILEGES ON SCHEMA raw TO crypto_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO crypto_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO crypto_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA reports TO crypto_user;
GRANT USAGE ON SCHEMA raw TO crypto_user;
GRANT USAGE ON SCHEMA staging TO crypto_user;
GRANT USAGE ON SCHEMA analytics TO crypto_user;
GRANT USAGE ON SCHEMA reports TO crypto_user;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA raw TO crypto_user;
