import time
import logging
import requests
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]

# Map Binance symbols to CoinGecko coin IDs to maintain consistency
SYMBOL_TO_COIN_ID = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "BNBUSDT": "binancecoin",
    "SOLUSDT": "solana",
    "XRPUSDT": "ripple"
}

class BinanceExtractor:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"
        self.session = requests.Session()
        
    def fetch_ohlcv(self, symbol: str, interval: str = "1h", limit: int = 24) -> list[dict]:
        """Fetch OHLCV candlestick data from Binance for a single symbol."""
        url = f"{self.base_url}/klines"
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        try:
            logger.info(f"Fetching {limit} candles ({interval}) for {symbol}...")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            results = []
            coin_id = SYMBOL_TO_COIN_ID.get(symbol, symbol.lower())
            
            for row in data:
                # Binance kline format:
                # [0: Open time, 1: Open, 2: High, 3: Low, 4: Close, 5: Volume, 6: Close time, ...]
                candle_ts = datetime.fromtimestamp(row[0] / 1000.0)
                results.append({
                    "coin_id": coin_id,
                    "exchange": "binance",
                    "interval": interval,
                    "open_price": float(row[1]),
                    "high_price": float(row[2]),
                    "low_price": float(row[3]),
                    "close_price": float(row[4]),
                    "volume": float(row[5]),
                    "candle_ts": candle_ts.isoformat()
                })
                
            logger.info(f"Successfully fetched {len(results)} candles for {symbol}.")
            return results
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch OHLCV for {symbol}. Error: {e}")
            return []

    def fetch_all_ohlcv(self, symbols: list = None, interval: str = "1h", limit: int = 24) -> list[dict]:
        """Fetch OHLCV for multiple symbols, looping with a small delay."""
        if symbols is None:
            symbols = DEFAULT_SYMBOLS
            
        all_results = []
        for symbol in symbols:
            results = self.fetch_ohlcv(symbol, interval, limit)
            all_results.extend(results)
            # Add a small delay between requests to be gentle on rate limits
            time.sleep(0.5)
            
        return all_results

if __name__ == "__main__":
    extractor = BinanceExtractor()
    print(f"--- Fetching 1h OHLCV for {DEFAULT_SYMBOLS} ---")
    data = extractor.fetch_all_ohlcv(limit=5) # fetch 5 candles for test
    for row in data[:10]: # Print first 10 rows
        print(row)
    print(f"Total rows fetched: {len(data)}")
