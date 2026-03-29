import os
import time
import logging
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

DEFAULT_COINS = ["bitcoin", "ethereum", "binancecoin", "solana", "ripple"]

class CoinGeckoExtractor:
    def __init__(self):
        self.api_key = os.getenv("COINGECKO_API_KEY")
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        
        # Retry with exponential backoff
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        # For public API with key, standard header is: x-cg-demo-api-key 
        # (or x-cg-pro-api-key if PRO plan). We will assume standard demo/public key.
        if self.api_key:
            self.session.headers.update({"x-cg-demo-api-key": self.api_key})

    def fetch_prices(self, coins: list = None) -> list[dict]:
        """Fetch current prices, market cap, and 24h volume/change."""
        if coins is None:
            coins = DEFAULT_COINS
            
        url = f"{self.base_url}/simple/price"
        params = {
            "ids": ",".join(coins),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true"
        }
        
        try:
            logger.info(f"Fetching prices for {len(coins)} coins...")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            results = []
            for coin_id, metrics in data.items():
                results.append({
                    "coin_id": coin_id,
                    "price_usd": metrics.get("usd"),
                    "market_cap_usd": metrics.get("usd_market_cap"),
                    "volume_24h_usd": metrics.get("usd_24h_vol"),
                    "price_change_24h_pct": metrics.get("usd_24h_change")
                })
            logger.info(f"Successfully fetched prices for {len(results)} coins.")
            return results
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch prices. Error: {e}")
            return []

    def fetch_metadata(self, coins: list = None) -> list[dict]:
        """Fetch metadata (rank, category, supply) for individual coins."""
        if coins is None:
            coins = DEFAULT_COINS
            
        results = []
        for coin_id in coins:
            url = f"{self.base_url}/coins/{coin_id}"
            params = {
                "localization": "false",
                "tickers": "false",
                "market_data": "true",
                "community_data": "false",
                "developer_data": "false",
                "sparkline": "false"
            }
            try:
                logger.info(f"Fetching metadata for {coin_id}...")
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                categories = data.get("categories", [])
                category = categories[0] if categories else "Unknown"
                
                market_data = data.get("market_data", {})
                
                results.append({
                    "coin_id": coin_id,
                    "symbol": data.get("symbol", "").upper(),
                    "name": data.get("name"),
                    "category": category,
                    "rank": data.get("market_cap_rank"),
                    "circulating_supply": market_data.get("circulating_supply")
                })
                logger.info(f"Successfully fetched metadata for {coin_id}.")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch metadata for {coin_id}. Status Code: {getattr(e.response, 'status_code', 'N/A')}. Error: {e}")
            
            # Sleep to respect rate limits
            time.sleep(1)
            
        return results

    def fetch_fear_greed(self) -> dict:
        """Fetch Fear & Greed Index from alternative.me."""
        url = "https://api.alternative.me/fng/"
        try:
            logger.info("Fetching Fear and Greed Index...")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data and "data" in data and len(data["data"]) > 0:
                item = data["data"][0]
                result = {
                    "value": int(item.get("value", 0)),
                    "value_classification": item.get("value_classification", "Unknown")
                }
                logger.info(f"Successfully fetched Fear and Greed Index: {result['value']} ({result['value_classification']})")
                return result
            else:
                logger.warning("Fear and Greed Index API returned empty data.")
                return {}
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch Fear and Greed Index. Error: {e}")
            return {}

if __name__ == "__main__":
    extractor = CoinGeckoExtractor()
    print("--- Prices ---")
    prices = extractor.fetch_prices()
    for p in prices:
        print(p)
        
    print("\n--- Fear & Greed ---")
    fng = extractor.fetch_fear_greed()
    print(fng)
    
    print("\n--- Metadata ---")
    metadata = extractor.fetch_metadata()
    for m in metadata:
        print(m)
