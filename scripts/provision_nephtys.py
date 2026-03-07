import os
import time
import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("provision_nephtys")

NEPHTYS_API_URL = os.getenv("NEPHTYS_API_URL", "http://localhost:3000/v1/streams")

def register_binance_btc_stream():
    payload = {
        "id": "binance_btc",
        "kind": "websocket",
        "url": "wss://stream.binance.com:9443/ws/btcusdt@trade",
        "topic": "nephtys.stream.crypto.btc",
        "pipeline": {
            "filter": { "match_types": ["trade"] },
            "transform": { "mapping": { "price": "p", "symbol": "s" } },
            "dedup": { "enabled": True, "ttl": "1m" },
            "enrich": { "tags": { "env": "prod" } }
        }
    }
    
    logger.info(f"Attempting to register stream 'binance_btc' with Nephtys at {NEPHTYS_API_URL}...")
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = requests.post(NEPHTYS_API_URL, json=payload)
            if response.status_code in [200, 201]:
                logger.info("Stream successfully registered!")
                return
            elif response.status_code == 409:
                logger.info("Stream already exists in Nephtys.")
                return
            else:
                logger.warning(f"Failed to register stream. Status: {response.status_code}, Response: {response.text}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection to Nephtys failed (attempt {attempt + 1}/{max_retries}). Retrying in 5 seconds...")
        
        time.sleep(5)
        
    logger.error("Could not provision Nephtys stream after multiple attempts.")

if __name__ == "__main__":
    register_binance_btc_stream()
