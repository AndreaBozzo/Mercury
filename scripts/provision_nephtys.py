import logging
import os
import time

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("provision_nephtys")

NEPHTYS_API_URL = os.getenv("NEPHTYS_API_URL", "http://localhost:3000/v1/streams")


STREAMS = [
    {
        "id": "binance_btc",
        "kind": "websocket",
        "url": "wss://stream.binance.com:9443/ws/btcusdt@trade",
        "topic": "nephtys.stream.crypto.btc",
        "pipeline": {
            "filter": {"match_types": ["trade"]},
            "transform": {"mapping": {"price": "p", "symbol": "s"}},
            "dedup": {"enabled": True, "ttl": "1m"},
            "enrich": {"tags": {"env": "prod"}},
        },
    },
    {
        "id": "binance_btc_depth",
        "kind": "websocket",
        "url": "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms",
        "topic": "nephtys.stream.crypto.btc",
        "pipeline": {
            "filter": {"match_types": ["depthUpdate"]},
            "enrich": {"tags": {"env": "prod"}},
        },
    },
]


def register_stream(payload: dict) -> bool:
    stream_id = payload["id"]
    logger.info(
        "Attempting to register stream %r with Nephtys at %s...",
        stream_id,
        NEPHTYS_API_URL,
    )

    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = requests.post(NEPHTYS_API_URL, json=payload)
            if response.status_code in [200, 201]:
                logger.info("Stream %r successfully registered.", stream_id)
                return True
            elif response.status_code == 409:
                logger.info("Stream %r already exists in Nephtys.", stream_id)
                return True
            else:
                logger.warning(
                    "Failed to register stream %r. Status: %s, Response: %s",
                    stream_id,
                    response.status_code,
                    response.text,
                )
        except requests.exceptions.ConnectionError:
            logger.warning(
                "Connection to Nephtys failed (attempt %s/%s). Retrying in 5 seconds...",
                attempt + 1,
                max_retries,
            )

        time.sleep(5)

    logger.error("Could not provision Nephtys stream %r after multiple attempts.", stream_id)
    return False


def register_binance_btc_streams() -> None:
    ok = True
    for payload in STREAMS:
        ok = register_stream(payload) and ok
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    register_binance_btc_streams()
