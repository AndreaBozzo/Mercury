import logging
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("provision_nephtys")

NEPHTYS_API_URL = os.getenv("NEPHTYS_API_URL", "http://localhost:3002/v1/streams")
NEPHTYS_ADMIN_TOKEN = os.getenv("NEPHTYS_ADMIN_TOKEN", "")


STREAM_PROFILES: dict[str, list[dict]] = {
    # Public, unauthenticated feed. This is the observation-alpha default,
    # avoids an implicit Binance dependency, and intentionally uses a EUR quote.
    "coinbase_btc_eur": [
        {
            "id": "coinbase_btc_eur_ticker",
            "kind": "websocket",
            "url": "wss://ws-feed.exchange.coinbase.com",
            "topic": "nephtys.stream.crypto.btc",
            "websocket": {
                "on_connect_send": (
                    '{"type":"subscribe","product_ids":["BTC-EUR"],"channels":["ticker"]}'
                )
            },
            "pipeline": {
                "transform": {
                    "mapping": {
                        "price": "price",
                        "qty": "last_size",
                        "volume": "last_size",
                        "symbol": "product_id",
                        "best_bid": "best_bid",
                        "best_ask": "best_ask",
                        "sequence": "sequence",
                    }
                },
                "enrich": {"tags": {"venue": "coinbase", "kind": "ticker"}},
            },
        }
    ],
    # Legacy research profile. It remains available for historical parity but
    # is never selected implicitly in the EU-oriented public configuration.
    "binance_btc_usdt": [
        {
            "id": "binance_btc",
            "kind": "websocket",
            "url": "wss://stream.binance.com:9443/ws/btcusdt@trade",
            "topic": "nephtys.stream.crypto.btc",
            "pipeline": {
                "filter": {"match_types": ["trade"]},
                "transform": {"mapping": {"price": "p", "symbol": "s"}},
                "dedup": {"enabled": True, "ttl": "1m"},
                "enrich": {"tags": {"venue": "binance", "kind": "trade"}},
            },
        },
        {
            "id": "binance_btc_depth",
            "kind": "websocket",
            "url": "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms",
            "topic": "nephtys.stream.crypto.btc",
            "pipeline": {
                "filter": {"match_types": ["depthUpdate"]},
                "enrich": {"tags": {"venue": "binance", "kind": "depth"}},
            },
        },
    ],
}


def selected_streams(profile: str | None = None) -> list[dict]:
    selected = profile or os.getenv("MERCURY_MARKET_DATA_PROFILE", "coinbase_btc_eur")
    try:
        return STREAM_PROFILES[selected]
    except KeyError as exc:
        choices = ", ".join(sorted(STREAM_PROFILES))
        raise ValueError(
            f"Unknown MERCURY_MARKET_DATA_PROFILE={selected!r}; choose one of: {choices}"
        ) from exc


def register_stream(payload: dict) -> bool:
    stream_id = payload["id"]
    headers = {}
    if NEPHTYS_ADMIN_TOKEN:
        headers["Authorization"] = f"Bearer {NEPHTYS_ADMIN_TOKEN}"

    logger.info(
        "Attempting to register stream %r with Nephtys at %s...",
        stream_id,
        NEPHTYS_API_URL,
    )

    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = requests.post(
                NEPHTYS_API_URL,
                json=payload,
                headers=headers,
                timeout=10,
            )
            if response.status_code in {200, 201}:
                logger.info("Stream %r successfully registered.", stream_id)
                return True
            if response.status_code == 409:
                logger.info("Stream %r already exists in Nephtys.", stream_id)
                return True
            logger.warning(
                "Failed to register stream %r. Status: %s, Response: %s",
                stream_id,
                response.status_code,
                response.text,
            )
        except requests.exceptions.RequestException as exc:
            logger.warning(
                "Nephtys request failed (attempt %s/%s): %s",
                attempt + 1,
                max_retries,
                exc,
            )

        if attempt + 1 < max_retries:
            time.sleep(5)

    logger.error("Could not provision Nephtys stream %r after multiple attempts.", stream_id)
    return False


def register_market_streams(profile: str | None = None) -> None:
    ok = True
    for payload in selected_streams(profile):
        ok = register_stream(payload) and ok
    if not ok:
        raise SystemExit(1)


def register_binance_btc_streams() -> None:
    """Compatibility wrapper for private deployments using the legacy profile."""
    register_market_streams("binance_btc_usdt")


if __name__ == "__main__":
    register_market_streams()
