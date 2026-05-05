from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.schemas import MarketEvent


@dataclass(frozen=True)
class BinanceDepthUpdate:
    symbol: str
    first_seq: int
    final_seq: int
    timestamp: int
    bids: list[list[float]]
    asks: list[list[float]]


def parse_binance_depth(event: MarketEvent) -> BinanceDepthUpdate | None:
    """
    Convert a Binance depthUpdate payload into an atomic L2 batch.

    Binance sends sequence ranges (`U`, `u`) for the whole update. Mercury must
    apply the full batch atomically; splitting each level into its own sequence
    would invent ordering that the exchange did not provide.
    """
    payload = event.payload
    if event.type != "depthUpdate" and payload.get("e") != "depthUpdate":
        return None

    symbol = _symbol(payload.get("s"))
    first_seq = _int(payload.get("U"))
    final_seq = _int(payload.get("u") or event.seq)

    if not symbol or first_seq is None or final_seq is None:
        return None

    return BinanceDepthUpdate(
        symbol=symbol,
        first_seq=first_seq,
        final_seq=final_seq,
        timestamp=_int(payload.get("E")) or event.timestamp,
        bids=_levels(payload.get("b")),
        asks=_levels(payload.get("a")),
    )


def _symbol(raw: Any) -> str:
    if not isinstance(raw, str) or not raw:
        return ""
    if "/" in raw:
        return raw
    if raw.endswith("USDT"):
        return f"{raw[:-4]}/USDT"
    return raw


def _int(raw: Any) -> int | None:
    try:
        if raw is None:
            return None
        return int(raw)
    except (TypeError, ValueError):
        return None


def _levels(raw: Any) -> list[list[float]]:
    if not isinstance(raw, list):
        return []

    levels: list[list[float]] = []
    for item in raw:
        if not isinstance(item, list | tuple) or len(item) < 2:
            continue
        try:
            price = float(item[0])
            qty = float(item[1])
        except (TypeError, ValueError):
            continue
        if price > 0 and qty >= 0:
            levels.append([price, qty])

    return levels
