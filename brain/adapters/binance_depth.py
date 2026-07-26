from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any, Protocol

import requests

from core.order_book import OrderBook
from core.schemas import MarketEvent

BINANCE_REST_URL = os.getenv("BINANCE_REST_URL", "https://api.binance.com")
BINANCE_DEPTH_LIMIT = int(os.getenv("BINANCE_DEPTH_LIMIT", "1000"))
BINANCE_DEPTH_BOOTSTRAP_BUFFER = int(os.getenv("BINANCE_DEPTH_BOOTSTRAP_BUFFER", "2000"))


@dataclass(frozen=True)
class BinanceDepthUpdate:
    symbol: str
    first_seq: int
    final_seq: int
    timestamp: int
    bids: list[list[float]]
    asks: list[list[float]]


@dataclass(frozen=True)
class BinanceDepthSnapshot:
    symbol: str
    last_update_id: int
    bids: list[list[float]]
    asks: list[list[float]]


class SnapshotFetcher(Protocol):
    async def __call__(self, symbol: str) -> BinanceDepthSnapshot: ...


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


def fetch_binance_depth_snapshot(
    symbol: str,
    limit: int = BINANCE_DEPTH_LIMIT,
    base_url: str = BINANCE_REST_URL,
) -> BinanceDepthSnapshot:
    """Fetch a REST order book snapshot from Binance."""
    params: dict[str, str | int] = {"symbol": _binance_symbol(symbol), "limit": limit}
    response = requests.get(
        f"{base_url.rstrip('/')}/api/v3/depth",
        params=params,
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()

    return BinanceDepthSnapshot(
        symbol=symbol,
        last_update_id=int(payload["lastUpdateId"]),
        bids=_levels(payload.get("bids")),
        asks=_levels(payload.get("asks")),
    )


async def fetch_binance_depth_snapshot_async(symbol: str) -> BinanceDepthSnapshot:
    return await asyncio.to_thread(fetch_binance_depth_snapshot, symbol)


class BinanceDepthBootstrapper:
    """
    Bootstrap a Binance local order book from REST snapshot + buffered stream.

    Algorithm:
      1. Buffer live depthUpdate events.
      2. Fetch REST snapshot concurrently.
      3. Drop buffered updates where u <= lastUpdateId.
      4. Apply snapshot, then the first update satisfying
         U <= lastUpdateId + 1 <= u, followed by subsequent buffered updates.
    """

    def __init__(
        self,
        symbol: str,
        fetch_snapshot: SnapshotFetcher = fetch_binance_depth_snapshot_async,
        max_buffer: int = BINANCE_DEPTH_BOOTSTRAP_BUFFER,
    ) -> None:
        self.symbol = symbol
        self._fetch_snapshot = fetch_snapshot
        self._max_buffer = max_buffer
        self._buffer: list[BinanceDepthUpdate] = []
        self._snapshot: BinanceDepthSnapshot | None = None
        self._snapshot_task: asyncio.Task[BinanceDepthSnapshot] | None = None
        self._lock = asyncio.Lock()

    @property
    def is_active(self) -> bool:
        return self._snapshot is not None or self._snapshot_task is not None

    def reset(self) -> None:
        if self._snapshot_task is not None and not self._snapshot_task.done():
            self._snapshot_task.cancel()
        self._buffer.clear()
        self._snapshot = None
        self._snapshot_task = None

    async def handle_update(self, update: BinanceDepthUpdate, book: OrderBook) -> bool:
        async with self._lock:
            self._buffer.append(update)
            if len(self._buffer) > self._max_buffer:
                self.reset()
                self._buffer.append(update)

            if self._snapshot is None:
                if self._snapshot_task is None:
                    self._snapshot_task = asyncio.create_task(self._fetch_snapshot(self.symbol))
                    return False
                if not self._snapshot_task.done():
                    return False

                completed_task = self._snapshot_task
                self._snapshot_task = None
                # Clear the cached task before propagating a fetch failure.
                # The next delivery can then start a fresh snapshot instead of
                # re-reading the same failed Task forever.
                self._snapshot = completed_task.result()

            return self._apply_if_ready(book)

    def _apply_if_ready(self, book: OrderBook) -> bool:
        if self._snapshot is None:
            return False

        snapshot = self._snapshot
        target_seq = snapshot.last_update_id + 1

        self._buffer = [u for u in self._buffer if u.final_seq > snapshot.last_update_id]
        if not self._buffer:
            book.apply_snapshot(snapshot.bids, snapshot.asks, snapshot.last_update_id)
            self.reset()
            return True

        first = self._buffer[0]
        if first.first_seq > target_seq:
            self._restart_from_gap()
            return False

        if not (first.first_seq <= target_seq <= first.final_seq):
            return False

        book.apply_snapshot(snapshot.bids, snapshot.asks, snapshot.last_update_id)
        for buffered in self._buffer:
            ok = book.apply_delta_batch(
                bids=buffered.bids,
                asks=buffered.asks,
                first_seq=buffered.first_seq,
                final_seq=buffered.final_seq,
            )
            if not ok:
                self._restart_from_gap()
                return False

        self.reset()
        return True

    def _restart_from_gap(self) -> None:
        self._buffer.clear()
        self._snapshot = None
        self._snapshot_task = asyncio.create_task(self._fetch_snapshot(self.symbol))


def _symbol(raw: Any) -> str:
    if not isinstance(raw, str) or not raw:
        return ""
    if "/" in raw:
        return raw
    if raw.endswith("USDT"):
        return f"{raw[:-4]}/USDT"
    return raw


def _binance_symbol(symbol: str) -> str:
    return symbol.replace("/", "").upper()


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
