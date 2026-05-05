from datetime import datetime
from typing import Any

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# L1 — current Nephtys output (JSON StreamEvent envelope)
# ---------------------------------------------------------------------------


class MarketEvent(BaseModel):
    source: str
    type: str
    timestamp: int
    payload: dict[str, Any]


class TradeSignal(BaseModel):
    strategy_id: str
    symbol: str
    action: str  # "BUY" | "SELL"
    order_type: str  # "MARKET" | "LIMIT"
    quantity: float
    price: float | None = None
    confidence_score: float = 1.0
    allocation_percentage: float = 0.0
    timestamp: datetime


# ---------------------------------------------------------------------------
# L2 — order book events (expected when Nephtys adds L2 feed support)
#
# Nephtys improvement notes (peer review, next month):
#   1. StreamEvent lacks a sequence number at the envelope level; Mercury must
#      fall back to NATS JetStream msg.metadata.sequence as the authoritative
#      sequencer.  Adding `seq: int64` to StreamEvent would be cleaner.
#   2. No "l2_snapshot" / "l2_delta" event types are defined in domain/event.go.
#      Nephtys would need to add a WebSocket L2 connector (e.g. Binance depth
#      stream) and emit these types for the order book to work in live trading.
#   3. Serialisation is JSON-only; switching to Arrow IPC for binary streams
#      would enable true zero-copy deserialization at Mercury's ingestion edge.
# ---------------------------------------------------------------------------


class OrderBookSnapshot(BaseModel):
    """Full L2 snapshot.  Expected MarketEvent.type == 'l2_snapshot'."""

    source: str
    symbol: str
    seq: int
    timestamp: int
    bids: list[list[float]]  # [[price, qty], ...]  descending
    asks: list[list[float]]  # [[price, qty], ...]  ascending


class OrderBookDelta(BaseModel):
    """Incremental L2 update.  Expected MarketEvent.type == 'l2_delta'.
    qty == 0.0 means the price level should be removed."""

    source: str
    symbol: str
    seq: int
    timestamp: int
    side: str  # "bid" | "ask"
    price: float
    qty: float


class L2Features(BaseModel):
    """Derived microstructure features emitted after each order book update."""

    symbol: str
    timestamp: int
    mid: float | None = None
    spread: float | None = None
    imbalance: float | None = None
    best_bid_price: float | None = None
    best_bid_qty: float | None = None
    best_ask_price: float | None = None
    best_ask_qty: float | None = None
