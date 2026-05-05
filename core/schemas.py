from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from core.symbols import normalize_symbol

# ---------------------------------------------------------------------------
# L1 — current Nephtys output (JSON StreamEvent envelope)
# ---------------------------------------------------------------------------


class MarketEvent(BaseModel):
    source: str
    type: str
    timestamp: int
    payload: dict[str, Any]
    seq: int | None = Field(default=None, ge=0)
    content_type: str | None = None


class TradeSignal(BaseModel):
    strategy_id: str
    symbol: str
    action: Literal["BUY", "SELL"]
    order_type: Literal["MARKET", "LIMIT"]
    quantity: float = Field(gt=0)
    price: float | None = Field(default=None, gt=0)
    confidence_score: float = Field(default=1.0, ge=0.0, le=1.0)
    allocation_percentage: float = Field(default=0.0, ge=0.0, le=100.0)
    timestamp: datetime

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        return normalize_symbol(value)


# ---------------------------------------------------------------------------
# L2 — order book events (expected when Nephtys adds L2 feed support)
#
# Integration notes:
#   - Live L2 deltas need a per-symbol exchange sequence. Broker sequence can
#     be used only when a subject contains one ordered book stream and no
#     unrelated events.
#   - Binary internal transport stays Mercury-local; upstream connectors may
#     continue to publish JSON at the ingestion boundary.
# ---------------------------------------------------------------------------


class OrderBookSnapshot(BaseModel):
    """Full L2 snapshot.  Expected MarketEvent.type == 'l2_snapshot'."""

    source: str
    symbol: str
    seq: int | None = Field(default=None, ge=0)
    timestamp: int = Field(ge=0)
    bids: list[list[float]]  # [[price, qty], ...]  descending
    asks: list[list[float]]  # [[price, qty], ...]  ascending

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        return normalize_symbol(value)


class OrderBookDelta(BaseModel):
    """Incremental L2 update.  Expected MarketEvent.type == 'l2_delta'.
    qty == 0.0 means the price level should be removed."""

    source: str
    symbol: str
    seq: int | None = Field(default=None, ge=0)
    timestamp: int = Field(ge=0)
    side: Literal["bid", "ask"]
    price: float = Field(gt=0)
    qty: float = Field(ge=0)

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        return normalize_symbol(value)


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

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        return normalize_symbol(value)
