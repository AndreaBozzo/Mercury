"""Tests for core/schemas.py — all run without optional dependencies."""

from datetime import datetime

import pytest
from pydantic import ValidationError

from core.schemas import (
    L2Features,
    MarketEvent,
    OrderBookDelta,
    OrderBookSnapshot,
    TradeSignal,
)


def _signal(**overrides) -> dict:
    base = {
        "strategy_id": "test_v1",
        "symbol": "BTC/USDT",
        "action": "BUY",
        "order_type": "MARKET",
        "quantity": 0.01,
        "confidence_score": 0.8,
        "allocation_percentage": 5.0,
        "timestamp": datetime.now(),
    }
    return {**base, **overrides}


class TestMarketEvent:
    def test_minimal(self):
        ev = MarketEvent(source="nephtys", type="trade", timestamp=1_000_000, payload={})
        assert ev.source == "nephtys"
        assert ev.payload == {}

    def test_rich_payload(self):
        ev = MarketEvent(
            source="nephtys",
            type="ticker",
            timestamp=1_700_000_000_000,
            seq=42,
            payload={"symbol": "BTC/USDT", "price": 67_000.0, "volume": 1.5},
        )
        assert ev.payload["price"] == 67_000.0
        assert ev.seq == 42

    def test_missing_required_field(self):
        with pytest.raises(ValidationError):
            MarketEvent(source="x", type="trade", payload={})  # missing timestamp


class TestTradeSignal:
    def test_without_price(self):
        sig = TradeSignal(**_signal())
        assert sig.price is None

    def test_with_price(self):
        sig = TradeSignal(**_signal(price=67_000.0))
        assert sig.price == 67_000.0

    def test_default_confidence(self):
        sig = TradeSignal(**_signal())
        assert sig.confidence_score == 0.8

    def test_invalid_action_rejected(self):
        with pytest.raises(ValidationError):
            TradeSignal(**_signal(action="HOLD"))

    def test_non_positive_quantity_rejected(self):
        with pytest.raises(ValidationError):
            TradeSignal(**_signal(quantity=0))


class TestOrderBookSnapshot:
    def test_parses_bid_ask_lists(self):
        snap = OrderBookSnapshot(
            source="binance",
            symbol="BTC/USDT",
            seq=42,
            timestamp=1_700_000_000_000,
            bids=[[67_000.0, 1.5], [66_999.0, 2.0]],
            asks=[[67_001.0, 0.5], [67_002.0, 1.0]],
        )
        assert snap.bids[0][0] == 67_000.0
        assert len(snap.asks) == 2

    def test_empty_sides(self):
        snap = OrderBookSnapshot(
            source="x", symbol="ETH/USDT", seq=1, timestamp=0, bids=[], asks=[]
        )
        assert snap.bids == []


class TestOrderBookDelta:
    def test_bid_add(self):
        delta = OrderBookDelta(
            source="binance",
            symbol="BTC/USDT",
            seq=43,
            timestamp=1_000,
            side="bid",
            price=67_000.0,
            qty=2.0,
        )
        assert delta.side == "bid"
        assert delta.qty == 2.0

    def test_ask_remove(self):
        delta = OrderBookDelta(
            source="binance",
            symbol="BTC/USDT",
            seq=44,
            timestamp=1_001,
            side="ask",
            price=67_001.0,
            qty=0.0,
        )
        assert delta.qty == 0.0  # remove signal

    def test_invalid_side_rejected(self):
        with pytest.raises(ValidationError):
            OrderBookDelta(
                source="binance",
                symbol="BTC/USDT",
                seq=44,
                timestamp=1_001,
                side="buy",
                price=67_001.0,
                qty=0.0,
            )


class TestL2Features:
    def test_all_optional(self):
        feat = L2Features(symbol="BTC/USDT", timestamp=0)
        assert feat.mid is None
        assert feat.imbalance is None

    def test_fully_populated(self):
        feat = L2Features(
            symbol="BTC/USDT",
            timestamp=1_000,
            mid=67_000.5,
            spread=1.0,
            imbalance=0.3,
            best_bid_price=67_000.0,
            best_bid_qty=1.5,
            best_ask_price=67_001.0,
            best_ask_qty=0.8,
        )
        assert feat.imbalance == 0.3
