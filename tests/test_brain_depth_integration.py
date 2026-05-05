import asyncio

import pytest

import brain.main as brain_main
from brain.adapters.binance_depth import BinanceDepthBootstrapper, BinanceDepthSnapshot
from brain.main import handle_market_event
from core.schemas import MarketEvent


class FakeJetStream:
    def __init__(self):
        self.published: list[tuple[str, bytes]] = []

    async def publish(self, subject: str, payload: bytes):
        self.published.append((subject, payload))


@pytest.mark.asyncio
async def test_handle_market_event_bootstraps_depth_updates_end_to_end(monkeypatch):
    async def fetch_snapshot(symbol: str) -> BinanceDepthSnapshot:
        return BinanceDepthSnapshot(
            symbol=symbol,
            last_update_id=100,
            bids=[[100.0, 1.0]],
            asks=[[101.0, 1.0]],
        )

    bootstrap = BinanceDepthBootstrapper("BTC/USDT", fetch_snapshot=fetch_snapshot)
    fake_js = FakeJetStream()

    monkeypatch.setattr(brain_main, "_books", {})
    monkeypatch.setattr(brain_main, "_depth_bootstraps", {"BTC/USDT": bootstrap})
    monkeypatch.setattr(brain_main, "_signal_counts", {})
    monkeypatch.setattr(brain_main, "_book_gap_counts", {})

    first = MarketEvent(
        source="binance_btc_depth",
        type="depthUpdate",
        timestamp=1,
        payload={
            "e": "depthUpdate",
            "E": 1_700_000_000_001,
            "s": "BTCUSDT",
            "U": 101,
            "u": 102,
            "b": [["100.5", "2.0"]],
            "a": [],
        },
    )
    second = MarketEvent(
        source="binance_btc_depth",
        type="depthUpdate",
        timestamp=2,
        payload={
            "e": "depthUpdate",
            "E": 1_700_000_000_002,
            "s": "BTCUSDT",
            "U": 103,
            "u": 103,
            "b": [],
            "a": [["101.5", "3.0"]],
        },
    )

    await handle_market_event(first, fake_js)
    await asyncio.sleep(0)
    await handle_market_event(second, fake_js)

    book = brain_main._books["BTC/USDT"]
    assert book.is_ready
    assert book.levels()["seq"] == 103
    assert book.best_bid == (100.5, 2.0)
    assert book.best_ask == (101.0, 1.0)
    assert fake_js.published == []
    assert brain_main._book_gap_counts == {}