import asyncio

import pytest

from brain.adapters.binance_depth import (
    BinanceDepthBootstrapper,
    BinanceDepthSnapshot,
    BinanceDepthUpdate,
    parse_binance_depth,
)
from core.order_book import OrderBook
from core.schemas import MarketEvent


def test_parse_binance_depth_update():
    event = MarketEvent(
        source="binance_btc",
        type="depthUpdate",
        timestamp=1,
        seq=102,
        payload={
            "e": "depthUpdate",
            "E": 1_700_000_000_000,
            "s": "BTCUSDT",
            "U": 100,
            "u": 102,
            "b": [["67000.0", "1.5"], ["66999.0", "0"]],
            "a": [["67001.0", "2.0"]],
        },
    )

    update = parse_binance_depth(event)

    assert update is not None
    assert update.symbol == "BTC/USDT"
    assert update.first_seq == 100
    assert update.final_seq == 102
    assert update.timestamp == 1_700_000_000_000
    assert update.bids == [[67000.0, 1.5], [66999.0, 0.0]]
    assert update.asks == [[67001.0, 2.0]]


def test_parse_binance_depth_ignores_non_depth_event():
    event = MarketEvent(source="binance_btc", type="trade", timestamp=1, payload={})

    assert parse_binance_depth(event) is None


def test_parse_binance_depth_uses_envelope_seq_when_payload_u_missing():
    event = MarketEvent(
        source="binance_btc",
        type="depthUpdate",
        timestamp=1,
        seq=12,
        payload={"e": "depthUpdate", "s": "ETHUSDT", "U": 10, "b": [], "a": []},
    )

    update = parse_binance_depth(event)

    assert update is not None
    assert update.final_seq == 12


@pytest.mark.asyncio
async def test_bootstrap_discards_stale_updates_and_applies_first_overlap():
    async def fetch_snapshot(symbol: str) -> BinanceDepthSnapshot:
        return BinanceDepthSnapshot(
            symbol=symbol,
            last_update_id=100,
            bids=[[100.0, 1.0]],
            asks=[[101.0, 1.0]],
        )

    book = OrderBook("BTC/USDT")
    bootstrap = BinanceDepthBootstrapper("BTC/USDT", fetch_snapshot=fetch_snapshot)

    stale = BinanceDepthUpdate(
        symbol="BTC/USDT",
        first_seq=90,
        final_seq=100,
        timestamp=1,
        bids=[[99.0, 5.0]],
        asks=[],
    )
    first_live = BinanceDepthUpdate(
        symbol="BTC/USDT",
        first_seq=101,
        final_seq=102,
        timestamp=2,
        bids=[[100.5, 2.0]],
        asks=[],
    )

    assert not await bootstrap.handle_update(stale, book)
    await asyncio.sleep(0)

    assert await bootstrap.handle_update(first_live, book)
    assert book.is_ready
    assert book._seq == 102
    assert book.best_bid == (100.5, 2.0)
    assert 99.0 not in book._bids


@pytest.mark.asyncio
async def test_bootstrap_buffers_updates_while_snapshot_fetch_is_pending():
    release_fetch = asyncio.Event()

    async def fetch_snapshot(symbol: str) -> BinanceDepthSnapshot:
        await release_fetch.wait()
        return BinanceDepthSnapshot(
            symbol=symbol,
            last_update_id=100,
            bids=[[100.0, 1.0]],
            asks=[[101.0, 1.0]],
        )

    book = OrderBook("BTC/USDT")
    bootstrap = BinanceDepthBootstrapper("BTC/USDT", fetch_snapshot=fetch_snapshot)

    first_live = BinanceDepthUpdate(
        symbol="BTC/USDT",
        first_seq=101,
        final_seq=102,
        timestamp=1,
        bids=[[100.5, 2.0]],
        asks=[],
    )
    second_live = BinanceDepthUpdate(
        symbol="BTC/USDT",
        first_seq=103,
        final_seq=103,
        timestamp=2,
        bids=[],
        asks=[[101.5, 4.0]],
    )

    assert not await bootstrap.handle_update(first_live, book)
    assert not await bootstrap.handle_update(second_live, book)

    release_fetch.set()
    await asyncio.sleep(0)

    trigger = BinanceDepthUpdate(
        symbol="BTC/USDT",
        first_seq=104,
        final_seq=104,
        timestamp=3,
        bids=[[100.6, 1.0]],
        asks=[],
    )

    assert await bootstrap.handle_update(trigger, book)
    assert book._seq == 104
    assert book.best_bid == (100.6, 1.0)
    assert book.best_ask == (101.0, 1.0)
    assert book._asks[101.5] == 4.0


@pytest.mark.asyncio
async def test_bootstrap_restarts_when_first_live_update_has_gap():
    calls = 0

    async def fetch_snapshot(symbol: str) -> BinanceDepthSnapshot:
        nonlocal calls
        calls += 1
        return BinanceDepthSnapshot(
            symbol=symbol,
            last_update_id=100,
            bids=[[100.0, 1.0]],
            asks=[[101.0, 1.0]],
        )

    book = OrderBook("BTC/USDT")
    bootstrap = BinanceDepthBootstrapper("BTC/USDT", fetch_snapshot=fetch_snapshot)

    gap = BinanceDepthUpdate(
        symbol="BTC/USDT",
        first_seq=105,
        final_seq=106,
        timestamp=1,
        bids=[[105.0, 1.0]],
        asks=[],
    )

    assert not await bootstrap.handle_update(gap, book)
    await asyncio.sleep(0)
    assert not await bootstrap.handle_update(gap, book)

    assert calls == 1
    assert not book.is_ready
    assert bootstrap.is_active


@pytest.mark.asyncio
async def test_bootstrap_retries_after_snapshot_fetch_failure():
    calls = 0

    async def fetch_snapshot(symbol: str) -> BinanceDepthSnapshot:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary REST failure")
        return BinanceDepthSnapshot(
            symbol=symbol,
            last_update_id=100,
            bids=[[100.0, 1.0]],
            asks=[[101.0, 1.0]],
        )

    book = OrderBook("BTC/USDT")
    bootstrap = BinanceDepthBootstrapper("BTC/USDT", fetch_snapshot=fetch_snapshot)
    update = BinanceDepthUpdate(
        symbol="BTC/USDT",
        first_seq=101,
        final_seq=101,
        timestamp=1,
        bids=[[100.5, 2.0]],
        asks=[],
    )

    assert not await bootstrap.handle_update(update, book)
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="temporary REST failure"):
        await bootstrap.handle_update(update, book)

    assert not bootstrap.is_active
    assert not await bootstrap.handle_update(update, book)
    await asyncio.sleep(0)

    assert await bootstrap.handle_update(update, book)
    assert calls == 2
    assert book.is_ready
