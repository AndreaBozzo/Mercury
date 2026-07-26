"""Regression tests for executor/sizing.py — focused on review findings #2 and #8."""

import json
from datetime import UTC, datetime, timedelta
from typing import Literal

import pytest

from core.schemas import INTENT_PLACEHOLDER_QTY, TradeSignal
from executor import sizing
from executor.sizing import (
    AVAILABLE_BALANCE_KEY,
    VOL_MODULATOR_KEY,
    size_signal,
)


def make_signal(
    confidence: float = 0.8,
    *,
    action: Literal["BUY", "SELL"] = "BUY",
    symbol: str = "BTC/EUR",
) -> TradeSignal:
    return TradeSignal(
        strategy_id="test",
        symbol=symbol,
        action=action,
        order_type="MARKET",
        quantity=INTENT_PLACEHOLDER_QTY,
        price=49000.0,  # stale strategy-supplied price
        confidence_score=confidence,
        allocation_percentage=10.0,
        timestamp=datetime.now(UTC),
    )


class FakeEntry:
    def __init__(self, value: bytes):
        self.value = value


class FakeKV:
    def __init__(self, initial: dict[str, bytes] | None = None):
        self.store = dict(initial or {})

    async def get(self, key: str) -> FakeEntry:
        if key not in self.store:
            raise KeyError(key)
        return FakeEntry(self.store[key])

    async def put(self, key: str, value: bytes):
        self.store[key] = value


class FakeJetStream:
    def __init__(self, kv: FakeKV):
        self.kv = kv

    async def key_value(self, _bucket: str) -> FakeKV:
        return self.kv


def _fresh_balance_payload(
    eur: float = 100.0,
    *,
    btc: float = 0.5,
    usdt: float = 100.0,
) -> bytes:
    return json.dumps(
        {
            "quote_free": {"EUR": eur, "USDT": usdt},
            "asset_free": {"BTC": btc, "EUR": eur, "USDT": usdt},
            "computed_at": datetime.now(UTC).isoformat(),
        }
    ).encode()


def _fresh_modulator_payload(multiplier: float = 1.0) -> bytes:
    return json.dumps(
        {
            "multiplier": multiplier,
            "annualized_vol": 0.4,
            "window_samples": 60,
            "computed_at": datetime.now(UTC).isoformat(),
        }
    ).encode()


@pytest.fixture(autouse=True)
def _clear_min_cost_cache():
    """Ensure no cache bleed between tests."""
    sizing._market_min_cost.clear()
    yield
    sizing._market_min_cost.clear()


@pytest.mark.asyncio
async def test_sizer_skips_when_min_notional_unavailable(monkeypatch):
    """Fix #2 regression: sizer must fail CLOSED when _load_min_cost returns None."""
    kv = FakeKV(
        {
            AVAILABLE_BALANCE_KEY: _fresh_balance_payload(eur=100.0),
            VOL_MODULATOR_KEY: _fresh_modulator_payload(),
        }
    )
    js = FakeJetStream(kv)

    async def fake_mark_price(_symbol):
        return 50000.0

    async def fake_min_cost(_symbol):
        return None  # simulate unavailable market metadata

    monkeypatch.setattr(sizing, "_fetch_mark_price", fake_mark_price)
    monkeypatch.setattr(sizing, "_load_min_cost", fake_min_cost)

    decision = await size_signal(make_signal(), js)

    assert decision.sized_signal is None
    assert decision.reason == "min_notional_unavailable"


@pytest.mark.asyncio
async def test_sized_signal_carries_mark_price(monkeypatch):
    """Fix #8 regression: sized signal must use mark_price (not strategy's stale price)."""
    kv = FakeKV(
        {
            AVAILABLE_BALANCE_KEY: _fresh_balance_payload(eur=1000.0),
            VOL_MODULATOR_KEY: _fresh_modulator_payload(),
        }
    )
    js = FakeJetStream(kv)
    mark_price = 50000.0

    async def fake_mark_price(_symbol):
        return mark_price

    async def fake_min_cost(_symbol):
        return 5.0  # 5 USDT min, well below our target

    monkeypatch.setattr(sizing, "_fetch_mark_price", fake_mark_price)
    monkeypatch.setattr(sizing, "_load_min_cost", fake_min_cost)
    monkeypatch.setattr(sizing, "MAX_NOTIONAL_EUR", 5.0)

    signal = make_signal(confidence=0.8)
    decision = await size_signal(signal, js)

    assert decision.sized_signal is not None
    assert decision.reason == "ok"
    # sized signal MUST report the mark price, overwriting the stale strategy price
    assert decision.sized_signal.price == mark_price
    assert decision.sized_signal.price != signal.price  # stale price discarded
    # quantity matches notional / mark_price
    assert abs(decision.sized_signal.quantity - decision.target_notional_quote / mark_price) < 1e-12


@pytest.mark.asyncio
async def test_sizer_skips_when_vol_modulator_stale(monkeypatch):
    """vol_modulator older than VOL_MODULATOR_MAX_AGE_SECONDS -> skip."""
    stale_ts = (datetime.now(UTC) - timedelta(seconds=600)).isoformat()
    kv = FakeKV(
        {
            AVAILABLE_BALANCE_KEY: _fresh_balance_payload(),
            VOL_MODULATOR_KEY: json.dumps({"multiplier": 0.5, "computed_at": stale_ts}).encode(),
        }
    )
    js = FakeJetStream(kv)

    decision = await size_signal(make_signal(), js)

    assert decision.sized_signal is None
    assert decision.reason == "vol_modulator_stale"


@pytest.mark.asyncio
async def test_sizer_skips_when_balance_missing(monkeypatch):
    """available_balance KV key absent -> skip with balance_missing."""
    kv = FakeKV({VOL_MODULATOR_KEY: _fresh_modulator_payload()})  # no balance key
    js = FakeJetStream(kv)

    decision = await size_signal(make_signal(), js)

    assert decision.sized_signal is None
    assert decision.reason == "balance_missing"


@pytest.mark.asyncio
async def test_sell_is_sized_from_free_base_inventory(monkeypatch):
    kv = FakeKV(
        {
            AVAILABLE_BALANCE_KEY: _fresh_balance_payload(eur=0.0, btc=0.5),
            VOL_MODULATOR_KEY: _fresh_modulator_payload(),
        }
    )
    js = FakeJetStream(kv)

    async def fake_mark_price(_symbol):
        return 100.0

    async def fake_min_cost(_symbol):
        return 1.0

    monkeypatch.setattr(sizing, "_fetch_mark_price", fake_mark_price)
    monkeypatch.setattr(sizing, "_load_min_cost", fake_min_cost)
    monkeypatch.setattr(sizing, "MAX_NOTIONAL_EUR", 1_000.0)

    decision = await size_signal(make_signal(action="SELL", confidence=0.8), js)

    assert decision.sized_signal is not None
    assert decision.sized_signal.quantity == pytest.approx(0.1)
    assert decision.target_notional_quote == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_sell_fails_closed_without_free_base_inventory():
    kv = FakeKV(
        {
            AVAILABLE_BALANCE_KEY: _fresh_balance_payload(eur=1_000.0, btc=0.0),
            VOL_MODULATOR_KEY: _fresh_modulator_payload(),
        }
    )

    decision = await size_signal(
        make_signal(action="SELL"),
        FakeJetStream(kv),
    )

    assert decision.sized_signal is None
    assert decision.reason == "balance_zero"


@pytest.mark.asyncio
async def test_non_eur_quote_is_rejected_before_exchange_or_balance_access():
    decision = await size_signal(
        make_signal(symbol="BTC/USDT"),
        FakeJetStream(FakeKV()),
    )

    assert decision.sized_signal is None
    assert decision.reason == "invalid_quote"


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
async def test_non_finite_balance_state_fails_closed(value):
    kv = FakeKV(
        {
            AVAILABLE_BALANCE_KEY: _fresh_balance_payload(eur=value),
            VOL_MODULATOR_KEY: _fresh_modulator_payload(),
        }
    )

    decision = await size_signal(make_signal(), FakeJetStream(kv))

    assert decision.sized_signal is None
    assert decision.reason == "balance_missing"


def test_invalid_or_future_state_timestamp_is_stale():
    assert sizing._is_stale(123, 60) is True
    future = (datetime.now(UTC) + timedelta(minutes=1)).isoformat()
    assert sizing._is_stale(future, 60) is True
