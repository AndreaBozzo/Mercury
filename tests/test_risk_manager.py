from datetime import UTC, datetime

import pytest

from core.schemas import TradeSignal
from executor.risk_manager import (
    EXPOSURE_KEY,
    HALT_KEY,
    check_risk,
    load_halt_flag,
    record_trade_execution,
)


def make_signal() -> TradeSignal:
    return TradeSignal(
        strategy_id="ma-cross",
        symbol="BTC/USDT",
        action="BUY",
        order_type="MARKET",
        quantity=0.01,
        price=50000.0,
        confidence_score=0.9,
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


@pytest.mark.asyncio
async def test_load_halt_flag_defaults_to_false_when_key_is_missing():
    js = FakeJetStream(FakeKV())

    assert await load_halt_flag(js) is False


@pytest.mark.asyncio
async def test_check_risk_rejects_trade_when_halt_flag_is_set():
    kv = FakeKV({HALT_KEY: b"true", EXPOSURE_KEY: b"0"})
    js = FakeJetStream(kv)

    allowed = await check_risk(make_signal(), js)

    assert allowed is False
    assert kv.store[EXPOSURE_KEY] == b"0"


@pytest.mark.asyncio
async def test_check_risk_does_not_mutate_exposure_when_trade_is_only_validated():
    kv = FakeKV({EXPOSURE_KEY: b"0"})
    js = FakeJetStream(kv)

    allowed = await check_risk(make_signal(), js)

    assert allowed is True
    assert kv.store[EXPOSURE_KEY] == b"0"


@pytest.mark.asyncio
async def test_record_trade_execution_updates_buy_exposure_after_submission():
    kv = FakeKV({EXPOSURE_KEY: b"0"})
    js = FakeJetStream(kv)

    new_exposure = await record_trade_execution(make_signal(), js)

    assert new_exposure == 500.0
    assert float(kv.store[EXPOSURE_KEY].decode()) == 500.0


@pytest.mark.asyncio
async def test_record_trade_execution_reduces_exposure_for_sell_orders():
    kv = FakeKV({EXPOSURE_KEY: b"500"})
    js = FakeJetStream(kv)

    sell_signal = make_signal().model_copy(update={"action": "SELL"})
    new_exposure = await record_trade_execution(sell_signal, js)

    assert new_exposure == 0.0
    assert float(kv.store[EXPOSURE_KEY].decode()) == 0.0