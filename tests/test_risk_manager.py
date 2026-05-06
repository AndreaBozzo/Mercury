from datetime import UTC, datetime

import pytest

from core.schemas import TradeSignal
from executor.risk_manager import (
    DEFAULT_MAX_EXPOSURE_DOLLARS,
    DEFAULT_MAX_TRADE_NOTIONAL_DOLLARS,
    EXPOSURE_KEY,
    HALT_KEY,
    RISK_LIMITS_KEY,
    check_risk,
    default_risk_limits,
    load_halt_flag,
    load_position_states,
    load_risk_limits,
    record_trade_execution,
    save_position_states,
    save_risk_limits,
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


@pytest.mark.asyncio
async def test_load_risk_limits_defaults_when_missing():
    js = FakeJetStream(FakeKV())

    limits = await load_risk_limits(js)

    assert limits == default_risk_limits()


@pytest.mark.asyncio
async def test_check_risk_uses_dynamic_kv_limits_for_buys():
    kv = FakeKV(
        {
            EXPOSURE_KEY: b"100",
            RISK_LIMITS_KEY: (
                b'{"max_exposure_dollars": 400, "max_trade_notional_dollars": 200, '
                b'"sentiment_score": -0.6, "source": "test", "rationale": "bad news"}'
            ),
        }
    )
    js = FakeJetStream(kv)

    allowed = await check_risk(make_signal(), js)

    assert allowed is False


@pytest.mark.asyncio
async def test_save_and_load_risk_limits_round_trip():
    kv = FakeKV()
    js = FakeJetStream(kv)

    await save_risk_limits(
        js,
        {
            "max_exposure_dollars": DEFAULT_MAX_EXPOSURE_DOLLARS / 2,
            "max_trade_notional_dollars": DEFAULT_MAX_TRADE_NOTIONAL_DOLLARS / 2,
            "sentiment_score": -0.8,
            "source": "llm",
            "rationale": "severe negative sentiment",
        },
    )

    limits = await load_risk_limits(js)

    assert limits["max_exposure_dollars"] == DEFAULT_MAX_EXPOSURE_DOLLARS / 2
    assert limits["max_trade_notional_dollars"] == DEFAULT_MAX_TRADE_NOTIONAL_DOLLARS / 2
    assert limits["source"] == "llm"


@pytest.mark.asyncio
async def test_save_and_load_position_states_round_trip():
    kv = FakeKV()
    js = FakeJetStream(kv)

    await save_position_states(
        js,
        {
            "BTC/USDT": {
                "symbol": "BTC/USDT",
                "quantity": 0.5,
                "cost_basis": 50.0,
                "last_trade_timestamp": 3,
            }
        },
    )

    states = await load_position_states(js, ["BTC/USDT", "ETH/USDT"])

    assert states == {
        "BTC/USDT": {
            "symbol": "BTC/USDT",
            "quantity": 0.5,
            "cost_basis": 50.0,
            "last_trade_timestamp": 3,
        }
    }
