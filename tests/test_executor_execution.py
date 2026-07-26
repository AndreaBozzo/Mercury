from datetime import UTC, datetime
from typing import Literal

import pytest

from core.schemas import TradeSignal
from executor.exchange_client import ExecutionResult, execute_trade
from executor.main import get_message_handler, subscribe_trade_signals


@pytest.fixture(autouse=True)
def _allow_trading_when_handler_tests_do_not_exercise_halt(monkeypatch):
    async def halt_disabled(_js):
        return False

    monkeypatch.setattr("executor.main.load_halt_flag", halt_disabled)


def make_signal(action: Literal["BUY", "SELL"] = "BUY") -> TradeSignal:
    return TradeSignal(
        strategy_id="ma-cross",
        symbol="BTC/USDT",
        action=action,
        order_type="MARKET",
        quantity=0.01,
        price=50000.0,
        confidence_score=0.9,
        allocation_percentage=10.0,
        timestamp=datetime.now(UTC),
    )


class FakeExchange:
    id = "binance"

    def __init__(self):
        self.buy_calls: list[tuple[str, float]] = []
        self.sell_calls: list[tuple[str, float]] = []
        self.fetch_calls: list[tuple[str, str]] = []
        self.fetch_order_responses: list[dict] = []
        self.closed = False

    async def create_market_buy_order(self, symbol: str, quantity: float):
        self.buy_calls.append((symbol, quantity))
        return {"id": "buy-123", "symbol": symbol, "amount": quantity, "status": "open"}

    async def create_market_sell_order(self, symbol: str, quantity: float):
        self.sell_calls.append((symbol, quantity))
        return {"id": "sell-123", "symbol": symbol, "amount": quantity, "status": "open"}

    async def fetch_order(self, order_id: str, symbol: str):
        self.fetch_calls.append((order_id, symbol))
        if self.fetch_order_responses:
            return self.fetch_order_responses.pop(0)
        return {
            "id": order_id,
            "symbol": symbol,
            "amount": 0.01,
            "status": "closed",
            "filled": 0.01,
        }

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_execute_trade_observes_without_creating_exchange_or_submitting_orders(monkeypatch):
    exchange = FakeExchange()

    monkeypatch.setattr("executor.exchange_client.get_exchange", lambda: exchange)
    monkeypatch.setattr("executor.exchange_client.is_observe_mode_enabled", lambda: True)

    result = await execute_trade(make_signal())

    assert result == ExecutionResult(status="observed")
    assert exchange.buy_calls == []
    assert exchange.sell_calls == []
    assert exchange.closed is False


@pytest.mark.asyncio
async def test_execute_trade_submits_market_buy_when_dry_run_disabled(monkeypatch):
    exchange = FakeExchange()
    exchange.fetch_order_responses = [
        {"id": "buy-123", "symbol": "BTC/USDT", "amount": 0.01, "status": "closed", "filled": 0.01}
    ]

    monkeypatch.setattr("executor.exchange_client.get_exchange", lambda: exchange)
    monkeypatch.setattr("executor.exchange_client.is_observe_mode_enabled", lambda: False)
    monkeypatch.setattr("executor.exchange_client.get_execution_mode", lambda: "sandbox")
    monkeypatch.setattr("executor.exchange_client.ORDER_CONFIRM_DELAY_SECONDS", 0.0)

    result = await execute_trade(make_signal(action="BUY"))

    assert result.status == "filled"
    assert result.order_id == "buy-123"
    assert exchange.buy_calls == [("BTC/USDT", 0.01)]
    assert exchange.sell_calls == []
    assert exchange.fetch_calls == [("buy-123", "BTC/USDT")]
    assert exchange.closed is True


@pytest.mark.asyncio
async def test_execute_trade_submits_market_sell_when_dry_run_disabled(monkeypatch):
    exchange = FakeExchange()
    exchange.fetch_order_responses = [
        {"id": "sell-123", "symbol": "BTC/USDT", "amount": 0.01, "status": "closed", "filled": 0.01}
    ]

    monkeypatch.setattr("executor.exchange_client.get_exchange", lambda: exchange)
    monkeypatch.setattr("executor.exchange_client.is_observe_mode_enabled", lambda: False)
    monkeypatch.setattr("executor.exchange_client.get_execution_mode", lambda: "sandbox")
    monkeypatch.setattr("executor.exchange_client.ORDER_CONFIRM_DELAY_SECONDS", 0.0)

    result = await execute_trade(make_signal(action="SELL"))

    assert result.status == "filled"
    assert result.order_id == "sell-123"
    assert exchange.buy_calls == []
    assert exchange.sell_calls == [("BTC/USDT", 0.01)]
    assert exchange.fetch_calls == [("sell-123", "BTC/USDT")]
    assert exchange.closed is True


@pytest.mark.asyncio
async def test_execute_trade_raises_when_fill_is_not_confirmed(monkeypatch):
    exchange = FakeExchange()
    exchange.fetch_order_responses = [
        {"id": "buy-123", "symbol": "BTC/USDT", "amount": 0.01, "status": "open", "filled": 0.0},
        {"id": "buy-123", "symbol": "BTC/USDT", "amount": 0.01, "status": "open", "filled": 0.0},
    ]

    monkeypatch.setattr("executor.exchange_client.get_exchange", lambda: exchange)
    monkeypatch.setattr("executor.exchange_client.is_observe_mode_enabled", lambda: False)
    monkeypatch.setattr("executor.exchange_client.get_execution_mode", lambda: "sandbox")
    monkeypatch.setattr("executor.exchange_client.ORDER_CONFIRM_ATTEMPTS", 2)
    monkeypatch.setattr("executor.exchange_client.ORDER_CONFIRM_DELAY_SECONDS", 0.0)

    with pytest.raises(RuntimeError, match="was not confirmed as filled"):
        await execute_trade(make_signal(action="BUY"))

    assert exchange.fetch_calls == [
        ("buy-123", "BTC/USDT"),
        ("buy-123", "BTC/USDT"),
    ]
    assert exchange.closed is True


class FakeMessage:
    def __init__(self, payload: bytes):
        self.data = payload
        self.ack_count = 0
        self.nak_delays: list[float] = []
        self.term_count = 0

    async def ack(self):
        self.ack_count += 1

    async def nak(self, delay: float):
        self.nak_delays.append(delay)

    async def term(self):
        self.term_count += 1


@pytest.mark.asyncio
async def test_message_handler_acks_on_successful_execution(monkeypatch):
    signal = make_signal()
    message = FakeMessage(signal.model_dump_json().encode())
    recorded_signals: list[TradeSignal] = []

    async def fake_check_risk(_signal, _js):
        return True

    async def fake_execute_trade(_signal):
        return ExecutionResult(status="observed")

    async def fake_record_trade_execution(_signal, _js):
        recorded_signals.append(_signal)
        return 500.0

    async def fake_refresh_exposure(_js):
        return None

    monkeypatch.setattr("executor.main.check_risk", fake_check_risk)
    monkeypatch.setattr("executor.main.execute_trade", fake_execute_trade)
    monkeypatch.setattr("executor.main.record_trade_execution", fake_record_trade_execution)
    monkeypatch.setattr("executor.main._refresh_current_exposure", fake_refresh_exposure)

    handler = await get_message_handler(js=object())
    await handler(message)

    assert message.ack_count == 1
    assert recorded_signals == []


@pytest.mark.asyncio
async def test_message_handler_records_exposure_after_filled_execution(monkeypatch):
    signal = make_signal()
    message = FakeMessage(signal.model_dump_json().encode())
    recorded_signals: list[TradeSignal] = []

    async def fake_check_risk(_signal, _js):
        return True

    async def fake_execute_trade(_signal):
        return ExecutionResult(status="filled", order_id="buy-123", order={"id": "buy-123"})

    async def fake_record_trade_execution(_signal, _js):
        recorded_signals.append(_signal)
        return 500.0

    monkeypatch.setattr("executor.main.check_risk", fake_check_risk)
    monkeypatch.setattr("executor.main.execute_trade", fake_execute_trade)
    monkeypatch.setattr("executor.main.record_trade_execution", fake_record_trade_execution)

    handler = await get_message_handler(js=object())
    await handler(message)

    assert message.ack_count == 1
    assert recorded_signals == [signal]


@pytest.mark.asyncio
async def test_message_handler_does_not_ack_when_execution_raises(monkeypatch):
    signal = make_signal()
    message = FakeMessage(signal.model_dump_json().encode())

    async def fake_check_risk(_signal, _js):
        return True

    async def fake_execute_trade(_signal):
        raise RuntimeError("exchange unavailable")

    monkeypatch.setattr("executor.main.check_risk", fake_check_risk)
    monkeypatch.setattr("executor.main.execute_trade", fake_execute_trade)

    handler = await get_message_handler(js=object())
    await handler(message)

    assert message.ack_count == 0
    assert message.nak_delays == [5.0]


@pytest.mark.asyncio
async def test_message_handler_terminates_malformed_signal():
    message = FakeMessage(b'{"symbol":')

    handler = await get_message_handler(js=object())
    await handler(message)

    assert message.ack_count == 0
    assert message.nak_delays == []
    assert message.term_count == 1


@pytest.mark.asyncio
async def test_trade_subscription_disables_callback_auto_ack():
    class FakeJetStream:
        def __init__(self):
            self.kwargs = None

        async def subscribe(self, *_args, **kwargs):
            self.kwargs = kwargs

    js = FakeJetStream()

    await subscribe_trade_signals(js)

    assert js.kwargs is not None
    assert js.kwargs["manual_ack"] is True
    assert js.kwargs["config"].max_deliver == 10


@pytest.mark.asyncio
async def test_message_handler_skips_when_sizer_rejects(monkeypatch):
    """Live mode: sizer returns no sized_signal -> no trade placed, message acked."""
    from executor.sizing import SizingDecision

    signal = make_signal()
    message = FakeMessage(signal.model_dump_json().encode())

    async def fake_check_risk(_signal, _js):
        return True

    async def fake_size_signal(_signal, _js):
        return SizingDecision(sized_signal=None, reason="vol_modulator_stale")

    execute_calls: list = []

    async def fake_execute_trade(_signal):
        execute_calls.append(_signal)
        return ExecutionResult(status="filled", order_id="x")

    monkeypatch.setattr("executor.main.is_observe_mode_enabled", lambda: False)
    monkeypatch.setattr("executor.main.check_risk", fake_check_risk)
    monkeypatch.setattr("executor.main.size_signal", fake_size_signal)
    monkeypatch.setattr("executor.main.execute_trade", fake_execute_trade)

    handler = await get_message_handler(js=object())
    await handler(message)

    assert message.ack_count == 1
    assert execute_calls == []


@pytest.mark.asyncio
async def test_message_handler_passes_sized_signal_to_execute(monkeypatch):
    """Live mode: sizer returns a sized_signal -> that signal (not placeholder) is executed."""
    from executor.sizing import SizingDecision

    signal = make_signal()
    sized = signal.model_copy(update={"quantity": 0.0001})
    message = FakeMessage(signal.model_dump_json().encode())

    async def fake_check_risk(_signal, _js):
        return True

    async def fake_size_signal(_signal, _js):
        return SizingDecision(sized_signal=sized, reason="ok")

    executed: list[TradeSignal] = []

    async def fake_execute_trade(s: TradeSignal):
        executed.append(s)
        return ExecutionResult(status="filled", order_id="ok-1", order={"id": "ok-1"})

    async def fake_record_trade_execution(_signal, _js):
        return 0.0

    async def fake_refresh(_js):
        return None

    monkeypatch.setattr("executor.main.is_observe_mode_enabled", lambda: False)
    monkeypatch.setattr("executor.main.check_risk", fake_check_risk)
    monkeypatch.setattr("executor.main.size_signal", fake_size_signal)
    monkeypatch.setattr("executor.main.execute_trade", fake_execute_trade)
    monkeypatch.setattr("executor.main.record_trade_execution", fake_record_trade_execution)
    monkeypatch.setattr("executor.main._refresh_runtime_state_safe", fake_refresh)

    handler = await get_message_handler(js=object())
    await handler(message)

    assert message.ack_count == 1
    assert len(executed) == 1
    assert executed[0].quantity == 0.0001
