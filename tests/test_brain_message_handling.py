import json

import pytest
from nats.js.errors import NotFoundError

from brain.main import get_market_handler, subscribe_market_events


class FakeMessage:
    def __init__(self, data: bytes):
        self.data = data
        self.ack_count = 0
        self.term_count = 0
        self.nak_delays: list[float] = []
        self.metadata = None

    async def ack(self):
        self.ack_count += 1

    async def term(self):
        self.term_count += 1

    async def nak(self, delay: float):
        self.nak_delays.append(delay)


@pytest.mark.asyncio
async def test_market_handler_terminates_malformed_event():
    message = FakeMessage(b'{"source":')

    await get_market_handler(object())(message)

    assert message.ack_count == 0
    assert message.term_count == 1
    assert message.nak_delays == []


@pytest.mark.asyncio
async def test_market_handler_naks_transient_processing_failure(monkeypatch):
    payload = {
        "source": "test",
        "type": "trade",
        "timestamp": 1,
        "payload": {"symbol": "BTC/EUR", "price": 100.0},
    }
    message = FakeMessage(json.dumps(payload).encode())

    async def fail_processing(*_args):
        raise RuntimeError("publisher unavailable")

    monkeypatch.setattr("brain.main.handle_market_event", fail_processing)

    await get_market_handler(object())(message)

    assert message.ack_count == 0
    assert message.term_count == 0
    assert message.nak_delays == [5.0]


@pytest.mark.asyncio
async def test_market_subscription_disables_callback_auto_ack():
    class FakeJetStream:
        def __init__(self):
            self.kwargs = None

        async def consumer_info(self, _stream, _durable):
            raise NotFoundError()

        async def subscribe(self, *_args, **kwargs):
            self.kwargs = kwargs

    js = FakeJetStream()

    await subscribe_market_events(js)

    assert js.kwargs is not None
    assert js.kwargs["manual_ack"] is True
    assert js.kwargs["config"].max_deliver == 10
