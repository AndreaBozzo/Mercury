import asyncio
import queue
from types import SimpleNamespace

import pytest

from brain.stream_processor import _NatsBridge


@pytest.mark.asyncio
async def test_bridge_subscription_owns_ack_and_connection(monkeypatch):
    calls: dict[str, object] = {}

    class FakeJetStream:
        async def stream_info(self, _name):
            return SimpleNamespace(config=SimpleNamespace(subjects=["nephtys.stream.crypto.*"]))

        async def subscribe(self, subject, **kwargs):
            calls["subject"] = subject
            calls["subscribe"] = kwargs

    class FakeClient:
        is_closed = False

        def __init__(self):
            self.js = FakeJetStream()
            self.drained = False

        def jetstream(self):
            return self.js

        async def drain(self):
            self.drained = True
            self.is_closed = True

        async def close(self):
            self.is_closed = True

    client = FakeClient()

    async def fake_connect(**_kwargs):
        return client

    async def stop_after_subscription(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr("brain.stream_processor.nats.connect", fake_connect)
    monkeypatch.setattr("brain.stream_processor.asyncio.sleep", stop_after_subscription)

    bridge = object.__new__(_NatsBridge)
    bridge._q = queue.Queue(maxsize=1)
    bridge._nats_url = "nats://test"
    bridge._subject = "nephtys.stream.crypto.btc"

    with pytest.raises(asyncio.CancelledError):
        await bridge._consume()

    assert calls["subject"] == "nephtys.stream.crypto.btc"
    assert calls["subscribe"]["manual_ack"] is True
    assert client.drained is True
