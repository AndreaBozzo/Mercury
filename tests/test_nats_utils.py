from types import SimpleNamespace

import pytest
from nats.js.errors import NotFoundError

from core.nats_utils import NatsSession, ensure_stream


class FakeClient:
    def __init__(self, *, is_closed: bool = False, drain_error: Exception | None = None):
        self.is_closed = is_closed
        self.drain_error = drain_error
        self.drain_count = 0
        self.close_count = 0

    async def drain(self):
        self.drain_count += 1
        if self.drain_error is not None:
            raise self.drain_error
        self.is_closed = True

    async def close(self):
        self.close_count += 1
        self.is_closed = True


class FakeJetStream:
    def __init__(self, *, subjects: list[str] | None = None, error: Exception | None = None):
        self.subjects = subjects
        self.error = error
        self.added: list[tuple[str, list[str]]] = []

    async def stream_info(self, _name):
        if self.error is not None:
            raise self.error
        return SimpleNamespace(config=SimpleNamespace(subjects=self.subjects))

    async def add_stream(self, *, name, subjects):
        self.added.append((name, subjects))


@pytest.mark.asyncio
async def test_nats_session_drains_owned_connection():
    client = FakeClient()
    session = NatsSession(client=client, js=object())

    await session.close()

    assert client.drain_count == 1
    assert client.close_count == 0


@pytest.mark.asyncio
async def test_nats_session_closes_when_drain_fails():
    client = FakeClient(drain_error=RuntimeError("broker disappeared"))
    session = NatsSession(client=client, js=object())

    await session.close()

    assert client.drain_count == 1
    assert client.close_count == 1


@pytest.mark.asyncio
async def test_ensure_stream_creates_only_when_missing():
    js = FakeJetStream(error=NotFoundError())

    await ensure_stream(
        js,
        name="market",
        subjects=["nephtys.stream.crypto.*"],
    )

    assert js.added == [("market", ["nephtys.stream.crypto.*"])]


@pytest.mark.asyncio
async def test_ensure_stream_rejects_incompatible_subjects():
    js = FakeJetStream(subjects=["unexpected.>"])

    with pytest.raises(RuntimeError, match="unexpected"):
        await ensure_stream(
            js,
            name="market",
            subjects=["nephtys.stream.crypto.*"],
        )


@pytest.mark.asyncio
async def test_ensure_stream_propagates_broker_failures():
    js = FakeJetStream(error=PermissionError("denied"))

    with pytest.raises(PermissionError, match="denied"):
        await ensure_stream(
            js,
            name="market",
            subjects=["nephtys.stream.crypto.*"],
        )
