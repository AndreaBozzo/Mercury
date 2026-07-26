from __future__ import annotations

import asyncio
import contextlib
import json
import os
import uuid

import lance
import nats
import pytest

from brain import main as brain_main
from brain.strategies.moving_average import MovingAverageStrategy
from core.nats_utils import MARKET_STREAM_NAME, MARKET_STREAM_SUBJECT, ensure_stream
from executor import main as executor_main
from plutarch import archiver

NATS_URL = os.getenv("MERCURY_INTEGRATION_NATS_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not NATS_URL,
        reason="MERCURY_INTEGRATION_NATS_URL is not configured",
    ),
]


async def _wait_until(predicate, *, timeout: float = 8.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_observation_signal_and_archive_pipeline(monkeypatch, tmp_path):
    """Exercise real JetStream delivery through Brain, Executor, and Plutarch."""
    assert NATS_URL is not None
    client = await nats.connect(servers=[NATS_URL])
    js = client.jetstream()
    archive_task: asyncio.Task | None = None
    subscriptions = []
    suffix = uuid.uuid4().hex
    market_subject = f"nephtys.stream.crypto.integration.{suffix}"
    brain_durable = f"brain_integration_{suffix}"
    plutarch_durable = f"plutarch_integration_{suffix}"

    brain_main._books.clear()
    brain_main._depth_bootstraps.clear()
    brain_main._signal_counts.clear()
    executor_main._trade_counts.clear()
    monkeypatch.setattr(
        brain_main,
        "_strategy",
        MovingAverageStrategy(window=3, buy_threshold=0.99, sell_threshold=1.01),
    )
    monkeypatch.setenv("EXECUTION_MODE", "observe")
    monkeypatch.setattr(brain_main, "_BRAIN_SUBJECT", market_subject)
    monkeypatch.setattr(brain_main, "_BRAIN_DURABLE_NAME", brain_durable)

    archive_path = tmp_path / "market_history_v2.lance"
    monkeypatch.setattr(archiver, "NATS_URL", NATS_URL)
    monkeypatch.setattr(archiver, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(archiver, "DATASET_PATH", str(archive_path))
    monkeypatch.setattr(archiver, "QUARANTINE_DIR", str(tmp_path / "quarantine"))
    monkeypatch.setattr(archiver, "FILTER_SUBJECT", market_subject)
    monkeypatch.setattr(archiver, "DURABLE_NAME", plutarch_durable)
    monkeypatch.setattr(archiver, "BATCH_SIZE", 1)
    monkeypatch.setattr(archiver, "FLUSH_INTERVAL_SEC", 0.2)
    monkeypatch.setattr(archiver, "ACK_WAIT_SEC", 2.0)
    monkeypatch.setattr(archiver, "CONSUMER_MAX_ACK_PENDING", 10)
    archiver._state.update(
        events_written=0,
        write_errors=0,
        buffer_depth=0,
        buffer_started_ns=0,
        last_flush_ns=0,
        last_flush_duration_seconds=0.0,
        quarantine_writes=0,
        invalid_messages=0,
        redelivered_messages=0,
    )

    observed = asyncio.Event()
    real_execute_trade = executor_main.execute_trade

    async def capture_observation(signal):
        result = await real_execute_trade(signal)
        if result.status == "observed":
            observed.set()
        return result

    monkeypatch.setattr(executor_main, "execute_trade", capture_observation)

    try:
        await ensure_stream(
            js,
            name=MARKET_STREAM_NAME,
            subjects=[MARKET_STREAM_SUBJECT],
        )
        await ensure_stream(js, name="trade_signals", subjects=["trade.*"])
        await js.create_key_value(bucket="mercury_wallet_state")

        archive_task = asyncio.create_task(archiver.run_archiver())
        await _wait_for_consumer(js, MARKET_STREAM_NAME, plutarch_durable)
        subscriptions.append(await executor_main.subscribe_trade_signals(js))
        subscriptions.append(await brain_main.subscribe_market_events(js))

        for index, price in enumerate((100.0, 100.0, 90.0), start=1):
            envelope = {
                "source": "integration",
                "type": "ticker",
                "timestamp": index,
                "seq": index,
                "payload": {"symbol": "BTC/EUR", "price": price},
            }
            await js.publish(
                brain_main._BRAIN_SUBJECT,
                json.dumps(envelope).encode(),
            )

        await asyncio.wait_for(observed.wait(), timeout=8)
        await _wait_until(lambda: archiver._state["events_written"] >= 3)
        await _wait_until(lambda: executor_main._trade_counts.get(("BUY", "observed"), 0) >= 1)

        assert executor_main._trade_counts[("BUY", "observed")] == 1
        assert archiver._state["write_errors"] == 0
        assert archiver._state["quarantine_writes"] == 0

        dataset = lance.dataset(str(archive_path))
        table = dataset.to_table(columns=["_stream_sequence"])
        stream_sequences = table.column("_stream_sequence").to_pylist()
        assert len(stream_sequences) == 3
        assert all(isinstance(sequence, int) for sequence in stream_sequences)
        assert len(set(stream_sequences)) == 3
    finally:
        for subscription in subscriptions:
            with contextlib.suppress(Exception):
                await subscription.unsubscribe()
        if archive_task is not None:
            archive_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await archive_task
        for durable in (brain_durable, plutarch_durable):
            with contextlib.suppress(Exception):
                await js.delete_consumer(MARKET_STREAM_NAME, durable)
        with contextlib.suppress(Exception):
            await client.drain()


@pytest.mark.asyncio
async def test_transient_brain_failure_is_redelivered(monkeypatch):
    """Prove the production manual-ack subscription actually honors a NAK."""
    assert NATS_URL is not None
    client = await nats.connect(servers=[NATS_URL])
    js = client.jetstream()
    subscription = None
    processed = asyncio.Event()
    attempts = 0
    suffix = uuid.uuid4().hex
    market_subject = f"nephtys.stream.crypto.integration.{suffix}"
    brain_durable = f"brain_integration_{suffix}"

    async def fail_once(*_args):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary processing failure")
        processed.set()

    monkeypatch.setattr(brain_main, "handle_market_event", fail_once)
    monkeypatch.setattr(brain_main, "_RETRY_DELAY_SECONDS", 0.05)
    monkeypatch.setattr(brain_main, "_BRAIN_SUBJECT", market_subject)
    monkeypatch.setattr(brain_main, "_BRAIN_DURABLE_NAME", brain_durable)

    try:
        await ensure_stream(
            js,
            name=MARKET_STREAM_NAME,
            subjects=[MARKET_STREAM_SUBJECT],
        )
        subscription = await brain_main.subscribe_market_events(js)
        envelope = {
            "source": "integration",
            "type": "ticker",
            "timestamp": 4,
            "seq": 4,
            "payload": {"symbol": "BTC/EUR", "price": 95.0},
        }
        await js.publish(
            brain_main._BRAIN_SUBJECT,
            json.dumps(envelope).encode(),
        )

        await asyncio.wait_for(processed.wait(), timeout=8)

        assert attempts == 2
    finally:
        if subscription is not None:
            with contextlib.suppress(Exception):
                await subscription.unsubscribe()
        with contextlib.suppress(Exception):
            await js.delete_consumer(MARKET_STREAM_NAME, brain_durable)
        with contextlib.suppress(Exception):
            await client.drain()


async def _wait_for_consumer(js, stream: str, durable: str) -> None:
    async def consumer_exists() -> bool:
        try:
            await js.consumer_info(stream, durable)
        except Exception:
            return False
        return True

    async with asyncio.timeout(8):
        while not await consumer_exists():
            await asyncio.sleep(0.05)
