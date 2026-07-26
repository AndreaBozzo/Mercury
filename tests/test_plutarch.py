"""Integration tests for plutarch/archiver.py.

These cover the four scenarios called out in the 2026-05-19 review:
  1. Ack happens AFTER the Lance write (no silent gap on crash).
  2. A poison batch is quarantined as NDJSON and the consumer is not wedged.
  3. Mixed-event-type batches (trade + l2_snapshot) flush without schema drift.
  4. Round-trip: rows written by plutarch decode back to the original payload
     when read via the backtester's `_payload_from_row` helper.

Lance and ccxt are NOT imported here — we monkeypatch `lance.write_dataset`
and use plain Python dicts as JetStream message stand-ins.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from nats.js.errors import NotFoundError

from backtesting.engine import _payload_from_row


@pytest.fixture(autouse=True)
def _isolate_state(tmp_path, monkeypatch):
    """Each test gets a fresh data dir + clean module state."""
    from plutarch import archiver

    monkeypatch.setattr(archiver, "DATASET_PATH", str(tmp_path / "test.lance"))
    monkeypatch.setattr(archiver, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(archiver, "QUARANTINE_DIR", str(tmp_path / "quarantine"))

    # Reset runtime counters
    archiver._state.update(
        events_written=0,
        write_errors=0,
        buffer_depth=0,
        buffer_started_ns=0,
        last_flush_ns=0,
        last_flush_duration_seconds=0.0,
        quarantine_writes=0,
        invalid_messages=0,
    )
    yield


class FakeMsg:
    def __init__(self, subject: str, payload: dict[str, Any]):
        self.subject = subject
        self.data = json.dumps(payload).encode()
        self.ack_count = 0

    async def ack(self) -> None:
        self.ack_count += 1


def _envelope(event_type: str = "trade", inner: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "source": "binance",
        "type": event_type,
        "timestamp": 1_700_000_000_000,
        "seq": 1,
        "payload": inner or {"price": 50000.0, "qty": 0.01, "side": "buy"},
    }


@pytest.mark.asyncio
async def test_ack_happens_after_successful_lance_write(monkeypatch):
    """Fix for review §3: ack-after-flush ordering is the load-bearing invariant."""
    from plutarch import archiver

    call_order: list[str] = []

    def fake_flush(rows):
        # touch `rows` so the linter sees it as used (param is required by signature)
        _ = len(rows)
        call_order.append("write")

    monkeypatch.setattr(archiver, "flush_buffer_to_lance", fake_flush)

    msgs = [FakeMsg(f"nephtys.stream.crypto.btc.{i}", _envelope()) for i in range(3)]
    rows = [archiver._row_from_envelope(json.loads(m.data.decode()), m.subject) for m in msgs]

    # Patch msg.ack to record ordering
    for m in msgs:
        original_ack = m.ack

        async def ack_recorded(_m=m, _orig=original_ack):
            call_order.append("ack")
            await _orig()

        m.ack = ack_recorded

    await archiver._flush_with_quarantine_and_ack(rows, msgs)

    assert call_order[0] == "write", f"ack happened before write: {call_order}"
    assert call_order.count("ack") == 3
    assert all(m.ack_count == 1 for m in msgs)
    assert archiver._state["events_written"] == 3
    assert archiver._state["write_errors"] == 0


@pytest.mark.asyncio
async def test_poison_batch_quarantined_and_consumer_not_wedged(monkeypatch, tmp_path):
    """Fix for review §1: write failure must NOT lose data and MUST ack so the
    consumer keeps moving. Rows go to an NDJSON quarantine file."""
    from plutarch import archiver

    def boom(*_args, **_kwargs):
        raise RuntimeError("Lance commit failed")

    monkeypatch.setattr(archiver, "flush_buffer_to_lance", boom)

    msgs = [FakeMsg(f"nephtys.stream.crypto.btc.{i}", _envelope()) for i in range(2)]
    rows = [archiver._row_from_envelope(json.loads(m.data.decode()), m.subject) for m in msgs]

    await archiver._flush_with_quarantine_and_ack(rows, msgs)

    # Acked even though write failed (quarantine file is now the durable record).
    assert all(m.ack_count == 1 for m in msgs)
    assert archiver._state["write_errors"] == 1
    assert archiver._state["quarantine_writes"] == 1

    quarantine_files = list(Path(archiver.QUARANTINE_DIR).glob("plutarch_quarantine_*.ndjson"))
    assert len(quarantine_files) == 1
    lines = quarantine_files[0].read_text().strip().splitlines()
    assert len(lines) == 2
    decoded = [json.loads(line) for line in lines]
    assert all("payload_json" in row for row in decoded)


@pytest.mark.asyncio
async def test_mixed_event_types_share_one_schema(monkeypatch):
    """Fix for review §4: trade + l2_snapshot in one batch must not blow up
    schema inference. The fix is the fixed Arrow schema with payload_json."""
    from plutarch import archiver

    captured: dict[str, Any] = {}

    import pyarrow as pa

    def fake_flush(rows):
        # Reproduce the same Arrow conversion the real flush would do
        table = pa.Table.from_pylist(rows, schema=archiver.ARCHIVE_SCHEMA)
        captured["schema_names"] = [f.name for f in table.schema]
        captured["row_count"] = table.num_rows

    monkeypatch.setattr(archiver, "flush_buffer_to_lance", fake_flush)

    trade = _envelope("trade", {"price": 50000.0, "qty": 0.01, "side": "buy"})
    l2 = _envelope(
        "l2_snapshot",
        {"bids": [[49999.0, 0.5], [49998.0, 1.0]], "asks": [[50001.0, 0.5]]},
    )

    msgs = [
        FakeMsg("nephtys.stream.crypto.btc.trade", trade),
        FakeMsg("nephtys.stream.crypto.btc.l2", l2),
    ]
    rows = [archiver._row_from_envelope(json.loads(m.data.decode()), m.subject) for m in msgs]

    await archiver._flush_with_quarantine_and_ack(rows, msgs)

    assert archiver._state["write_errors"] == 0
    assert archiver._state["events_written"] == 2
    assert captured["row_count"] == 2
    # Schema is always the fixed envelope shape — no payload field widening
    assert set(captured["schema_names"]) == {
        "source",
        "type",
        "timestamp",
        "seq",
        "payload_json",
        "_subject",
        "_archived_at_ns",
    }


def test_roundtrip_through_payload_decoder():
    """Fix for review §7: a row produced by Plutarch must decode via
    backtesting._payload_from_row to the EXACT inner payload that was archived."""
    from plutarch import archiver

    original_payload = {
        "price": 50123.45,
        "qty": 0.001,
        "side": "buy",
        "nested": {"venue": "binance-spot", "fee_bps": 1},
    }
    envelope = _envelope("trade", original_payload)
    row = archiver._row_from_envelope(envelope, "nephtys.stream.crypto.btc")

    # Simulate the .iterrows() shape — dict-like row with .get()
    class FakeRow(dict):
        def get(self, k, default=None):
            return super().get(k, default)

    decoded = _payload_from_row(FakeRow(row))
    assert decoded == original_payload


def test_envelope_missing_fields_does_not_crash():
    """Defensive: an envelope without `payload` or `seq` should still produce a
    valid archive row (empty payload, seq=-1)."""
    from plutarch import archiver

    row = archiver._row_from_envelope({"source": "x", "type": "y"}, "subj")
    assert row["source"] == "x"
    assert row["type"] == "y"
    assert row["seq"] == -1
    assert row["payload_json"] == "{}"
    assert row["_subject"] == "subj"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw",
    [
        b'{"source":"coinbase","timestamp":"not-an-integer","payload":{}}',
        b"{not valid json",
        b"\xff\xfe\x00",
        b'["an","array"]',
    ],
)
async def test_invalid_message_is_quarantined_before_ack(raw):
    """Invalid input must be preserved exactly instead of crashing or disappearing."""
    from plutarch import archiver

    msg = FakeMsg("nephtys.stream.crypto.btc", {})
    msg.data = raw

    row = await archiver._row_or_quarantine(msg)

    assert row is None
    assert msg.ack_count == 1
    assert archiver._state["invalid_messages"] == 1
    assert archiver._state["quarantine_writes"] == 1

    quarantine_files = list(Path(archiver.QUARANTINE_DIR).glob("plutarch_quarantine_*.ndjson"))
    assert len(quarantine_files) == 1
    record = json.loads(quarantine_files[0].read_text(encoding="utf-8"))
    assert record["_quarantine_kind"] == "invalid_message"
    assert record["_subject"] == msg.subject
    assert base64.b64decode(record["raw_data_b64"]) == raw
    assert record["_error_type"]


@pytest.mark.asyncio
async def test_invalid_message_remains_unacked_if_quarantine_fails(monkeypatch):
    """A failed forensic write must leave the event eligible for redelivery."""
    from plutarch import archiver

    def fail_quarantine(*_args, **_kwargs):
        raise OSError("disk unavailable")

    monkeypatch.setattr(archiver, "_write_quarantine", fail_quarantine)
    msg = FakeMsg("nephtys.stream.crypto.btc", {})
    msg.data = b"{invalid"

    row = await archiver._row_or_quarantine(msg)

    assert row is None
    assert msg.ack_count == 0
    assert archiver._state["invalid_messages"] == 0
    assert archiver._state["write_errors"] == 1


def test_health_detects_stalled_buffer_before_first_flush(monkeypatch):
    from plutarch import archiver

    monkeypatch.setattr(archiver, "FLUSH_INTERVAL_SEC", 10.0)
    archiver._state.update(
        buffer_depth=1,
        buffer_started_ns=archiver.time.time_ns() - 31_000_000_000,
        last_flush_ns=0,
    )

    payload = json.loads(archiver._healthz_body())

    assert payload["status"] == "stalled"
    assert payload["last_flush_age_seconds"] is None
    assert payload["buffer_age_seconds"] >= 31.0
    assert "plutarch_buffer_oldest_age_seconds" in archiver._metrics_body().decode()


def test_health_accepts_fresh_buffer_before_first_flush(monkeypatch):
    from plutarch import archiver

    monkeypatch.setattr(archiver, "FLUSH_INTERVAL_SEC", 10.0)
    archiver._state.update(
        buffer_depth=1,
        buffer_started_ns=archiver.time.time_ns(),
        last_flush_ns=0,
    )

    payload = json.loads(archiver._healthz_body())

    assert payload["status"] == "ok"


@pytest.mark.asyncio
async def test_ensure_stream_creates_only_when_absent():
    from plutarch import archiver

    class FakeJetStream:
        def __init__(self):
            self.added: list[dict[str, Any]] = []

        async def stream_info(self, _name):
            raise NotFoundError()

        async def add_stream(self, **kwargs):
            self.added.append(kwargs)

    js = FakeJetStream()
    await archiver._ensure_stream(js)

    assert js.added == [
        {
            "name": archiver.STREAM_NAME,
            "subjects": [archiver.STREAM_SUBJECT],
        }
    ]


@pytest.mark.asyncio
async def test_ensure_stream_accepts_covering_existing_subject():
    from plutarch import archiver

    class FakeJetStream:
        async def stream_info(self, _name):
            return SimpleNamespace(config=SimpleNamespace(subjects=["nephtys.stream.crypto.>"]))

    await archiver._ensure_stream(FakeJetStream())


@pytest.mark.asyncio
async def test_ensure_stream_rejects_incompatible_existing_subject():
    from plutarch import archiver

    class FakeJetStream:
        async def stream_info(self, _name):
            return SimpleNamespace(config=SimpleNamespace(subjects=["unrelated.>"]))

    with pytest.raises(RuntimeError, match="do not cover"):
        await archiver._ensure_stream(FakeJetStream())


@pytest.mark.asyncio
async def test_ensure_stream_does_not_hide_transport_failure():
    from plutarch import archiver

    class FakeJetStream:
        async def stream_info(self, _name):
            raise ConnectionError("NATS unavailable")

    with pytest.raises(ConnectionError, match="NATS unavailable"):
        await archiver._ensure_stream(FakeJetStream())
