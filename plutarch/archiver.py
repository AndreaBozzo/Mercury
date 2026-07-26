"""
Plutarch — Mercury's market-data archiver.

Subscribes to NATS JetStream subjects and persists every event to a Lance
dataset for later replay by `backtesting/engine.py`.

Durability contract (must hold for live post-mortems):
  1. Messages are ACKed ONLY after a successful Lance write — never before.
     A SIGKILL between write and ack leaves duplicates (replay must dedupe);
     ack-before-write would have left silent gaps, which is unrecoverable.
  2. The buffer flushes on either size (`PLUTARCH_BATCH_SIZE`, default 500)
     OR elapsed time (`PLUTARCH_FLUSH_INTERVAL_SEC`, default 30s) — whichever
     fires first. No 10k-event in-memory queue.
  3. If `flush_buffer_to_lance` raises, the unflushed payloads are written to
     a quarantine NDJSON file (`PLUTARCH_QUARANTINE_DIR`) and the consumer
     continues from that durable record.
  4. Schema is fixed: {source, type, timestamp, seq, payload_json, _subject,
     _archived_at_ns}. Lance never sees a schema-drift batch.
  5. Invalid messages are ACKed only after their exact raw bytes and validation
     error have been durably written to quarantine.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import lance
import nats
import pyarrow as pa
from dotenv import load_dotenv
from nats.js.errors import NotFoundError

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("plutarch")

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
STREAM_NAME = os.getenv("PLUTARCH_STREAM_NAME", "nephtys_crypto")
STREAM_SUBJECT = os.getenv("PLUTARCH_STREAM_SUBJECT", "nephtys.stream.crypto.*")
DURABLE_NAME = os.getenv("PLUTARCH_DURABLE_NAME", "plutarch_archiver")
DATA_DIR = os.getenv("PLUTARCH_DATA_DIR", "data/history")
DATASET_NAME = os.getenv("PLUTARCH_DATASET_NAME", "market_history.lance")
QUARANTINE_DIR = os.getenv("PLUTARCH_QUARANTINE_DIR", "data/quarantine")
BATCH_SIZE = int(os.getenv("PLUTARCH_BATCH_SIZE", "500"))
FLUSH_INTERVAL_SEC = float(os.getenv("PLUTARCH_FLUSH_INTERVAL_SEC", "30"))
HTTP_HOST = os.getenv("PLUTARCH_HTTP_HOST", "127.0.0.1")
HTTP_PORT = int(os.getenv("PLUTARCH_HTTP_PORT", "9110"))

DATASET_PATH = str(Path(DATA_DIR) / DATASET_NAME)


# Fixed Arrow schema — never changes regardless of event type.
# All event-specific fields live inside payload_json (string).
ARCHIVE_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string()),
        pa.field("type", pa.string()),
        pa.field("timestamp", pa.int64()),
        pa.field("seq", pa.int64()),
        pa.field("payload_json", pa.string()),
        pa.field("_subject", pa.string()),
        pa.field("_archived_at_ns", pa.int64()),
    ]
)


# Runtime state for /healthz + /metrics
_state: dict[str, Any] = {
    "events_written": 0,
    "write_errors": 0,
    "buffer_depth": 0,
    "buffer_started_ns": 0,
    "last_flush_ns": 0,
    "last_flush_duration_seconds": 0.0,
    "quarantine_writes": 0,
    "invalid_messages": 0,
    "started_at_ns": time.time_ns(),
}


def _row_from_envelope(envelope: dict[str, Any], subject: str) -> dict[str, Any]:
    """Project the Nephtys StreamEvent envelope onto the fixed archive schema."""
    inner = envelope.get("payload")
    if not isinstance(inner, dict):
        inner = {}
    seq = envelope.get("seq")
    if seq is None:
        seq = -1
    return {
        "source": str(envelope.get("source") or ""),
        "type": str(envelope.get("type") or ""),
        "timestamp": int(envelope.get("timestamp") or 0),
        "seq": int(seq),
        "payload_json": json.dumps(inner, separators=(",", ":"), sort_keys=True),
        "_subject": subject,
        "_archived_at_ns": time.time_ns(),
    }


def _write_quarantine(records: list[dict[str, Any]], error: BaseException) -> None:
    """Persist archive rows or invalid-message records after processing fails."""
    if not records:
        return
    Path(QUARANTINE_DIR).mkdir(parents=True, exist_ok=True)
    fname = (
        Path(QUARANTINE_DIR)
        / f"plutarch_quarantine_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S_%f')}.ndjson"
    )
    with fname.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")
    _state["quarantine_writes"] += 1
    logger.error(
        "Wrote %d records to quarantine file %s after processing failure: %s",
        len(records),
        fname,
        error,
    )


async def _quarantine_invalid_message(msg: Any, error: BaseException) -> bool:
    """Quarantine one invalid message before acknowledging it.

    The raw payload is base64 encoded so even non-UTF-8 input can be reconstructed
    exactly. If the quarantine write fails, the message remains unacknowledged and
    JetStream may redeliver it.
    """
    record = {
        "_quarantine_kind": "invalid_message",
        "_subject": str(msg.subject),
        "_quarantined_at_ns": time.time_ns(),
        "_error_type": type(error).__name__,
        "_error": str(error),
        "raw_data_b64": base64.b64encode(bytes(msg.data)).decode("ascii"),
    }
    try:
        _write_quarantine([record], error)
    except Exception:
        _state["write_errors"] += 1
        logger.exception(
            "Plutarch: could not quarantine invalid message on subject %s; leaving unacked",
            msg.subject,
        )
        return False

    _state["invalid_messages"] += 1
    try:
        await msg.ack()
    except Exception:
        logger.exception(
            "Plutarch: invalid message was quarantined but its ack failed on subject %s",
            msg.subject,
        )
        return False
    return True


async def _row_or_quarantine(msg: Any) -> dict[str, Any] | None:
    """Decode and project one message, quarantining any invalid input."""
    try:
        envelope = json.loads(msg.data.decode("utf-8"))
        if not isinstance(envelope, dict):
            raise ValueError("event envelope must be a JSON object")
        return _row_from_envelope(envelope, str(msg.subject))
    except Exception as exc:
        logger.warning(
            "Plutarch: invalid event on subject %s: %s",
            msg.subject,
            exc,
        )
        await _quarantine_invalid_message(msg, exc)
        return None


def flush_buffer_to_lance(rows: list[dict[str, Any]]) -> None:
    """Append `rows` to the Lance dataset using the fixed archive schema.

    Raises on write failure — callers must decide whether to quarantine
    or retry. Does NOT ack messages.
    """
    if not rows:
        return
    table = pa.Table.from_pylist(rows, schema=ARCHIVE_SCHEMA)
    lance.write_dataset(table, DATASET_PATH, mode="append")


async def _ensure_stream(js: Any) -> None:
    """Create the stream if absent and reject incompatible existing subjects."""
    try:
        info = await js.stream_info(STREAM_NAME)
    except NotFoundError:
        await js.add_stream(name=STREAM_NAME, subjects=[STREAM_SUBJECT])
        logger.info("Ensured stream %s exists with subject %s", STREAM_NAME, STREAM_SUBJECT)
        return

    configured_subjects = tuple(info.config.subjects or ())
    if not any(
        _stream_subject_covers(configured, STREAM_SUBJECT) for configured in configured_subjects
    ):
        raise RuntimeError(
            f"JetStream stream {STREAM_NAME!r} subjects {configured_subjects!r} "
            f"do not cover Plutarch subject {STREAM_SUBJECT!r}"
        )


def _stream_subject_covers(configured: str, desired: str) -> bool:
    """Return whether one NATS subject pattern contains the desired pattern."""
    configured_tokens = configured.split(".")
    desired_tokens = desired.split(".")
    index = 0
    while index < len(configured_tokens):
        configured_token = configured_tokens[index]
        if configured_token == ">":
            return True
        if index >= len(desired_tokens):
            return False
        desired_token = desired_tokens[index]
        if desired_token == ">":
            return configured_token == ">"
        if configured_token != "*" and configured_token != desired_token:
            return False
        index += 1
    return index == len(desired_tokens)


async def _flush_with_quarantine_and_ack(
    pending_rows: list[dict[str, Any]],
    pending_msgs: list[Any],
) -> None:
    """Flush + ack-after-write. On flush failure, quarantine and ack so that the
    next round is not stuck on the same poison batch.

    Quarantining is a deliberate trade-off: we prefer "data lives in a
    quarantine NDJSON" over "consumer is wedged forever". An alert on
    quarantine_writes > 0 must be wired in M2.
    """
    if not pending_rows:
        return

    started = time.monotonic()
    try:
        flush_buffer_to_lance(pending_rows)
    except Exception as exc:
        _state["write_errors"] += 1
        _write_quarantine(pending_rows, exc)
        # Ack anyway — the NDJSON file is the durable record now.
        for msg in pending_msgs:
            with contextlib.suppress(Exception):
                await msg.ack()
        return

    elapsed = time.monotonic() - started
    _state["events_written"] += len(pending_rows)
    _state["last_flush_ns"] = time.time_ns()
    _state["last_flush_duration_seconds"] = elapsed
    logger.info("Flushed %d rows to %s in %.3fs", len(pending_rows), DATASET_PATH, elapsed)

    for msg in pending_msgs:
        with contextlib.suppress(Exception):
            await msg.ack()


async def run_archiver() -> None:
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

    nc = await nats.connect(servers=[NATS_URL])
    js = nc.jetstream()

    logger.info(
        "Plutarch connected to NATS at %s. Stream=%s subject=%s",
        NATS_URL,
        STREAM_NAME,
        STREAM_SUBJECT,
    )

    await _ensure_stream(js)

    # Pull consumer gives us backpressure for free: no fetch -> no delivery.
    sub = await js.pull_subscribe(STREAM_SUBJECT, durable=DURABLE_NAME)

    pending_rows: list[dict[str, Any]] = []
    pending_msgs: list[Any] = []
    last_flush_monotonic = time.monotonic()

    try:
        while True:
            try:
                msgs = await sub.fetch(batch=BATCH_SIZE, timeout=FLUSH_INTERVAL_SEC)
            except TimeoutError:
                msgs = []

            for msg in msgs:
                row = await _row_or_quarantine(msg)
                if row is None:
                    continue

                if not pending_rows:
                    _state["buffer_started_ns"] = time.time_ns()
                pending_rows.append(row)
                pending_msgs.append(msg)
                _state["buffer_depth"] = len(pending_rows)

            time_since_flush = time.monotonic() - last_flush_monotonic
            should_flush_size = len(pending_rows) >= BATCH_SIZE
            should_flush_time = pending_rows and time_since_flush >= FLUSH_INTERVAL_SEC

            if should_flush_size or should_flush_time:
                await _flush_with_quarantine_and_ack(pending_rows, pending_msgs)
                pending_rows = []
                pending_msgs = []
                _state["buffer_depth"] = 0
                _state["buffer_started_ns"] = 0
                last_flush_monotonic = time.monotonic()

    except asyncio.CancelledError:
        logger.info("Plutarch shutting down. Flushing remaining %d rows...", len(pending_rows))
        await _flush_with_quarantine_and_ack(pending_rows, pending_msgs)
        raise
    finally:
        await nc.close()


# ---------------------------------------------------------------------------
# Minimal HTTP server for /healthz and /metrics
# ---------------------------------------------------------------------------


def _healthz_body() -> bytes:
    now_ns = time.time_ns()
    last_flush_ns = _state["last_flush_ns"]
    last_flush_age_seconds = (now_ns - last_flush_ns) / 1e9 if last_flush_ns else None
    buffer_started_ns = _state["buffer_started_ns"]
    buffer_age_seconds = (
        max(0.0, (now_ns - buffer_started_ns) / 1e9)
        if _state["buffer_depth"] > 0 and buffer_started_ns
        else None
    )

    # This works before the first successful flush as well as after it.
    healthy = buffer_age_seconds is None or buffer_age_seconds < 3 * FLUSH_INTERVAL_SEC

    payload = {
        "status": "ok" if healthy else "stalled",
        "events_written": _state["events_written"],
        "write_errors": _state["write_errors"],
        "quarantine_writes": _state["quarantine_writes"],
        "invalid_messages": _state["invalid_messages"],
        "buffer_depth": _state["buffer_depth"],
        "buffer_age_seconds": buffer_age_seconds,
        "last_flush_age_seconds": last_flush_age_seconds,
        "uptime_seconds": (now_ns - _state["started_at_ns"]) / 1e9,
    }
    return json.dumps(payload).encode()


def _metrics_body() -> bytes:
    now_ns = time.time_ns()
    buffer_started_ns = _state["buffer_started_ns"]
    buffer_age_seconds = (
        max(0.0, (now_ns - buffer_started_ns) / 1e9)
        if _state["buffer_depth"] > 0 and buffer_started_ns
        else 0.0
    )
    lines = [
        f"plutarch_events_written_total {_state['events_written']}",
        f"plutarch_write_errors_total {_state['write_errors']}",
        f"plutarch_quarantine_writes_total {_state['quarantine_writes']}",
        f"plutarch_invalid_messages_total {_state['invalid_messages']}",
        f"plutarch_buffer_depth {_state['buffer_depth']}",
        f"plutarch_buffer_oldest_age_seconds {buffer_age_seconds}",
        f"plutarch_last_flush_duration_seconds {_state['last_flush_duration_seconds']}",
    ]
    return ("\n".join(lines) + "\n").encode()


async def _serve_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        request_line = await reader.readline()
        if not request_line:
            return
        parts = request_line.decode("ascii", errors="ignore").strip().split()
        if len(parts) < 2:
            await _write_response(writer, "400 Bad Request", "text/plain", b"bad request\n")
            return
        method, path = parts[0], parts[1]
        while True:
            line = await reader.readline()
            if not line or line in {b"\r\n", b"\n"}:
                break
        if method != "GET":
            await _write_response(
                writer,
                "405 Method Not Allowed",
                "text/plain",
                b"method not allowed\n",
            )
            return
        if path in {"/healthz", "/health"}:
            body = _healthz_body()
            status = "200 OK"
            try:
                if json.loads(body)["status"] != "ok":
                    status = "503 Service Unavailable"
            except Exception:
                pass
            await _write_response(writer, status, "application/json", body)
            return
        if path == "/metrics":
            await _write_response(writer, "200 OK", "text/plain; version=0.0.4", _metrics_body())
            return
        await _write_response(writer, "404 Not Found", "text/plain", b"not found\n")
    finally:
        writer.close()
        await writer.wait_closed()


async def _write_response(
    writer: asyncio.StreamWriter,
    status: str,
    content_type: str,
    body: bytes,
) -> None:
    headers = [
        f"HTTP/1.1 {status}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body)}",
        "Connection: close",
        "",
        "",
    ]
    writer.write("\r\n".join(headers).encode() + body)
    await writer.drain()


async def main() -> None:
    http_server = await asyncio.start_server(_serve_http, HTTP_HOST, HTTP_PORT)
    logger.info("Plutarch observability listening on http://%s:%d", HTTP_HOST, HTTP_PORT)
    archiver_task = asyncio.create_task(run_archiver())
    try:
        async with http_server:
            await archiver_task
    finally:
        archiver_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await archiver_task


if __name__ == "__main__":
    asyncio.run(main())
