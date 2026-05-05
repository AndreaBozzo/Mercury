"""
Zero-copy Arrow IPC codec for Mercury's internal NATS transport.

Architecture constraint:
  Nephtys publishes JSON (StreamEvent envelope, domain/event.go).  We cannot
  change that today (peer review).  The zero-copy boundary therefore sits one
  hop *inside* Mercury: the ingestion edge converts JSON→Arrow once, and every
  subsequent internal hop (brain → executor, brain → plutarch, etc.) passes
  Arrow IPC bytes over NATS.

Why Arrow IPC over JSON:
  - No string parsing after the first conversion → ~4–8× less CPU on hot paths
  - pa.py_buffer() wraps the raw NATS bytes without memcopy; the IPC reader
    hands back RecordBatch columns that point directly into that buffer
  - Polars / Lance accept Arrow RecordBatch without re-allocating column buffers
    (true zero-copy for the numerical data once it's in Arrow layout)

Usage:
    # Producer (brain ingestion edge — JSON arrives from Nephtys)
    raw: bytes = encode_market_tick({"symbol": "BTC/USDT", "price": 67000.0, ...})
    await js.publish("mercury.internal.ticks", raw)

    # Consumer (strategy worker — zero-copy read)
    batch: pa.RecordBatch = decode_market_tick(msg.data)
    price = batch.column("price")[0].as_py()          # scalar
    df    = pl.from_arrow(batch)                       # Polars, shares buffers
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.ipc as ipc

# ---------------------------------------------------------------------------
# Schemas — defined once, reused for every encode/decode call
# ---------------------------------------------------------------------------

MARKET_TICK_SCHEMA = pa.schema(
    [
        pa.field("source", pa.string()),
        pa.field("type", pa.string()),
        pa.field("timestamp", pa.int64()),
        pa.field("symbol", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("volume", pa.float64()),
        pa.field("mid", pa.float64()),
        pa.field("spread", pa.float64()),
        pa.field("imbalance", pa.float64()),
        pa.field("bid_price", pa.float64()),
        pa.field("ask_price", pa.float64()),
    ]
)

TRADE_SIGNAL_SCHEMA = pa.schema(
    [
        pa.field("strategy_id", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("action", pa.string()),
        pa.field("order_type", pa.string()),
        pa.field("quantity", pa.float64()),
        pa.field("price", pa.float64()),
        pa.field("confidence_score", pa.float64()),
        pa.field("allocation_percentage", pa.float64()),
        pa.field("timestamp_ns", pa.int64()),
    ]
)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _to_ipc(batch: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _from_ipc(data: bytes) -> pa.RecordBatch:
    # pa.py_buffer wraps `data` in an Arrow buffer without copying the bytes.
    # open_stream + read_next_batch then build column arrays that point into
    # that same buffer — no additional allocation for the numerical payload.
    buf = pa.py_buffer(data)
    reader = ipc.open_stream(buf)
    return reader.read_next_batch()


# ---------------------------------------------------------------------------
# Market tick (L1 trade / ticker enriched with optional L2 features)
# ---------------------------------------------------------------------------


def encode_market_tick(d: dict) -> bytes:
    """
    Encode a market tick dict (from Nephtys JSON + optional order book fields)
    into Arrow IPC bytes suitable for NATS publish.
    """
    batch = pa.record_batch(
        [
            pa.array([d.get("source", "")]),
            pa.array([d.get("type", "")]),
            pa.array([d.get("timestamp", 0)], type=pa.int64()),
            pa.array([d.get("symbol", "")]),
            pa.array([float(d.get("price", 0))]),
            pa.array([float(d.get("volume", 0))]),
            pa.array([float(d.get("mid") or 0)]),
            pa.array([float(d.get("spread") or 0)]),
            pa.array([float(d.get("imbalance") or 0)]),
            pa.array([float(d.get("bid_price") or 0)]),
            pa.array([float(d.get("ask_price") or 0)]),
        ],
        schema=MARKET_TICK_SCHEMA,
    )
    return _to_ipc(batch)


def decode_market_tick(data: bytes) -> pa.RecordBatch:
    """
    Zero-copy decode.  Returns a RecordBatch whose column buffers alias the
    input bytes — caller must keep `data` alive while using the batch.
    Access columns as Arrow arrays or pass directly to polars.from_arrow().
    """
    return _from_ipc(data)


# ---------------------------------------------------------------------------
# Trade signal
# ---------------------------------------------------------------------------


def encode_signal(d: dict) -> bytes:
    batch = pa.record_batch(
        [
            pa.array([d["strategy_id"]]),
            pa.array([d["symbol"]]),
            pa.array([d["action"]]),
            pa.array([d["order_type"]]),
            pa.array([float(d["quantity"])]),
            pa.array([float(d.get("price") or 0)]),
            pa.array([float(d["confidence_score"])]),
            pa.array([float(d["allocation_percentage"])]),
            pa.array([int(d.get("timestamp_ns", 0))], type=pa.int64()),
        ],
        schema=TRADE_SIGNAL_SCHEMA,
    )
    return _to_ipc(batch)


def decode_signal(data: bytes) -> pa.RecordBatch:
    return _from_ipc(data)
