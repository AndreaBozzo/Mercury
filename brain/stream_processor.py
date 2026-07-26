"""
Bytewax stream processor — stateful alternative to brain/main.py.

Architecture
------------
Bytewax runs its Timely Dataflow engine synchronously in Python workers.
NATS JetStream is async.  The bridge pattern solves the impedance mismatch:

    ┌─────────────────────────────────┐
    │  _NatsBridge  (daemon thread)   │
    │  asyncio event loop             │
    │  nats.subscribe → queue.put()   │
    └────────────┬────────────────────┘
                 │  threading.Queue
    ┌────────────▼────────────────────┐
    │  NatsSource  (Bytewax input)    │
    │  _NatsPartition.next_batch()    │
    │  → queue.get_nowait() x N       │
    └────────────┬────────────────────┘
                 │  (key, raw_dict) stream
    ┌────────────▼──────────────────────────────────┐
    │  stateful_map("vwap")                          │
    │  VwapState per symbol — rolling VWAP           │
    ├────────────────────────────────────────────────┤
    │  stateful_map("imbalance")                     │
    │  ImbalanceState per symbol — rolling mean OBI  │
    ├────────────────────────────────────────────────┤
    │  map("signal") → emit TradeSignal or None      │
    │  filter("valid") → drop Nones                  │
    └────────────┬───────────────────────────────────┘
                 │  TradeSignal stream
    ┌────────────▼────────────────────┐
    │  SignalSink  (Bytewax output)   │
    │  async NATS publish             │
    └─────────────────────────────────┘

State recovery
--------------
Bytewax snapshots operator state after each epoch (configurable interval).
The snapshot is stored on disk via `--snapshot-dir`; on restart Bytewax
replays from the last checkpoint, so VWAP/imbalance windows are not lost
and the strategy doesn't produce incorrect signals from a cold start.

Usage
-----
    python -m brain.stream_processor --workers 2 --snapshot-dir /tmp/mercury

Requires Bytewax >= 0.21.  If the API changes, this file documents the
design intent clearly enough to adapt.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

import bytewax.operators as op
import nats
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.run import cli_main
from nats.js import JetStreamContext

from core.nats_utils import NatsSession, ensure_stream
from core.schemas import INTENT_PLACEHOLDER_QTY, TradeSignal

logger = logging.getLogger("stream_processor")

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
SUBJECT = os.getenv("BRAIN_SUBJECT", "nephtys.stream.crypto.btc")
VWAP_WINDOW = int(os.getenv("VWAP_WINDOW", "50"))
IMBALANCE_WINDOW = int(os.getenv("IMBALANCE_WINDOW", "20"))
SIGNAL_THRESHOLD = float(os.getenv("SIGNAL_THRESHOLD", "0.01"))

# ---------------------------------------------------------------------------
# NATS → Queue bridge
# ---------------------------------------------------------------------------


class _NatsBridge:
    """Async NATS JetStream consumer in a daemon thread → sync queue."""

    def __init__(self, nats_url: str, subject: str, maxsize: int = 10_000) -> None:
        self._q: queue.Queue[bytes] = queue.Queue(maxsize=maxsize)
        self._nats_url = nats_url
        self._subject = subject
        self._loop = asyncio.new_event_loop()
        t = threading.Thread(target=self._run, daemon=True, name="nats-bridge")
        t.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._consume())

    async def _consume(self) -> None:
        nc = await nats.connect(servers=[self._nats_url])
        js = nc.jetstream()
        session = NatsSession(client=nc, js=js)
        try:
            await ensure_stream(
                js,
                name="nephtys_crypto",
                subjects=["nephtys.stream.crypto.*"],
            )

            async def _handler(msg) -> None:
                try:
                    self._q.put_nowait(msg.data)
                    await msg.ack()
                except queue.Full:
                    logger.warning("Bridge queue full — dropping message")
                    await msg.nak()

            await js.subscribe(
                self._subject,
                durable="stream_processor",
                cb=_handler,
                manual_ack=True,
            )
            while True:
                await asyncio.sleep(1)
        finally:
            await session.close()

    def drain(self, n: int = 200) -> list[bytes]:
        out: list[bytes] = []
        for _ in range(n):
            try:
                out.append(self._q.get_nowait())
            except queue.Empty:
                break
        return out


_bridge: _NatsBridge | None = None
_bridge_lock = threading.Lock()


def _get_bridge() -> _NatsBridge:
    """Create the NATS bridge lazily so importing this module has no network side effect."""
    global _bridge
    if _bridge is None:
        with _bridge_lock:
            if _bridge is None:
                _bridge = _NatsBridge(NATS_URL, SUBJECT)
    return _bridge


# ---------------------------------------------------------------------------
# Bytewax Input
# ---------------------------------------------------------------------------


class _NatsPartition(StatelessSourcePartition):
    def __init__(self) -> None:
        self._bridge = _get_bridge()

    def next_batch(self) -> list[dict]:
        raw_msgs = self._bridge.drain(200)
        if not raw_msgs:
            time.sleep(0.001)
            return []
        out = []
        for raw in raw_msgs:
            try:
                out.append(json.loads(raw))
            except Exception:
                pass
        return out

    def close(self) -> None:
        pass


class _EmptyPartition(StatelessSourcePartition):
    def next_batch(self) -> list:
        time.sleep(0.01)
        return []

    def close(self) -> None:
        pass


class NatsSource(DynamicSource):
    """
    Single-partition NATS source.  Only worker 0 reads from the bridge;
    others return empty — the durable JetStream consumer already fans out
    across processes if you run multiple stream_processor instances.
    """

    def build(self, step_id: str, worker_index: int, worker_count: int) -> StatelessSourcePartition:
        return _NatsPartition() if worker_index == 0 else _EmptyPartition()


# ---------------------------------------------------------------------------
# Bytewax Output
# ---------------------------------------------------------------------------


class _SignalSinkPartition(StatelessSinkPartition):
    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._js: JetStreamContext | None = None
        t = threading.Thread(target=self._setup, daemon=True, name="signal-sink")
        t.start()
        # give the thread a moment to connect before the first write
        time.sleep(0.5)

    def _setup(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._connect())

    async def _connect(self) -> None:
        nc = await nats.connect(servers=[NATS_URL])
        self._js = nc.jetstream()

    def write_batch(self, items: list[TradeSignal | None]) -> None:
        if self._js is None:
            return
        for signal in items:
            if signal is None:
                continue
            payload = signal.model_dump_json().encode()
            asyncio.run_coroutine_threadsafe(
                self._js.publish("trade.signals", payload),
                self._loop,
            )

    def close(self) -> None:
        pass


class SignalSink(DynamicSink):
    def build(self, step_id: str, worker_index: int, worker_count: int) -> _SignalSinkPartition:
        return _SignalSinkPartition()


# ---------------------------------------------------------------------------
# Stateful operator state containers
# ---------------------------------------------------------------------------


@dataclass
class VwapState:
    prices: list[float] = field(default_factory=list)
    volumes: list[float] = field(default_factory=list)

    def update(self, price: float, volume: float) -> float | None:
        self.prices.append(price)
        self.volumes.append(volume)
        # keep rolling window
        if len(self.prices) > VWAP_WINDOW:
            self.prices.pop(0)
            self.volumes.pop(0)
        total_vol = sum(self.volumes)
        if total_vol == 0:
            return None
        return sum(p * v for p, v in zip(self.prices, self.volumes)) / total_vol


@dataclass
class ImbalanceState:
    values: list[float] = field(default_factory=list)

    def update(self, imbalance: float) -> float | None:
        self.values.append(imbalance)
        if len(self.values) > IMBALANCE_WINDOW:
            self.values.pop(0)
        if len(self.values) < 3:
            return None
        return sum(self.values) / len(self.values)


# ---------------------------------------------------------------------------
# Operator functions — must be top-level (picklable for Bytewax workers)
# ---------------------------------------------------------------------------


def _key_by_symbol(d: dict) -> tuple[str, dict]:
    return (d.get("payload", {}).get("symbol", "UNKNOWN"), d)


def _vwap_mapper(state: VwapState | None, item: dict) -> tuple[VwapState, dict]:
    if state is None:
        state = VwapState()
    p = item.get("payload", {})
    vwap = state.update(float(p.get("price", 0)), float(p.get("volume", 1.0)))
    item["_vwap"] = vwap
    return state, item


def _imbalance_mapper(state: ImbalanceState | None, item: dict) -> tuple[ImbalanceState, dict]:
    if state is None:
        state = ImbalanceState()
    raw_imbalance = item.get("_imbalance_raw")
    if raw_imbalance is None:
        raw_imbalance = item.get("payload", {}).get("imbalance")
    mean_imbalance = state.update(raw_imbalance) if raw_imbalance is not None else None
    item["_imbalance"] = mean_imbalance
    return state, item


def _emit_signal(kv: tuple[str, dict]) -> tuple[str, TradeSignal | None]:
    symbol, item = kv
    payload = item.get("payload", {})
    price = float(payload.get("price", 0))
    vwap = item.get("_vwap")
    imbalance = item.get("_imbalance")

    if not price or not vwap:
        return (symbol, None)

    deviation = (price - vwap) / vwap

    action: Literal["BUY", "SELL"]
    if deviation < -SIGNAL_THRESHOLD:
        action = "BUY"
        # Imbalance confirmation: if selling pressure dominates, skip
        if imbalance is not None and imbalance < -0.3:
            return (symbol, None)
    elif deviation > SIGNAL_THRESHOLD:
        action = "SELL"
        if imbalance is not None and imbalance > 0.3:
            return (symbol, None)
    else:
        return (symbol, None)

    confidence = min(1.0, abs(deviation) / SIGNAL_THRESHOLD * 0.5)
    signal = TradeSignal(
        strategy_id="vwap_imbalance_v1",
        symbol=symbol,
        action=action,
        order_type="MARKET",
        quantity=INTENT_PLACEHOLDER_QTY,
        price=price,
        confidence_score=confidence,
        allocation_percentage=0.1 * confidence * 100,
        timestamp=datetime.now(UTC),
    )
    return (symbol, signal)


def _is_valid_signal(kv: tuple[str, TradeSignal | None]) -> bool:
    return kv[1] is not None


def _unwrap_signal(kv: tuple[str, TradeSignal | None]) -> TradeSignal:
    signal = kv[1]
    assert signal is not None  # guaranteed by upstream _is_valid_signal filter
    return signal


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------


def build_dataflow() -> Dataflow:
    flow = Dataflow("mercury_stream")

    # 1. Raw JSON dicts from NATS
    raw = op.input("nats_in", flow, NatsSource())

    # 2. Key by symbol for per-symbol stateful operators
    keyed = op.map("key_by_symbol", raw, _key_by_symbol)

    # 3. Rolling VWAP (Bytewax preserves per-key state across epochs)
    # Bytewax type stubs incomplete; the 4-arg form is correct at runtime.
    vwap_out = op.stateful_map("vwap", keyed, lambda: None, _vwap_mapper)  # type: ignore[call-arg, arg-type, misc, var-annotated]

    # 4. Rolling mean order book imbalance (populated by l2_delta handler in main.py)
    imbal_out = op.stateful_map("imbalance", vwap_out, lambda: None, _imbalance_mapper)  # type: ignore[call-arg, arg-type, misc, var-annotated]

    # 5. Signal emission with VWAP divergence + imbalance confirmation
    signals = op.map("emit_signal", imbal_out, _emit_signal)

    # 6. Drop None signals
    valid = op.filter("filter_valid", signals, _is_valid_signal)

    # 7. Unwrap (key, signal) → signal
    unwrapped = op.map("unwrap", valid, _unwrap_signal)

    # 8. Publish to NATS trade.signals
    op.output("nats_out", unwrapped, SignalSink())

    return flow


if __name__ == "__main__":
    cli_main(build_dataflow(), workers_per_process=2)
