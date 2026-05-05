"""
Brain service — event consumer and signal publisher.

Message routing by MarketEvent.type:
  "l2_snapshot"  → feed full book state into the per-symbol OrderBook
  "l2_delta"     → apply incremental update; log warning when seq gap detected
  anything else  → L1 trade/ticker path: evaluate strategy, publish signal

The OrderBook is populated here so that strategies (and the Bytewax processor)
can consume pre-built microstructure features rather than raw ticks.
Live L2 support depends on upstream "l2_snapshot" / "l2_delta" events with a
per-symbol exchange sequence. See core/schemas.py for the Mercury-side contract.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

from brain.adapters.binance_depth import BinanceDepthBootstrapper, parse_binance_depth
from brain.observability import health_payload, json_response, metrics_payload
from brain.strategies import build_strategy_from_env
from core.nats_utils import get_jetstream
from core.order_book import OrderBook
from core.schemas import MarketEvent, OrderBookDelta, OrderBookSnapshot
from core.symbols import normalize_symbol

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("brain")

_books: dict[str, OrderBook] = {}
_depth_bootstraps: dict[str, BinanceDepthBootstrapper] = {}
_signal_counts: dict[tuple[str, str], int] = {}
_book_gap_counts: dict[str, int] = {}
_strategy = build_strategy_from_env()
_BRAIN_HTTP_HOST = os.getenv("BRAIN_HTTP_HOST", "127.0.0.1")
_BRAIN_HTTP_PORT = int(os.getenv("BRAIN_HTTP_PORT", "9108"))


def _book(symbol: str) -> OrderBook:
    if symbol not in _books:
        _books[symbol] = OrderBook(symbol)
    return _books[symbol]


def _depth_bootstrap(symbol: str) -> BinanceDepthBootstrapper:
    if symbol not in _depth_bootstraps:
        _depth_bootstraps[symbol] = BinanceDepthBootstrapper(symbol)
    return _depth_bootstraps[symbol]


def _bootstrap_state() -> dict[str, bool]:
    return {symbol: bootstrap.is_active for symbol, bootstrap in _depth_bootstraps.items()}


def _record_signal_emitted(action: str, symbol: str) -> None:
    key = (action, symbol)
    _signal_counts[key] = _signal_counts.get(key, 0) + 1


def _record_book_gap(symbol: str) -> None:
    _book_gap_counts[symbol] = _book_gap_counts.get(symbol, 0) + 1


def _event_with_book_features(event: MarketEvent) -> MarketEvent:
    payload = dict(event.payload)
    try:
        symbol = normalize_symbol(payload.get("symbol"))
    except ValueError:
        return event

    book = _books.get(symbol)
    if book is None or not book.is_ready:
        return event

    levels = book.levels()
    payload.setdefault("symbol", symbol)
    payload.setdefault("mid", levels.get("mid"))
    payload.setdefault("spread", levels.get("spread"))
    payload.setdefault("imbalance", levels.get("imbalance"))
    return event.model_copy(update={"payload": payload})


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

        bootstrap_state = _bootstrap_state()
        if path == "/health":
            body = json_response(health_payload(_books, bootstrap_state))
            await _write_response(writer, "200 OK", "application/json", body)
            return

        if path == "/metrics":
            body = metrics_payload(
                _books,
                bootstrap_state,
                signal_counts=_signal_counts,
                book_gap_counts=_book_gap_counts,
            ).encode()
            await _write_response(writer, "200 OK", "text/plain; version=0.0.4", body)
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
    js = await get_jetstream()
    logger.info("Brain service started.")

    try:
        await js.add_stream(name="nephtys_crypto", subjects=["nephtys.stream.crypto.*"])
    except Exception:
        pass

    http_server = await asyncio.start_server(_serve_http, _BRAIN_HTTP_HOST, _BRAIN_HTTP_PORT)
    logger.info("Brain observability listening on http://%s:%d", _BRAIN_HTTP_HOST, _BRAIN_HTTP_PORT)

    async def market_handler(msg) -> None:
        try:
            data = json.loads(msg.data.decode())
            event = MarketEvent(**data)

            await handle_market_event(event, js, msg.metadata.sequence)

            await msg.ack()

        except Exception as exc:
            logger.error("Error in market_handler: %s", exc, exc_info=True)

    await js.subscribe(
        "nephtys.stream.crypto.btc",
        durable="brain_service",
        cb=market_handler,
    )

    async with http_server:
        await http_server.serve_forever()


async def handle_market_event(event: MarketEvent, js, broker_sequence: int | None = None) -> None:
    if event.type == "l2_snapshot":
        snap = OrderBookSnapshot(**event.payload)
        # Prefer exchange sequence. Broker sequence is a fallback only
        # for single-stream subjects without interleaved event types.
        seq = snap.seq or event.seq or broker_sequence or 0
        _book(snap.symbol).apply_snapshot(snap.bids, snap.asks, seq)
        logger.debug("[%s] l2_snapshot applied seq=%d", snap.symbol, seq)

    elif event.type == "l2_delta":
        delta = OrderBookDelta(**event.payload)
        seq = delta.seq or event.seq or broker_sequence or 0
        ok = _book(delta.symbol).apply_delta(delta.side, delta.price, delta.qty, seq)
        if not ok:
            _record_book_gap(delta.symbol)
            logger.warning(
                "[%s] Seq gap on l2_delta seq=%d — book not ready, re-snapshot needed",
                delta.symbol,
                seq,
            )

    elif event.type == "depthUpdate":
        update = parse_binance_depth(event)
        if update is None:
            logger.warning("Invalid depthUpdate payload from %s", event.source)
        else:
            book = _book(update.symbol)
            bootstrap = _depth_bootstrap(update.symbol)
            bootstrapping = not book.is_ready or bootstrap.is_active
            if bootstrapping:
                ok = await bootstrap.handle_update(update, book)
                if ok and book.is_ready:
                    logger.info(
                        "[%s] Order book ready after snapshot bootstrap seq=%d",
                        update.symbol,
                        book.levels()["seq"],
                    )
            else:
                ok = book.apply_delta_batch(
                    bids=update.bids,
                    asks=update.asks,
                    first_seq=update.first_seq,
                    final_seq=update.final_seq,
                )
                if not ok:
                    _record_book_gap(update.symbol)
                    ok = await bootstrap.handle_update(update, book)

            if not ok:
                logger.warning(
                    "[%s] Buffering depthUpdate range=[%d,%d] during bootstrap",
                    update.symbol,
                    update.first_seq,
                    update.final_seq,
                )

    else:
        # L1 path: trade / ticker events from Nephtys today
        signal = _strategy.evaluate(_event_with_book_features(event))
        if signal:
            await js.publish("trade.signals", signal.model_dump_json().encode())
            _record_signal_emitted(signal.action, signal.symbol)
            logger.info(
                "Signal: %s %s @ %.2f (conf=%.2f)",
                signal.action,
                signal.symbol,
                signal.price or 0,
                signal.confidence_score,
            )


if __name__ == "__main__":
    asyncio.run(main())
