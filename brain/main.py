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

from brain.adapters.binance_depth import parse_binance_depth
from brain.strategies.moving_average import MovingAverageStrategy
from core.nats_utils import get_jetstream
from core.order_book import OrderBook
from core.schemas import MarketEvent, OrderBookDelta, OrderBookSnapshot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("brain")

_books: dict[str, OrderBook] = {}
_strategy = MovingAverageStrategy()


def _book(symbol: str) -> OrderBook:
    if symbol not in _books:
        _books[symbol] = OrderBook(symbol)
    return _books[symbol]


async def main() -> None:
    js = await get_jetstream()
    logger.info("Brain service started.")

    try:
        await js.add_stream(name="nephtys_crypto", subjects=["nephtys.stream.crypto.*"])
    except Exception:
        pass

    async def market_handler(msg) -> None:
        try:
            data = json.loads(msg.data.decode())
            event = MarketEvent(**data)

            if event.type == "l2_snapshot":
                snap = OrderBookSnapshot(**event.payload)
                # Prefer exchange sequence. Broker sequence is a fallback only
                # for single-stream subjects without interleaved event types.
                seq = snap.seq or event.seq or msg.metadata.sequence
                _book(snap.symbol).apply_snapshot(snap.bids, snap.asks, seq)
                logger.debug("[%s] l2_snapshot applied seq=%d", snap.symbol, seq)

            elif event.type == "l2_delta":
                delta = OrderBookDelta(**event.payload)
                seq = delta.seq or event.seq or msg.metadata.sequence
                ok = _book(delta.symbol).apply_delta(delta.side, delta.price, delta.qty, seq)
                if not ok:
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
                    ok = _book(update.symbol).apply_delta_batch(
                        bids=update.bids,
                        asks=update.asks,
                        first_seq=update.first_seq,
                        final_seq=update.final_seq,
                    )
                    if not ok:
                        logger.warning(
                            "[%s] Seq gap on depthUpdate range=[%d,%d] — re-snapshot needed",
                            update.symbol,
                            update.first_seq,
                            update.final_seq,
                        )

            else:
                # L1 path: trade / ticker events from Nephtys today
                signal = _strategy.evaluate(event)
                if signal:
                    await js.publish("trade.signals", signal.model_dump_json().encode())
                    logger.info(
                        "Signal: %s %s @ %.2f (conf=%.2f)",
                        signal.action,
                        signal.symbol,
                        signal.price or 0,
                        signal.confidence_score,
                    )

            await msg.ack()

        except Exception as exc:
            logger.error("Error in market_handler: %s", exc, exc_info=True)

    await js.subscribe(
        "nephtys.stream.crypto.btc",
        durable="brain_service",
        cb=market_handler,
    )

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
