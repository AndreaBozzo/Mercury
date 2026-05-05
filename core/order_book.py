"""
Level 2 Order Book with sequence-gap recovery.

Design decisions:
  - __slots__ on every class → no __dict__ per instance, ~40% less RAM per level
  - SortedDict(neg) for bids → O(log n) insert/delete, O(1) best-bid peek
  - SortedDict()    for asks → same, ascending
  - Gap buffer: out-of-sequence updates are held until the missing seq arrives.
    If the buffer overflows (_MAX_GAP_BUFFER) the book marks itself not-ready
    and a re-snapshot must be applied before deltas are accepted again.
  - Prefer exchange-provided per-symbol sequence numbers. Broker sequence is a
    fallback only for a subject that contains one ordered book stream and no
    interleaved event types.
"""

from __future__ import annotations

import logging

from sortedcontainers import SortedDict

logger = logging.getLogger("order_book")

_MAX_GAP_BUFFER = 500  # request re-snapshot above this threshold


def _neg(k: float) -> float:
    return -k


class OrderBook:
    __slots__ = (
        "_asks",
        "_bids",
        "_gap_buffer",
        "_ready",
        "_seq",
        "symbol",
    )

    def __init__(self, symbol: str) -> None:
        self.symbol: str = symbol
        # bids: highest price first (best bid = index 0)
        self._bids: SortedDict = SortedDict(_neg)
        # asks: lowest price first (best ask = index 0)
        self._asks: SortedDict = SortedDict()
        self._seq: int = 0
        self._gap_buffer: dict[int, tuple[list[list[float]], list[list[float]], int]] = {}
        self._ready: bool = False

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def apply_snapshot(
        self,
        bids: list[list[float]],
        asks: list[list[float]],
        seq: int,
    ) -> None:
        """Replace the entire book with a fresh L2 snapshot."""
        self._bids.clear()
        self._asks.clear()
        self._gap_buffer.clear()
        for price, qty in bids:
            if qty > 0:
                self._bids[float(price)] = float(qty)
        for price, qty in asks:
            if qty > 0:
                self._asks[float(price)] = float(qty)
        self._seq = seq
        self._ready = True
        self._drain_buffer()
        logger.debug(
            "[%s] Snapshot applied seq=%d  bids=%d  asks=%d",
            self.symbol,
            seq,
            len(self._bids),
            len(self._asks),
        )

    def apply_delta(self, side: str, price: float, qty: float, seq: int) -> bool:
        """
        Apply one incremental L2 update.

        Returns True when the update was accepted (or was a harmless duplicate).
        Returns False when a gap was detected — the caller should stop sending
        deltas and request a new snapshot from the market-data source.
        """
        bids = [[price, qty]] if side == "bid" else []
        asks = [[price, qty]] if side == "ask" else []
        return self.apply_delta_batch(bids=bids, asks=asks, first_seq=seq, final_seq=seq)

    def apply_delta_batch(
        self,
        bids: list[list[float]],
        asks: list[list[float]],
        first_seq: int,
        final_seq: int,
    ) -> bool:
        """
        Apply an atomic batch update.

        Binance depth updates cover an inclusive sequence range [U, u]. A batch
        is valid when it overlaps the next expected sequence:
        first_seq <= self._seq + 1 <= final_seq.
        """
        if not self._ready:
            return False

        if first_seq > final_seq:
            logger.warning(
                "[%s] Invalid delta batch range first_seq=%d final_seq=%d",
                self.symbol,
                first_seq,
                final_seq,
            )
            self._ready = False
            self._gap_buffer.clear()
            return False

        expected = self._seq + 1

        if final_seq <= self._seq:
            return True  # stale duplicate — ignore silently

        if first_seq <= expected <= final_seq:
            self._apply_batch(bids, asks)
            self._seq = final_seq
            self._drain_buffer()
            return True

        # Gap detected: buffer and keep waiting
        self._gap_buffer[first_seq] = (bids, asks, final_seq)

        if len(self._gap_buffer) > _MAX_GAP_BUFFER:
            logger.warning(
                "[%s] Gap buffer overflow (expected %d, highest buffered %d) "
                "— marking book not-ready, re-snapshot required",
                self.symbol,
                expected,
                max(self._gap_buffer),
            )
            self._ready = False
            self._gap_buffer.clear()
            return False

        logger.debug(
            "[%s] Seq gap — expected %d got range [%d, %d], buffered",
            self.symbol,
            expected,
            first_seq,
            final_seq,
        )
        return False

    def _apply(self, side: str, price: float, qty: float) -> None:
        book = self._bids if side == "bid" else self._asks
        if qty == 0.0:
            book.pop(price, None)
        else:
            book[price] = qty

    def _apply_batch(self, bids: list[list[float]], asks: list[list[float]]) -> None:
        for price, qty in bids:
            self._apply("bid", float(price), float(qty))
        for price, qty in asks:
            self._apply("ask", float(price), float(qty))

    def _drain_buffer(self) -> None:
        while True:
            expected = self._seq + 1
            drained = False

            for first_seq in sorted(self._gap_buffer):
                bids, asks, final_seq = self._gap_buffer[first_seq]
                if final_seq < expected:
                    self._gap_buffer.pop(first_seq)
                    drained = True
                    break
                if first_seq > expected:
                    break

                self._gap_buffer.pop(first_seq)
                self._apply_batch(bids, asks)
                self._seq = final_seq
                drained = True
                break

            if not drained:
                break

    # ------------------------------------------------------------------
    # Read path — all O(1) or O(depth)
    # ------------------------------------------------------------------

    @property
    def is_ready(self) -> bool:
        return self._ready

    @property
    def best_bid(self) -> tuple[float, float] | None:
        if not self._bids:
            return None
        p = self._bids.keys()[0]
        return (p, self._bids[p])

    @property
    def best_ask(self) -> tuple[float, float] | None:
        if not self._asks:
            return None
        p = self._asks.keys()[0]
        return (p, self._asks[p])

    @property
    def mid(self) -> float | None:
        b, a = self.best_bid, self.best_ask
        if b and a:
            return (b[0] + a[0]) * 0.5
        return None

    @property
    def spread(self) -> float | None:
        b, a = self.best_bid, self.best_ask
        if b and a:
            return a[0] - b[0]
        return None

    def imbalance(self, depth: int = 10) -> float | None:
        """(bid_vol - ask_vol) / (bid_vol + ask_vol) ∈ [-1, 1]."""
        bv = sum(list(self._bids.values())[:depth])
        av = sum(list(self._asks.values())[:depth])
        total = bv + av
        if total == 0:
            return None
        return (bv - av) / total

    def levels(self, depth: int = 20) -> dict:
        """Return top-N bids/asks plus derived microstructure features."""
        return {
            "symbol": self.symbol,
            "seq": self._seq,
            "bids": list(self._bids.items())[:depth],
            "asks": list(self._asks.items())[:depth],
            "mid": self.mid,
            "spread": self.spread,
            "imbalance": self.imbalance(),
        }
