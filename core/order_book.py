"""
Level 2 Order Book with sequence-gap recovery.

Design decisions:
  - __slots__ on every class → no __dict__ per instance, ~40% less RAM per level
  - SortedDict(neg) for bids → O(log n) insert/delete, O(1) best-bid peek
  - SortedDict()    for asks → same, ascending
  - Gap buffer: out-of-sequence deltas are held until the missing seq arrives.
    If the buffer overflows (_MAX_GAP_BUFFER) the book marks itself not-ready
    and a re-snapshot must be applied before deltas are accepted again.
  - NATS JetStream sequence numbers (msg.metadata.sequence) are the authoritative
    sequencer because Nephtys' StreamEvent envelope has no seq field.
    Callers pass the NATS seq into apply_snapshot / apply_delta.
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
        self._gap_buffer: dict[int, tuple] = {}
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
        deltas and request a new snapshot from the exchange/Nephtys.
        """
        if not self._ready:
            return False

        expected = self._seq + 1

        if seq == expected:
            self._apply(side, price, qty)
            self._seq = seq
            self._drain_buffer()
            return True

        if seq <= self._seq:
            return True  # stale duplicate — ignore silently

        # Gap detected: buffer and keep waiting
        self._gap_buffer[seq] = (side, price, qty)

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

        logger.debug("[%s] Seq gap — expected %d got %d, buffered", self.symbol, expected, seq)
        return False

    def _apply(self, side: str, price: float, qty: float) -> None:
        book = self._bids if side == "bid" else self._asks
        if qty == 0.0:
            book.pop(price, None)
        else:
            book[price] = qty

    def _drain_buffer(self) -> None:
        while True:
            nxt = self._seq + 1
            if nxt not in self._gap_buffer:
                break
            side, price, qty = self._gap_buffer.pop(nxt)
            self._apply(side, price, qty)
            self._seq = nxt

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
