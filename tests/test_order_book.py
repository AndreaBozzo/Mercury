"""
Tests for core/order_book.py

Requires: sortedcontainers (skipped if not installed)
"""

import pytest

sortedcontainers = pytest.importorskip("sortedcontainers")

from core.order_book import _MAX_GAP_BUFFER, OrderBook  # noqa: E402


def _snap(book: OrderBook, bids=None, asks=None, seq: int = 1) -> None:
    book.apply_snapshot(bids or [], asks or [], seq)


def _simple_snap(book: OrderBook, seq: int = 1) -> None:
    """Apply a snapshot with a single bid/ask level."""
    _snap(book, bids=[[100.0, 5.0]], asks=[[101.0, 3.0]], seq=seq)


class TestEmptyBook:
    def test_not_ready_before_snapshot(self):
        book = OrderBook("BTC/USDT")
        assert not book.is_ready

    def test_best_bid_none(self):
        assert OrderBook("X").best_bid is None

    def test_best_ask_none(self):
        assert OrderBook("X").best_ask is None

    def test_mid_none(self):
        assert OrderBook("X").mid is None

    def test_spread_none(self):
        assert OrderBook("X").spread is None

    def test_imbalance_none_when_empty(self):
        book = OrderBook("X")
        _snap(book)  # empty snapshot
        assert book.imbalance() is None

    def test_delta_before_snapshot_returns_false(self):
        book = OrderBook("X")
        ok = book.apply_delta("bid", 100.0, 1.0, seq=1)
        assert not ok


class TestSnapshot:
    def test_is_ready_after_snapshot(self):
        book = OrderBook("BTC/USDT")
        _simple_snap(book)
        assert book.is_ready

    def test_best_bid_after_snapshot(self):
        book = OrderBook("BTC/USDT")
        _snap(book, bids=[[100.0, 5.0], [99.0, 2.0]], asks=[[101.0, 1.0]])
        assert book.best_bid == (100.0, 5.0)  # highest bid first

    def test_best_ask_after_snapshot(self):
        book = OrderBook("BTC/USDT")
        _snap(book, bids=[[100.0, 5.0]], asks=[[101.0, 1.0], [102.0, 2.0]])
        assert book.best_ask == (101.0, 1.0)  # lowest ask first

    def test_zero_qty_levels_excluded(self):
        book = OrderBook("X")
        _snap(book, bids=[[100.0, 0.0], [99.0, 1.0]], asks=[[101.0, 1.0]])
        assert book.best_bid == (99.0, 1.0)

    def test_second_snapshot_replaces_first(self):
        book = OrderBook("X")
        _snap(book, bids=[[100.0, 1.0]], asks=[[101.0, 1.0]], seq=1)
        _snap(book, bids=[[200.0, 9.0]], asks=[[201.0, 9.0]], seq=2)
        assert book.best_bid == (200.0, 9.0)
        assert book.best_ask == (201.0, 9.0)

    def test_mid_price(self):
        book = OrderBook("X")
        _snap(book, bids=[[100.0, 1.0]], asks=[[102.0, 1.0]])
        assert book.mid == 101.0

    def test_spread(self):
        book = OrderBook("X")
        _snap(book, bids=[[100.0, 1.0]], asks=[[103.0, 1.0]])
        assert book.spread == 3.0

    def test_imbalance_bid_heavy(self):
        book = OrderBook("X")
        _snap(book, bids=[[100.0, 9.0]], asks=[[101.0, 1.0]])
        imb = book.imbalance()
        assert imb is not None
        assert imb > 0  # more bid volume

    def test_imbalance_ask_heavy(self):
        book = OrderBook("X")
        _snap(book, bids=[[100.0, 1.0]], asks=[[101.0, 9.0]])
        imb = book.imbalance()
        assert imb is not None
        assert imb < 0  # more ask volume

    def test_imbalance_balanced(self):
        book = OrderBook("X")
        _snap(book, bids=[[100.0, 5.0]], asks=[[101.0, 5.0]])
        assert book.imbalance() == 0.0

    def test_levels_structure(self):
        book = OrderBook("BTC/USDT")
        _simple_snap(book, seq=7)
        lvl = book.levels()
        assert lvl["symbol"] == "BTC/USDT"
        assert lvl["seq"] == 7
        assert len(lvl["bids"]) == 1
        assert len(lvl["asks"]) == 1


class TestDeltaApply:
    def test_delta_adds_level(self):
        book = OrderBook("X")
        _simple_snap(book, seq=1)
        ok = book.apply_delta("bid", 99.5, 2.0, seq=2)
        assert ok
        assert book._bids[99.5] == 2.0

    def test_delta_updates_existing_level(self):
        book = OrderBook("X")
        _snap(book, bids=[[100.0, 5.0]], asks=[[101.0, 1.0]], seq=1)
        book.apply_delta("bid", 100.0, 8.0, seq=2)
        assert book._bids[100.0] == 8.0

    def test_delta_removes_level_on_zero_qty(self):
        book = OrderBook("X")
        _simple_snap(book, seq=1)
        book.apply_delta("bid", 100.0, 0.0, seq=2)
        assert 100.0 not in book._bids

    def test_stale_delta_ignored(self):
        book = OrderBook("X")
        _snap(book, bids=[[100.0, 5.0]], asks=[[101.0, 1.0]], seq=5)
        ok = book.apply_delta("bid", 99.0, 1.0, seq=3)  # older than current seq
        assert ok  # silently accepted
        assert 99.0 not in book._bids

    def test_exact_next_seq_accepted(self):
        book = OrderBook("X")
        _simple_snap(book, seq=10)
        ok = book.apply_delta("ask", 102.0, 1.0, seq=11)
        assert ok
        assert book._seq == 11


class TestDeltaBatchApply:
    def test_batch_applies_atomically_and_sets_final_seq(self):
        book = OrderBook("X")
        _simple_snap(book, seq=99)

        ok = book.apply_delta_batch(
            bids=[[100.0, 6.0], [99.5, 2.0]],
            asks=[[101.0, 0.0], [101.5, 3.0]],
            first_seq=100,
            final_seq=102,
        )

        assert ok
        assert book._seq == 102
        assert book.best_bid == (100.0, 6.0)
        assert book.best_ask == (101.5, 3.0)

    def test_batch_overlap_accepts_binance_range_after_snapshot(self):
        book = OrderBook("X")
        _simple_snap(book, seq=100)

        ok = book.apply_delta_batch(
            bids=[[100.5, 1.0]],
            asks=[],
            first_seq=98,
            final_seq=101,
        )

        assert ok
        assert book._seq == 101
        assert book.best_bid == (100.5, 1.0)

    def test_stale_batch_ignored(self):
        book = OrderBook("X")
        _simple_snap(book, seq=100)

        ok = book.apply_delta_batch(
            bids=[[150.0, 1.0]],
            asks=[],
            first_seq=90,
            final_seq=99,
        )

        assert ok
        assert book.best_bid == (100.0, 5.0)
        assert book._seq == 100

    def test_batch_gap_buffered_until_missing_range_arrives(self):
        book = OrderBook("X")
        _simple_snap(book, seq=100)

        ok = book.apply_delta_batch(
            bids=[[103.0, 1.0]],
            asks=[],
            first_seq=103,
            final_seq=104,
        )
        assert not ok
        assert 103.0 not in book._bids

        ok = book.apply_delta_batch(
            bids=[[101.0, 1.0]],
            asks=[],
            first_seq=101,
            final_seq=102,
        )

        assert ok
        assert book._seq == 104
        assert 101.0 in book._bids
        assert 103.0 in book._bids

    def test_invalid_batch_range_marks_book_not_ready(self):
        book = OrderBook("X")
        _simple_snap(book, seq=100)

        ok = book.apply_delta_batch(bids=[], asks=[], first_seq=102, final_seq=101)

        assert not ok
        assert not book.is_ready


class TestGapHandling:
    def test_gap_returns_false(self):
        book = OrderBook("X")
        _simple_snap(book, seq=1)
        ok = book.apply_delta("bid", 99.0, 1.0, seq=5)  # gap: expected 2
        assert not ok

    def test_gap_buffered_and_applied_when_filled(self):
        book = OrderBook("X")
        _simple_snap(book, seq=1)

        # Seq 3 arrives before seq 2
        book.apply_delta("bid", 98.0, 1.0, seq=3)
        assert 98.0 not in book._bids  # not applied yet

        # Seq 2 fills the gap
        book.apply_delta("bid", 99.0, 2.0, seq=2)
        assert 99.0 in book._bids  # seq 2 applied
        assert 98.0 in book._bids  # seq 3 drained from buffer
        assert book._seq == 3

    def test_gap_buffer_overflow_clears_book_readiness(self):
        book = OrderBook("X")
        _simple_snap(book, seq=1)

        # Flood the buffer beyond _MAX_GAP_BUFFER
        for i in range(_MAX_GAP_BUFFER + 1):
            book.apply_delta("bid", float(90 + i), 1.0, seq=100 + i)

        assert not book.is_ready
        assert len(book._gap_buffer) == 0

    def test_book_accepts_snapshot_after_overflow(self):
        book = OrderBook("X")
        _simple_snap(book, seq=1)
        for i in range(_MAX_GAP_BUFFER + 1):
            book.apply_delta("bid", float(90 + i), 1.0, seq=100 + i)

        # Fresh snapshot re-initialises the book
        _snap(book, bids=[[200.0, 1.0]], asks=[[201.0, 1.0]], seq=9999)
        assert book.is_ready
        assert book.best_bid == (200.0, 1.0)

    def test_multi_gap_drains_in_order(self):
        book = OrderBook("X")
        _simple_snap(book, seq=0)

        # Deliver 4, 3, 2, 1 out of order
        book.apply_delta("bid", 94.0, 1.0, seq=4)
        book.apply_delta("bid", 93.0, 1.0, seq=3)
        book.apply_delta("bid", 92.0, 1.0, seq=2)
        book.apply_delta("bid", 91.0, 1.0, seq=1)  # fills gap → drains 2, 3, 4

        assert book._seq == 4
        assert all(float(90 + i) in book._bids for i in range(1, 5))
