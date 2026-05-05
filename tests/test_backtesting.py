"""
Tests for backtesting/engine.py

Lance is mocked — these tests run without installing the lance package.
"""

import sys
from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from backtesting.engine import BacktestEngine, BacktestResult
from brain.strategies.moving_average import MovingAverageStrategy
from core.schemas import MarketEvent, TradeSignal

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_dataset(rows: list[dict]) -> MagicMock:
    """Return a mock lance dataset whose to_batches() yields a single pandas batch."""
    df = pd.DataFrame(rows)
    mock_batch = MagicMock()
    mock_batch.to_pandas.return_value = df
    mock_ds = MagicMock()
    mock_ds.to_batches.return_value = [mock_batch]
    return mock_ds


def _row(price: float, i: int = 0) -> dict:
    return {"source": "test", "type": "trade", "timestamp": i, "symbol": "BTC/USDT", "price": price}


@pytest.fixture
def mock_lance(monkeypatch):
    mock = MagicMock()
    monkeypatch.setitem(sys.modules, "lance", mock)
    return mock


# ---------------------------------------------------------------------------
# BacktestResult unit tests (no lance needed)
# ---------------------------------------------------------------------------


class TestBacktestResult:
    def test_win_rate_zero_trades(self):
        r = BacktestResult(strategy_id="x", params={})
        assert r.win_rate == 0.0

    def test_win_rate_calculation(self):
        r = BacktestResult(strategy_id="x", params={}, total_trades=4, wins=3)
        assert r.win_rate == pytest.approx(0.75)

    def test_sharpe_no_signals(self):
        r = BacktestResult(strategy_id="x", params={})
        assert r.sharpe == 0.0

    def test_to_dict_has_required_keys(self):
        r = BacktestResult(strategy_id="x", params={"window": 20})
        d = r.to_dict()
        for key in (
            "strategy_id",
            "params",
            "total_trades",
            "wins",
            "losses",
            "total_pnl",
            "max_drawdown",
            "win_rate",
            "sharpe",
        ):
            assert key in d, f"missing key: {key}"

    def test_to_dict_params_preserved(self):
        params = {"window": 10, "buy_threshold": 0.98}
        r = BacktestResult(strategy_id="ma", params=params)
        assert r.to_dict()["params"] == params


# ---------------------------------------------------------------------------
# BacktestEngine integration tests (lance mocked)
# ---------------------------------------------------------------------------


class TestBacktestEngine:
    def test_empty_dataset_zero_trades(self, mock_lance):
        mock_lance.dataset.return_value = _make_mock_dataset([])
        engine = BacktestEngine(MovingAverageStrategy, {"window": 5})
        result = engine.run("fake.lance")
        assert result.total_trades == 0
        assert result.total_pnl == 0.0

    def test_buy_sell_cycle_positive_pnl(self, mock_lance):
        """5 warmup prices at 100, then BUY at 97.9, SELL at 103.0."""
        rows = [_row(100.0, i) for i in range(5)] + [_row(97.9, 5), _row(103.0, 6)]
        mock_lance.dataset.return_value = _make_mock_dataset(rows)

        engine = BacktestEngine(MovingAverageStrategy, {"window": 5})
        result = engine.run("fake.lance")

        assert result.total_trades == 1
        assert result.wins == 1
        assert result.losses == 0
        assert result.total_pnl > 0
        assert result.win_rate == pytest.approx(1.0)

    def test_buy_sell_cycle_records_signals(self, mock_lance):
        rows = [_row(100.0, i) for i in range(5)] + [_row(97.9, 5), _row(103.0, 6)]
        mock_lance.dataset.return_value = _make_mock_dataset(rows)

        engine = BacktestEngine(MovingAverageStrategy, {"window": 5})
        result = engine.run("fake.lance")

        actions = [s.action for s in result.signals]
        assert "BUY" in actions
        assert "SELL" in actions

    def test_max_drawdown_computed(self, mock_lance):
        """A losing trade increases max_drawdown above zero."""

        # Force a BUY at a high price, then SELL at a lower price:
        # Use a mock strategy instead of fighting the MA logic
        class _ForcedStrategy:
            def __init__(self, **_):
                self._step = 0

            def evaluate(self, event: MarketEvent) -> TradeSignal | None:
                self._step += 1
                price = float(event.payload.get("price", 0))
                if self._step == 1:
                    return TradeSignal(
                        strategy_id="forced",
                        symbol="X",
                        action="BUY",
                        order_type="MARKET",
                        quantity=1.0,
                        price=price,
                        confidence_score=1.0,
                        allocation_percentage=10.0,
                        timestamp=datetime.now(),
                    )
                if self._step == 2:
                    return TradeSignal(
                        strategy_id="forced",
                        symbol="X",
                        action="SELL",
                        order_type="MARKET",
                        quantity=1.0,
                        price=price,
                        confidence_score=1.0,
                        allocation_percentage=10.0,
                        timestamp=datetime.now(),
                    )
                return None

        rows = [_row(200.0, 0), _row(150.0, 1)]  # BUY at 200, SELL at 150 → loss
        mock_lance.dataset.return_value = _make_mock_dataset(rows)

        engine = BacktestEngine(_ForcedStrategy, {}, initial_balance=10_000.0)
        result = engine.run("fake.lance")

        assert result.losses == 1
        assert result.total_pnl < 0
        assert result.max_drawdown > 0

    def test_two_engines_isolated(self, mock_lance):
        """Two BacktestEngine instances with different params must not share state."""
        rows = [_row(100.0, i) for i in range(5)] + [_row(97.9, 5), _row(103.0, 6)]
        # Both engines read the same dataset (reset mock each time)
        mock_lance.dataset.return_value = _make_mock_dataset(rows)
        e1 = BacktestEngine(MovingAverageStrategy, {"window": 5})
        r1 = e1.run("fake.lance")

        mock_lance.dataset.return_value = _make_mock_dataset(rows)
        e2 = BacktestEngine(MovingAverageStrategy, {"window": 5})
        r2 = e2.run("fake.lance")

        # Results should be identical (strategies don't share deque state)
        assert r1.total_trades == r2.total_trades
        assert r1.total_pnl == pytest.approx(r2.total_pnl)

    def test_malformed_rows_skipped(self, mock_lance):
        """Rows that cannot form a valid MarketEvent must not crash the engine."""
        bad_rows = [
            {"source": None, "type": None, "timestamp": None, "price": "not_a_float"},
            _row(100.0, 1),  # one good row to ensure the engine ran
        ]
        mock_lance.dataset.return_value = _make_mock_dataset(bad_rows)
        engine = BacktestEngine(MovingAverageStrategy, {"window": 5})
        # Should not raise
        result = engine.run("fake.lance")
        assert result is not None
