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
            "ending_equity",
            "fees_paid",
            "turnover",
            "open_positions",
            "unfilled_signals",
            "max_drawdown",
            "win_rate",
            "sharpe",
        ):
            assert key in d, f"missing key: {key}"

    def test_to_dict_params_preserved(self):
        params = {"window": 10, "buy_threshold": 0.98}
        r = BacktestResult(strategy_id="ma", params=params)
        assert r.to_dict()["params"] == params

    def test_result_balances_start_at_configured_initial_balance(self):
        result = BacktestResult(strategy_id="x", params={}, initial_balance=123.0)
        assert result.ending_equity == 123.0
        assert result.cash_balance == 123.0


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
                        symbol="X/USDT",
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
                        symbol="X/USDT",
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

    def test_same_engine_can_be_run_twice_without_state_leakage(self, mock_lance):
        rows = [_row(100.0, i) for i in range(5)] + [_row(97.9, 5), _row(103.0, 6)]
        mock_lance.dataset.return_value = _make_mock_dataset(rows)
        engine = BacktestEngine(MovingAverageStrategy, {"window": 5})

        first = engine.run("fake.lance").to_dict()
        second = engine.run("fake.lance").to_dict()

        assert second == first

    def test_intent_quantity_does_not_control_simulated_position_size(self, mock_lance):
        class _QuantityStrategy:
            def __init__(self, quantity: float):
                self.quantity = quantity
                self.step = 0

            def evaluate(self, event: MarketEvent) -> TradeSignal:
                self.step += 1
                return TradeSignal(
                    strategy_id="quantity",
                    symbol="BTC/USDT",
                    action="BUY" if self.step == 1 else "SELL",
                    order_type="MARKET",
                    quantity=self.quantity,
                    price=float(event.payload["price"]),
                    confidence_score=1.0,
                    allocation_percentage=10.0,
                    timestamp=datetime.now(),
                )

        rows = [_row(100.0, 0), _row(110.0, 1)]
        mock_lance.dataset.return_value = _make_mock_dataset(rows)
        tiny = BacktestEngine(
            _QuantityStrategy,
            {"quantity": 1e-8},
            fee_bps=0,
            slippage_bps=0,
        ).run("fake.lance")

        mock_lance.dataset.return_value = _make_mock_dataset(rows)
        huge = BacktestEngine(
            _QuantityStrategy,
            {"quantity": 1_000.0},
            fee_bps=0,
            slippage_bps=0,
        ).run("fake.lance")

        assert tiny.total_pnl == pytest.approx(100.0)
        assert huge.total_pnl == pytest.approx(tiny.total_pnl)

    def test_default_costs_make_flat_round_trip_negative(self, mock_lance):
        class _RoundTripStrategy:
            def __init__(self):
                self.step = 0

            def evaluate(self, event: MarketEvent) -> TradeSignal:
                self.step += 1
                return TradeSignal(
                    strategy_id="round_trip",
                    symbol="BTC/USDT",
                    action="BUY" if self.step == 1 else "SELL",
                    order_type="MARKET",
                    quantity=1e-8,
                    price=float(event.payload["price"]),
                    confidence_score=1.0,
                    allocation_percentage=10.0,
                    timestamp=datetime.now(),
                )

        mock_lance.dataset.return_value = _make_mock_dataset([_row(100.0), _row(100.0, 1)])

        result = BacktestEngine(_RoundTripStrategy, {}).run("fake.lance")

        assert result.total_trades == 1
        assert result.total_pnl < 0
        assert result.fees_paid > 0
        assert result.turnover > 0

    def test_open_position_equity_includes_estimated_exit_costs(self, mock_lance):
        class _BuyStrategy:
            def evaluate(self, event: MarketEvent) -> TradeSignal:
                return TradeSignal(
                    strategy_id="buy",
                    symbol="BTC/USDT",
                    action="BUY",
                    order_type="MARKET",
                    quantity=1e-8,
                    price=float(event.payload["price"]),
                    confidence_score=1.0,
                    allocation_percentage=10.0,
                    timestamp=datetime.now(),
                )

        mock_lance.dataset.return_value = _make_mock_dataset([_row(100.0)])

        result = BacktestEngine(_BuyStrategy, {}).run("fake.lance")

        assert result.open_positions == 1
        assert result.unrealized_pnl < 0
        assert result.total_pnl == pytest.approx(result.unrealized_pnl)

    def test_sell_from_another_instrument_cannot_close_position(self, mock_lance):
        class _CrossInstrumentStrategy:
            def __init__(self):
                self.step = 0

            def evaluate(self, event: MarketEvent) -> TradeSignal:
                self.step += 1
                return TradeSignal(
                    strategy_id="cross_instrument",
                    symbol=str(event.payload["symbol"]),
                    action="BUY" if self.step == 1 else "SELL",
                    order_type="MARKET",
                    quantity=1.0,
                    price=float(event.payload["price"]),
                    confidence_score=1.0,
                    allocation_percentage=10.0,
                    timestamp=datetime.now(),
                )

        rows = [_row(100.0, 0), {**_row(200.0, 1), "symbol": "ETH/USDT"}]
        mock_lance.dataset.return_value = _make_mock_dataset(rows)

        result = BacktestEngine(
            _CrossInstrumentStrategy,
            {},
            fee_bps=0,
            slippage_bps=0,
        ).run("fake.lance")

        assert result.total_trades == 0
        assert result.open_positions == 1
        assert result.unfilled_signals == 1

    def test_limit_signal_is_not_assumed_filled(self, mock_lance):
        class _LimitStrategy:
            def evaluate(self, event: MarketEvent) -> TradeSignal:
                return TradeSignal(
                    strategy_id="limit",
                    symbol="BTC/USDT",
                    action="BUY",
                    order_type="LIMIT",
                    quantity=1.0,
                    price=float(event.payload["price"]),
                    confidence_score=1.0,
                    allocation_percentage=10.0,
                    timestamp=datetime.now(),
                )

        mock_lance.dataset.return_value = _make_mock_dataset([_row(100.0)])

        result = BacktestEngine(_LimitStrategy, {}).run("fake.lance")

        assert result.total_trades == 0
        assert result.open_positions == 0
        assert result.unfilled_signals == 1

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"initial_balance": 0}, "initial_balance"),
            ({"fee_bps": -1}, "fee_bps"),
            ({"fee_bps": 10_000}, "fee_bps"),
            ({"slippage_bps": 10_000}, "slippage_bps"),
            ({"max_position_fraction": 0}, "max_position_fraction"),
        ],
    )
    def test_invalid_fill_model_configuration_rejected(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            BacktestEngine(MovingAverageStrategy, {}, **kwargs)
