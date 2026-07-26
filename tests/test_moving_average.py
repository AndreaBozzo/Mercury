"""
Tests for brain/strategies/moving_average.py

All tests run without optional dependencies (only pydantic + stdlib).
"""

import importlib
from datetime import datetime

from brain.strategies.moving_average import MovingAverageStrategy
from core.schemas import MarketEvent, TradeSignal


def _ev(price: float, symbol: str = "BTC/USDT", source: str = "test") -> MarketEvent:
    return MarketEvent(
        source=source,
        type="trade",
        timestamp=int(datetime.now().timestamp() * 1000),
        payload={"symbol": symbol, "price": price},
    )


def _fill(strategy: MovingAverageStrategy, price: float, n: int) -> None:
    """Push `n` events at `price` into a strategy to warm it up."""
    for _ in range(n):
        strategy.evaluate(_ev(price))


class TestMovingAverageStrategy:
    def test_does_not_require_order_book_readiness(self):
        assert MovingAverageStrategy.requires_order_book is False

    def test_returns_none_during_warmup(self):
        s = MovingAverageStrategy(window=5)
        for i in range(4):
            assert s.evaluate(_ev(100.0)) is None, f"expected None at tick {i}"

    def test_returns_signal_after_warmup(self):
        s = MovingAverageStrategy(window=5)
        _fill(s, 100.0, 5)  # fully warmed up
        # Price at exactly MA — no signal
        result = s.evaluate(_ev(100.0))
        assert result is None

    def test_buy_signal_below_threshold(self):
        s = MovingAverageStrategy(window=5, buy_threshold=0.99)
        _fill(s, 100.0, 5)
        # MA ≈ 100. Need price < 100 * 0.99 = 99.  Use 97 to be safe.
        result = s.evaluate(_ev(97.0))
        assert isinstance(result, TradeSignal)
        assert result.action == "BUY"

    def test_sell_signal_above_threshold(self):
        s = MovingAverageStrategy(window=5, sell_threshold=1.01)
        _fill(s, 100.0, 5)
        # MA ≈ 100. Need price > 100 * 1.01 = 101.  Use 103 to be safe.
        result = s.evaluate(_ev(103.0))
        assert isinstance(result, TradeSignal)
        assert result.action == "SELL"

    def test_no_signal_within_band(self):
        s = MovingAverageStrategy(window=5, buy_threshold=0.99, sell_threshold=1.01)
        _fill(s, 100.0, 5)
        assert s.evaluate(_ev(100.5)) is None
        assert s.evaluate(_ev(99.5)) is None

    def test_zero_price_returns_none(self):
        s = MovingAverageStrategy(window=5)
        _fill(s, 100.0, 5)
        assert s.evaluate(_ev(0.0)) is None

    def test_negative_price_returns_none(self):
        s = MovingAverageStrategy(window=5)
        assert s.evaluate(_ev(-1.0)) is None

    def test_invalid_and_non_finite_prices_do_not_poison_history(self):
        s = MovingAverageStrategy(window=3)
        assert s.evaluate(_ev(float("nan"))) is None
        invalid = _ev(100.0).model_copy(update={"payload": {"symbol": "BTC/USDT", "price": "bad"}})
        assert s.evaluate(invalid) is None
        assert s.evaluate(_ev(100.0)) is None
        assert s.evaluate(_ev(100.0)) is None
        assert s.evaluate(_ev(100.0)) is None

    def test_confidence_scales_with_deviation(self):
        s = MovingAverageStrategy(window=5)
        _fill(s, 100.0, 5)
        sig_small = s.evaluate(_ev(98.5))  # small deviation from MA ≈ 100
        s2 = MovingAverageStrategy(window=5)
        _fill(s2, 100.0, 5)
        sig_large = s2.evaluate(_ev(90.0))  # large deviation
        assert sig_small is not None
        assert sig_large is not None
        assert sig_large.confidence_score > sig_small.confidence_score

    def test_confidence_capped_at_one(self):
        s = MovingAverageStrategy(window=5)
        _fill(s, 100.0, 5)
        sig = s.evaluate(_ev(50.0))  # massive deviation
        assert sig is not None
        assert sig.confidence_score <= 1.0

    def test_strategy_id_includes_window(self):
        s = MovingAverageStrategy(window=7)
        _fill(s, 100.0, 7)
        sig = s.evaluate(_ev(90.0))
        assert sig is not None
        assert "7" in sig.strategy_id

    def test_state_isolation_between_instances(self):
        """Two instances with different history must not interfere."""
        s1 = MovingAverageStrategy(window=5)
        s2 = MovingAverageStrategy(window=5)

        _fill(s1, 100.0, 5)  # s1: MA=100
        _fill(s2, 200.0, 5)  # s2: MA=200

        sig1 = s1.evaluate(_ev(97.0))  # 97 < 99 → BUY for s1
        sig2 = s2.evaluate(_ev(197.0))  # 197 < 198 → BUY for s2

        assert sig1 is not None and sig1.action == "BUY"
        assert sig2 is not None and sig2.action == "BUY"

    def test_state_isolation_between_instruments(self):
        """One strategy instance must warm each venue/product independently."""
        strategy = MovingAverageStrategy(window=3)

        assert strategy.evaluate(_ev(100.0, symbol="BTC/USDT")) is None
        assert strategy.evaluate(_ev(100.0, symbol="BTC/USDT")) is None
        assert strategy.evaluate(_ev(200.0, symbol="ETH/EUR")) is None
        assert strategy.evaluate(_ev(200.0, symbol="ETH/EUR")) is None
        assert strategy.evaluate(_ev(200.0, symbol="ETH/EUR")) is None

    def test_state_isolation_between_sources(self):
        """The same product on two venues must not share price history."""
        strategy = MovingAverageStrategy(window=3)

        assert strategy.evaluate(_ev(100.0, symbol="BTC/EUR", source="coinbase")) is None
        assert strategy.evaluate(_ev(100.0, symbol="BTC/EUR", source="coinbase")) is None
        assert strategy.evaluate(_ev(200.0, symbol="BTC/EUR", source="kraken")) is None
        assert strategy.evaluate(_ev(200.0, symbol="BTC/EUR", source="kraken")) is None
        assert strategy.evaluate(_ev(200.0, symbol="BTC/EUR", source="kraken")) is None

    def test_custom_thresholds(self):
        # Tighter band
        s = MovingAverageStrategy(window=5, buy_threshold=0.999, sell_threshold=1.001)
        _fill(s, 100.0, 5)
        # 99.8 < 99.9 (= 100 * 0.999) → BUY
        sig = s.evaluate(_ev(99.8))
        assert sig is not None and sig.action == "BUY"

    def test_normalizes_binance_compact_symbol_in_emitted_signal(self):
        s = MovingAverageStrategy(window=5, buy_threshold=0.99)
        _fill(s, 100.0, 5)
        sig = s.evaluate(_ev(97.0, symbol="BTCUSDT"))
        assert sig is not None
        assert sig.symbol == "BTC/USDT"

    def test_invalid_symbol_returns_none(self):
        s = MovingAverageStrategy(window=5, buy_threshold=0.99)
        _fill(s, 100.0, 5)
        assert s.evaluate(_ev(97.0, symbol="")) is None


class TestBackwardCompatShim:
    def test_shim_is_callable(self):
        """evaluate_moving_average() must exist and return TradeSignal | None."""
        import brain.strategies.moving_average as ma_mod

        # Fresh state so warm-up assertions are reliable
        importlib.reload(ma_mod)
        result = ma_mod.evaluate_moving_average(_ev(100.0))
        assert result is None or isinstance(result, TradeSignal)

    def test_shim_delegates_to_default_instance(self):
        import brain.strategies.moving_average as ma_mod

        importlib.reload(ma_mod)
        # After reload _default has empty history; 20 events needed
        for _ in range(20):
            ma_mod.evaluate_moving_average(_ev(100.0))
        # Now warmed up — a deep price should produce a signal
        result = ma_mod.evaluate_moving_average(_ev(80.0))
        assert result is not None
        assert isinstance(result, TradeSignal)
