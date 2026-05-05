from datetime import datetime

from brain.strategies.order_book_vwap_imbalance import OrderBookVwapImbalanceStrategy
from core.schemas import MarketEvent, TradeSignal


def _ev(
    price: float,
    *,
    symbol: str = "BTC/USDT",
    volume: float = 1.0,
    imbalance: float | None = None,
    spread: float | None = None,
    mid: float | None = None,
) -> MarketEvent:
    payload = {"symbol": symbol, "price": price, "volume": volume}
    if imbalance is not None:
        payload["imbalance"] = imbalance
    if spread is not None:
        payload["spread"] = spread
    if mid is not None:
        payload["mid"] = mid
    return MarketEvent(
        source="test",
        type="trade",
        timestamp=int(datetime.now().timestamp() * 1000),
        payload=payload,
    )


def _warm(
    strategy: OrderBookVwapImbalanceStrategy,
    *,
    price: float = 100.0,
    count: int = 5,
    imbalance: float = 0.0,
) -> None:
    for _ in range(count):
        strategy.evaluate(_ev(price, imbalance=imbalance, spread=0.02, mid=100.0))


class TestOrderBookVwapImbalanceStrategy:
    def test_returns_none_until_vwap_and_imbalance_are_warm(self):
        strategy = OrderBookVwapImbalanceStrategy(vwap_window=5, imbalance_window=5)
        for _ in range(2):
            assert strategy.evaluate(_ev(100.0, imbalance=0.1, spread=0.02, mid=100.0)) is None

    def test_emits_buy_when_price_is_below_vwap_and_book_is_bid_heavy(self):
        strategy = OrderBookVwapImbalanceStrategy(
            vwap_window=5,
            imbalance_window=5,
            deviation_threshold=0.01,
            imbalance_threshold=0.2,
            max_spread_bps=5.0,
            imbalance_slope_threshold=0.05,
        )
        strategy.evaluate(_ev(100.0, imbalance=0.05, spread=0.02, mid=100.0))
        strategy.evaluate(_ev(100.0, imbalance=0.15, spread=0.02, mid=100.0))
        strategy.evaluate(_ev(100.0, imbalance=0.25, spread=0.02, mid=100.0))
        strategy.evaluate(_ev(100.0, imbalance=0.30, spread=0.02, mid=100.0))
        signal = strategy.evaluate(_ev(98.0, imbalance=0.45, spread=0.02, mid=99.0))
        assert isinstance(signal, TradeSignal)
        assert signal.action == "BUY"

    def test_emits_sell_when_price_is_above_vwap_and_book_is_ask_heavy(self):
        strategy = OrderBookVwapImbalanceStrategy(
            vwap_window=5,
            imbalance_window=5,
            deviation_threshold=0.01,
            imbalance_threshold=0.2,
            max_spread_bps=5.0,
            imbalance_slope_threshold=0.05,
        )
        strategy.evaluate(_ev(100.0, imbalance=-0.05, spread=0.02, mid=100.0))
        strategy.evaluate(_ev(100.0, imbalance=-0.15, spread=0.02, mid=100.0))
        strategy.evaluate(_ev(100.0, imbalance=-0.25, spread=0.02, mid=100.0))
        strategy.evaluate(_ev(100.0, imbalance=-0.30, spread=0.02, mid=100.0))
        signal = strategy.evaluate(_ev(102.0, imbalance=-0.45, spread=0.02, mid=101.0))
        assert isinstance(signal, TradeSignal)
        assert signal.action == "SELL"

    def test_rejects_signal_when_spread_is_too_wide(self):
        strategy = OrderBookVwapImbalanceStrategy(vwap_window=5, imbalance_window=5)
        _warm(strategy)
        assert strategy.evaluate(_ev(98.0, imbalance=0.5, spread=0.50, mid=100.0)) is None

    def test_returns_none_without_book_features(self):
        strategy = OrderBookVwapImbalanceStrategy(vwap_window=5, imbalance_window=5)
        _warm(strategy)
        assert strategy.evaluate(_ev(98.0)) is None

    def test_normalizes_compact_symbol(self):
        strategy = OrderBookVwapImbalanceStrategy(vwap_window=5, imbalance_window=5)
        strategy.evaluate(_ev(100.0, imbalance=0.05, spread=0.02, mid=100.0, symbol="BTCUSDT"))
        strategy.evaluate(_ev(100.0, imbalance=0.15, spread=0.02, mid=100.0, symbol="BTCUSDT"))
        strategy.evaluate(_ev(100.0, imbalance=0.25, spread=0.02, mid=100.0, symbol="BTCUSDT"))
        strategy.evaluate(_ev(100.0, imbalance=0.30, spread=0.02, mid=100.0, symbol="BTCUSDT"))
        signal = strategy.evaluate(
            _ev(98.0, imbalance=0.45, spread=0.02, mid=99.0, symbol="BTCUSDT")
        )
        assert signal is not None
        assert signal.symbol == "BTC/USDT"