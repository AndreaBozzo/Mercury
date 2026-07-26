"""
Moving average strategy — refactored to a class for Ray/backtesting safety.

The module-level `evaluate_moving_average` function is kept as a shim so
existing callers (brain/main.py v0.1) continue to work unchanged.

Why class-based:
  The old design stored PRICE_HISTORY as a module-level deque, which means:
    - Two backtest runs in the same process share state → wrong results
    - Ray workers each import the module in a fresh subprocess, but within
      one worker different parameter combinations would stomp each other
  A class instance per (strategy, params, worker), with history keyed by event
  source and symbol, eliminates both problems.
"""

from __future__ import annotations

import statistics
from collections import deque
from datetime import UTC, datetime
from math import isfinite
from typing import Literal

from core.schemas import INTENT_PLACEHOLDER_QTY, MarketEvent, TradeSignal
from core.symbols import normalize_symbol


class MovingAverageStrategy:
    """
    Stateful moving average strategy — safe to instantiate many times in
    parallel (no shared module state).

    Parameters
    ----------
    window : int
        Number of ticks in the rolling window.
    buy_threshold : float
        Fraction below MA that triggers a BUY  (e.g. 0.99 = 1% below MA).
    sell_threshold : float
        Fraction above MA that triggers a SELL (e.g. 1.01 = 1% above MA).
    """

    requires_order_book = False

    def __init__(
        self,
        window: int = 20,
        buy_threshold: float = 0.99,
        sell_threshold: float = 1.01,
    ) -> None:
        self.window = window
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self._prices: dict[tuple[str, str], deque[float]] = {}

    def evaluate(self, event: MarketEvent) -> TradeSignal | None:
        payload = event.payload
        try:
            symbol = normalize_symbol(payload.get("symbol"))
        except ValueError:
            return None
        try:
            price = float(payload.get("price", 0.0))
        except (TypeError, ValueError):
            return None

        if not isfinite(price) or price <= 0:
            return None

        state_key = (event.source.strip().casefold(), symbol)
        prices = self._prices.setdefault(state_key, deque(maxlen=self.window))
        prices.append(price)
        if len(prices) < self.window:
            return None

        ma = statistics.mean(prices)

        action: Literal["BUY", "SELL"]
        if price < ma * self.buy_threshold:
            action = "BUY"
        elif price > ma * self.sell_threshold:
            action = "SELL"
        else:
            return None

        deviation = abs(price - ma) / ma
        confidence = min(1.0, deviation * 10)

        return TradeSignal(
            strategy_id=f"moving_average_w{self.window}",
            symbol=symbol,
            action=action,
            order_type="MARKET",
            quantity=INTENT_PLACEHOLDER_QTY,
            price=price,
            confidence_score=confidence,
            allocation_percentage=0.1 * confidence * 100,
            timestamp=datetime.now(UTC),
        )


# ---------------------------------------------------------------------------
# Backward-compatible module-level function
# ---------------------------------------------------------------------------

_default = MovingAverageStrategy()


def evaluate_moving_average(event: MarketEvent) -> TradeSignal | None:
    return _default.evaluate(event)
