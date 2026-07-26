from __future__ import annotations

from collections import deque
from datetime import UTC, datetime
from math import isfinite
from typing import Literal

from core.schemas import INTENT_PLACEHOLDER_QTY, MarketEvent, TradeSignal
from core.symbols import normalize_symbol


class OrderBookVwapImbalanceStrategy:
    requires_order_book = True

    def __init__(
        self,
        vwap_window: int = 50,
        imbalance_window: int = 20,
        deviation_threshold: float = 0.002,
        imbalance_threshold: float = 0.2,
        max_spread_bps: float = 5.0,
        imbalance_slope_threshold: float = 0.02,
    ) -> None:
        self.vwap_window = vwap_window
        self.imbalance_window = imbalance_window
        self.deviation_threshold = deviation_threshold
        self.imbalance_threshold = imbalance_threshold
        self.max_spread_bps = max_spread_bps
        self.imbalance_slope_threshold = imbalance_slope_threshold
        self._price_volume: dict[tuple[str, str], deque[tuple[float, float]]] = {}
        self._imbalances: dict[tuple[str, str], deque[float]] = {}

    def evaluate(self, event: MarketEvent) -> TradeSignal | None:
        payload = event.payload
        try:
            symbol = normalize_symbol(payload.get("symbol"))
        except ValueError:
            return None

        price = _as_float(payload.get("price"))
        if price is None or price <= 0:
            return None

        volume = _as_float(payload.get("volume")) or _as_float(payload.get("qty")) or 1.0
        if volume <= 0:
            volume = 1.0

        state_key = (event.source.strip().casefold(), symbol)
        price_volume = self._price_volume.setdefault(
            state_key,
            deque(maxlen=self.vwap_window),
        )
        price_volume.append((price, volume))
        vwap = self._current_vwap(price_volume)
        if vwap is None:
            return None

        imbalance = _as_float(payload.get("imbalance"))
        spread = _as_float(payload.get("spread"))
        mid = _as_float(payload.get("mid")) or price
        if imbalance is None or spread is None or mid <= 0:
            return None

        imbalances = self._imbalances.setdefault(
            state_key,
            deque(maxlen=self.imbalance_window),
        )
        imbalances.append(imbalance)
        if len(imbalances) < min(3, self.imbalance_window):
            return None

        mean_imbalance = sum(imbalances) / len(imbalances)
        imbalance_slope = imbalances[-1] - imbalances[0]
        spread_bps = spread / mid * 10_000
        if spread_bps > self.max_spread_bps:
            return None

        deviation = (price - vwap) / vwap
        action: Literal["BUY", "SELL"] | None = None
        if (
            deviation <= -self.deviation_threshold
            and mean_imbalance >= self.imbalance_threshold
            and imbalance_slope >= self.imbalance_slope_threshold
        ):
            action = "BUY"
        elif (
            deviation >= self.deviation_threshold
            and mean_imbalance <= -self.imbalance_threshold
            and imbalance_slope <= -self.imbalance_slope_threshold
        ):
            action = "SELL"

        if action is None:
            return None

        confidence = min(
            1.0,
            (abs(deviation) / self.deviation_threshold) * 0.5
            + (abs(mean_imbalance) / max(self.imbalance_threshold, 1e-9)) * 0.3
            + min(spread_bps / max(self.max_spread_bps, 1e-9), 1.0) * 0.2,
        )

        return TradeSignal(
            strategy_id="order_book_vwap_imbalance_v1",
            symbol=symbol,
            action=action,
            order_type="MARKET",
            quantity=INTENT_PLACEHOLDER_QTY,
            price=price,
            confidence_score=confidence,
            allocation_percentage=min(100.0, confidence * 10.0),
            timestamp=datetime.now(UTC),
        )

    @staticmethod
    def _current_vwap(price_volume: deque[tuple[float, float]]) -> float | None:
        if not price_volume:
            return None
        total_volume = sum(volume for _, volume in price_volume)
        if total_volume <= 0:
            return None
        return sum(price * volume for price, volume in price_volume) / total_volume


def _as_float(value) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if isfinite(number) else None
