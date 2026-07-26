from __future__ import annotations

import os

from brain.strategies.moving_average import MovingAverageStrategy
from brain.strategies.order_book_vwap_imbalance import OrderBookVwapImbalanceStrategy


def build_strategy_from_env():
    strategy_name = os.getenv("BRAIN_STRATEGY", "moving_average").strip().lower()

    if strategy_name in {"moving_average", "ma"}:
        return MovingAverageStrategy()

    if strategy_name in {"order_book_vwap_imbalance", "vwap_imbalance", "obi_vwap"}:
        return OrderBookVwapImbalanceStrategy()

    raise ValueError(f"Unsupported BRAIN_STRATEGY={strategy_name!r}")


__all__ = ["MovingAverageStrategy", "OrderBookVwapImbalanceStrategy", "build_strategy_from_env"]
