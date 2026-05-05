"""
Event-sourced backtesting engine.

Design goal: production code-path parity.
  The same strategy class and evaluate() call used in brain/main.py runs
  unchanged here.  Historical data from the Lance archive is replayed as
  MarketEvent objects — structurally identical to what arrive over NATS in
  live trading.  There is no special "backtesting mode"; if you change the
  strategy logic, both live and backtest behaviour change together.

Lance replay vs. NATS replay:
  Publishing to a real NATS server and subscribing to it would guarantee
  exact production parity, but adds ~200 µs per message of broker latency
  and requires a live NATS instance.  Reading Lance directly is ~10× faster
  for offline parameter sweeps and is safe because:
    - The archived rows were written by Plutarch using the same MarketEvent
      payload structure that brain/main.py consumes.
    - We call strategy.evaluate(event) directly, bypassing only the async
      NATS ACK round-trip (irrelevant for correctness testing).

P&L simulation:
  Simple long-only model — enter on BUY, exit on SELL.  Replace with a more
  sophisticated model (slippage, fees, position sizing) once the order book
  feed is live.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from core.schemas import MarketEvent, TradeSignal

logger = logging.getLogger("backtesting")


@dataclass
class BacktestResult:
    strategy_id: str
    params: dict
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0
    signals: list[TradeSignal] = field(default_factory=list, repr=False)

    @property
    def win_rate(self) -> float:
        return self.wins / self.total_trades if self.total_trades else 0.0

    @property
    def sharpe(self) -> float:
        """Simplified Sharpe over per-trade returns (no risk-free rate)."""
        if not self.signals:
            return 0.0
        returns = []
        entry: tuple[float, float] | None = None
        for sig in self.signals:
            price = sig.price or 0.0
            if sig.action == "BUY" and entry is None:
                entry = (price, sig.quantity)
            elif sig.action == "SELL" and entry is not None:
                ret = (price - entry[0]) / entry[0]
                returns.append(ret)
                entry = None
        if len(returns) < 2:
            return 0.0
        mean = sum(returns) / len(returns)
        var = sum((r - mean) ** 2 for r in returns) / len(returns)
        std = var**0.5
        return mean / std if std else 0.0

    def to_dict(self) -> dict:
        return {
            "strategy_id": self.strategy_id,
            "params": self.params,
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "total_pnl": self.total_pnl,
            "max_drawdown": self.max_drawdown,
            "win_rate": self.win_rate,
            "sharpe": self.sharpe,
        }


class BacktestEngine:
    """
    Replays a Lance dataset through a strategy, returning a BacktestResult.

    Parameters
    ----------
    strategy_factory : callable
        A class (or factory function) that accepts **params and returns an
        object with an evaluate(event: MarketEvent) -> Optional[TradeSignal]
        method.  MovingAverageStrategy is the canonical example.
    params : dict
        Keyword arguments forwarded to strategy_factory.
    initial_balance : float
        Starting virtual balance for P&L simulation.
    """

    def __init__(
        self,
        strategy_factory: Callable[..., Any],
        params: dict,
        initial_balance: float = 10_000.0,
    ) -> None:
        self.params = params
        self.initial_balance = initial_balance
        self._strategy = strategy_factory(**params)
        self._result = BacktestResult(
            strategy_id=type(self._strategy).__name__,
            params=params,
        )

    def run(self, dataset_path: str, batch_size: int = 1_000) -> BacktestResult:
        """
        Stream through the Lance dataset in Arrow batches and call the
        strategy for every row.  Uses Lance's native batch reader so only
        `batch_size` rows are in memory at a time.
        """
        import lance

        ds = lance.dataset(dataset_path)
        balance = self.initial_balance
        peak_balance = balance
        position: tuple[float, float] | None = None  # (entry_price, qty)

        for arrow_batch in ds.to_batches(batch_size=batch_size):
            df = arrow_batch.to_pandas()
            for _, row in df.iterrows():
                payload = {k: v for k, v in row.items() if not k.startswith("_") and v is not None}
                try:
                    event = MarketEvent(
                        source=str(row.get("source", "archive")),
                        type=str(row.get("type", "trade")),
                        timestamp=int(row.get("timestamp", 0)),
                        payload=payload,
                    )
                except Exception:
                    continue

                signal: TradeSignal | None = self._strategy.evaluate(event)
                if signal is None:
                    continue

                price = float(signal.price or 0)
                if price <= 0:
                    continue

                self._result.signals.append(signal)

                if signal.action == "BUY" and position is None:
                    position = (price, signal.quantity)
                    self._result.total_trades += 1

                elif signal.action == "SELL" and position is not None:
                    entry_price, qty = position
                    pnl = (price - entry_price) * qty
                    balance += pnl
                    self._result.total_pnl += pnl

                    if pnl > 0:
                        self._result.wins += 1
                    else:
                        self._result.losses += 1

                    position = None

                    if balance > peak_balance:
                        peak_balance = balance

                    drawdown = (peak_balance - balance) / peak_balance
                    if drawdown > self._result.max_drawdown:
                        self._result.max_drawdown = drawdown

        return self._result
