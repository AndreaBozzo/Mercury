"""Event-sourced strategy replay with a conservative, explicit fill model.

The same strategy class and ``evaluate()`` call used by the Brain receives
``MarketEvent`` objects reconstructed from Plutarch rows. Broker acknowledgement
behavior is outside this offline replay boundary.

The fill model is intentionally modest:

- long-only MARKET round trips;
- independent positions per event source and canonical symbol;
- cash-backed sizing from ``TradeSignal.allocation_percentage``;
- configurable fees and adverse slippage on entry and exit;
- mark-to-market equity and drawdown;
- LIMIT signals counted as unfilled rather than optimistically filled.

It still does not model latency, order-book liquidity, partial fills, rejects,
funding, or venue-specific rules. Results are research diagnostics, not evidence
that an order would have filled or that a strategy has an edge.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from math import isfinite
from typing import Any

from core.schemas import MarketEvent, TradeSignal
from core.symbols import normalize_symbol

logger = logging.getLogger("backtesting")


def _payload_from_row(row: Any) -> dict[str, Any]:
    """Reconstruct the original MarketEvent payload from a current or legacy row."""
    payload_json = row.get("payload_json") if hasattr(row, "get") else None
    if isinstance(payload_json, str) and payload_json:
        try:
            decoded = json.loads(payload_json)
            if isinstance(decoded, dict):
                return decoded
        except ValueError:
            pass

    envelope_keys = {"source", "type", "timestamp", "seq", "payload_json"}
    return {
        key: value
        for key, value in row.items()
        if not key.startswith("_") and key not in envelope_keys and value is not None
    }


@dataclass
class BacktestResult:
    strategy_id: str
    params: dict
    initial_balance: float = 10_000.0
    ending_equity: float = field(init=False)
    cash_balance: float = field(init=False)
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    max_drawdown: float = 0.0
    fees_paid: float = 0.0
    turnover: float = 0.0
    open_positions: int = 0
    unfilled_signals: int = 0
    fee_bps: float = 10.0
    slippage_bps: float = 5.0
    signals: list[TradeSignal] = field(default_factory=list, repr=False)
    trade_returns: list[float] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        self.ending_equity = self.initial_balance
        self.cash_balance = self.initial_balance

    @property
    def win_rate(self) -> float:
        return self.wins / self.total_trades if self.total_trades else 0.0

    @property
    def sharpe(self) -> float:
        """Unannualized Sharpe-like ratio over net completed-trade returns."""
        if len(self.trade_returns) < 2:
            return 0.0
        mean = sum(self.trade_returns) / len(self.trade_returns)
        variance = sum((value - mean) ** 2 for value in self.trade_returns) / len(
            self.trade_returns
        )
        standard_deviation = variance**0.5
        return mean / standard_deviation if standard_deviation else 0.0

    def to_dict(self) -> dict:
        return {
            "strategy_id": self.strategy_id,
            "params": self.params,
            "initial_balance": self.initial_balance,
            "ending_equity": self.ending_equity,
            "cash_balance": self.cash_balance,
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "total_pnl": self.total_pnl,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "max_drawdown": self.max_drawdown,
            "fees_paid": self.fees_paid,
            "turnover": self.turnover,
            "open_positions": self.open_positions,
            "unfilled_signals": self.unfilled_signals,
            "fee_bps": self.fee_bps,
            "slippage_bps": self.slippage_bps,
            "win_rate": self.win_rate,
            "sharpe": self.sharpe,
        }


@dataclass(frozen=True)
class _Position:
    quantity: float
    entry_price: float
    entry_cost: float


class BacktestEngine:
    """Replay a Lance dataset through a fresh strategy and cash ledger per run."""

    def __init__(
        self,
        strategy_factory: Callable[..., Any],
        params: dict,
        initial_balance: float = 10_000.0,
        *,
        fee_bps: float = 10.0,
        slippage_bps: float = 5.0,
        max_position_fraction: float = 0.10,
    ) -> None:
        if not isfinite(initial_balance) or initial_balance <= 0:
            raise ValueError("initial_balance must be finite and greater than zero")
        if not isfinite(fee_bps) or not 0 <= fee_bps < 10_000:
            raise ValueError("fee_bps must be finite and in [0, 10000)")
        if not isfinite(slippage_bps) or not 0 <= slippage_bps < 10_000:
            raise ValueError("slippage_bps must be finite and in [0, 10000)")
        if not isfinite(max_position_fraction) or not 0 < max_position_fraction <= 1:
            raise ValueError("max_position_fraction must be finite and in (0, 1]")

        self.strategy_factory = strategy_factory
        self.params = dict(params)
        self.initial_balance = initial_balance
        self.fee_bps = fee_bps
        self.slippage_bps = slippage_bps
        self.max_position_fraction = max_position_fraction

    def run(self, dataset_path: str, batch_size: int = 1_000) -> BacktestResult:
        """Replay the dataset with fresh strategy, portfolio, and result state."""
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")

        import lance

        strategy = self.strategy_factory(**self.params)
        result = BacktestResult(
            strategy_id=type(strategy).__name__,
            params=dict(self.params),
            initial_balance=self.initial_balance,
            fee_bps=self.fee_bps,
            slippage_bps=self.slippage_bps,
        )
        cash = self.initial_balance
        peak_equity = self.initial_balance
        positions: dict[tuple[str, str], _Position] = {}
        marks: dict[tuple[str, str], float] = {}
        fee_rate = self.fee_bps / 10_000
        slippage_rate = self.slippage_bps / 10_000

        dataset = lance.dataset(dataset_path)
        for arrow_batch in dataset.to_batches(batch_size=batch_size):
            frame = arrow_batch.to_pandas()
            for _, row in frame.iterrows():
                payload = _payload_from_row(row)
                try:
                    event = MarketEvent(
                        source=str(row.get("source", "archive")),
                        type=str(row.get("type", "trade")),
                        timestamp=int(row.get("timestamp", 0)),
                        payload=payload,
                    )
                except Exception:
                    continue

                source_key = event.source.strip().casefold()
                try:
                    event_symbol = normalize_symbol(payload.get("symbol"))
                    raw_mark_price = payload.get("price")
                    if raw_mark_price is None:
                        raise ValueError("missing mark price")
                    mark_price = float(raw_mark_price)
                    if isfinite(mark_price) and mark_price > 0:
                        marks[(source_key, event_symbol)] = mark_price
                except (TypeError, ValueError):
                    pass

                signal: TradeSignal | None = strategy.evaluate(event)
                if signal is not None:
                    cash = self._apply_signal(
                        signal,
                        source_key,
                        cash,
                        positions,
                        marks,
                        result,
                        fee_rate,
                        slippage_rate,
                    )

                equity = self._equity(
                    cash,
                    positions,
                    marks,
                    fee_rate,
                    slippage_rate,
                )
                peak_equity = max(peak_equity, equity)
                drawdown = (peak_equity - equity) / peak_equity
                result.max_drawdown = max(result.max_drawdown, drawdown)

        result.cash_balance = cash
        result.open_positions = len(positions)
        result.unrealized_pnl = sum(
            position.quantity
            * marks.get(key, position.entry_price)
            * (1 - slippage_rate)
            * (1 - fee_rate)
            - position.entry_cost
            for key, position in positions.items()
        )
        result.ending_equity = self._equity(
            cash,
            positions,
            marks,
            fee_rate,
            slippage_rate,
        )
        result.total_pnl = result.ending_equity - self.initial_balance
        return result

    def _apply_signal(
        self,
        signal: TradeSignal,
        source_key: str,
        cash: float,
        positions: dict[tuple[str, str], _Position],
        marks: dict[tuple[str, str], float],
        result: BacktestResult,
        fee_rate: float,
        slippage_rate: float,
    ) -> float:
        price = float(signal.price or 0)
        if not isfinite(price) or price <= 0:
            return cash

        result.signals.append(signal)
        if signal.order_type != "MARKET":
            result.unfilled_signals += 1
            return cash

        key = (source_key, signal.symbol)
        marks[key] = price

        if signal.action == "BUY":
            if key in positions:
                result.unfilled_signals += 1
                return cash

            allocation = signal.allocation_percentage / 100
            allocation = min(allocation, self.max_position_fraction)
            current_equity = self._equity(
                cash,
                positions,
                marks,
                fee_rate,
                slippage_rate,
            )
            cash_budget = min(cash, current_equity * allocation)
            if cash_budget <= 0:
                result.unfilled_signals += 1
                return cash

            execution_price = price * (1 + slippage_rate)
            quantity = cash_budget / (execution_price * (1 + fee_rate))
            entry_notional = quantity * execution_price
            entry_fee = entry_notional * fee_rate
            entry_cost = entry_notional + entry_fee
            positions[key] = _Position(
                quantity=quantity,
                entry_price=execution_price,
                entry_cost=entry_cost,
            )
            result.fees_paid += entry_fee
            result.turnover += entry_notional
            return cash - entry_cost

        position = positions.pop(key, None)
        if position is None:
            result.unfilled_signals += 1
            return cash

        execution_price = price * (1 - slippage_rate)
        exit_notional = position.quantity * execution_price
        exit_fee = exit_notional * fee_rate
        net_proceeds = exit_notional - exit_fee
        pnl = net_proceeds - position.entry_cost

        result.total_trades += 1
        result.realized_pnl += pnl
        result.fees_paid += exit_fee
        result.turnover += exit_notional
        result.trade_returns.append(pnl / position.entry_cost)
        if pnl > 0:
            result.wins += 1
        else:
            result.losses += 1
        return cash + net_proceeds

    @staticmethod
    def _equity(
        cash: float,
        positions: dict[tuple[str, str], _Position],
        marks: dict[tuple[str, str], float],
        fee_rate: float,
        slippage_rate: float,
    ) -> float:
        return cash + sum(
            position.quantity
            * marks.get(key, position.entry_price)
            * (1 - slippage_rate)
            * (1 - fee_rate)
            for key, position in positions.items()
        )
