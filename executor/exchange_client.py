import asyncio
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

import ccxt.async_support as ccxt
from dotenv import load_dotenv

from core.config import env_bool
from core.schemas import TradeSignal
from core.symbols import normalize_symbol, split_symbol

load_dotenv()
logger = logging.getLogger("exchange_client")
ORDER_CONFIRM_ATTEMPTS = int(os.getenv("ORDER_CONFIRM_ATTEMPTS", "5"))
ORDER_CONFIRM_DELAY_SECONDS = float(os.getenv("ORDER_CONFIRM_DELAY_SECONDS", "1.0"))
TRADE_HISTORY_PAGE_LIMIT = int(os.getenv("TRADE_HISTORY_PAGE_LIMIT", "200"))
TRADE_HISTORY_MAX_PAGES = int(os.getenv("TRADE_HISTORY_MAX_PAGES", "25"))
LIVE_TRADING_CONFIRMATION = "I_UNDERSTAND_REAL_FUNDS_ARE_AT_RISK"

ExecutionMode = Literal["observe", "sandbox", "live"]


class ExchangeConfigurationError(RuntimeError):
    """Raised when execution settings do not satisfy Mercury's safety gates."""


@dataclass(frozen=True)
class ExecutionResult:
    status: Literal["observed", "filled"]
    order_id: str | None = None
    order: dict[str, Any] | None = None


@dataclass(frozen=True)
class SymbolPortfolioState:
    symbol: str
    quantity: float
    mark_price: float
    cost_basis: float
    market_value: float
    unrealized_pnl: float
    last_trade_timestamp: int | None = None


@dataclass(frozen=True)
class PortfolioSnapshot:
    symbols: dict[str, SymbolPortfolioState]
    current_exposure: float
    position_pnl: float
    quote_free: dict[str, float]  # cash balance per quote asset (e.g. {"EUR": 9.0, "USDT": 0.0})


@dataclass(frozen=True)
class PersistedPositionState:
    symbol: str
    quantity: float
    cost_basis: float
    last_trade_timestamp: int | None = None


def _env_flag(name: str, default: bool) -> bool:
    try:
        return env_bool(name, default)
    except ValueError as exc:
        raise ExchangeConfigurationError(str(exc)) from exc


def get_execution_mode() -> ExecutionMode:
    configured = os.getenv("EXECUTION_MODE")
    if configured is not None:
        mode = configured.strip().lower()
        if mode not in {"observe", "sandbox", "live"}:
            raise ExchangeConfigurationError(
                "EXECUTION_MODE must be one of: observe, sandbox, live"
            )
        return cast(ExecutionMode, mode)

    # Backward-compatible migration for private pre-alpha deployments.
    if _env_flag("DRY_RUN", default=True):
        return "observe"
    return "sandbox" if _env_flag("BINANCE_SANDBOX", default=True) else "live"


def is_observe_mode_enabled() -> bool:
    return get_execution_mode() == "observe"


def is_dry_run_enabled() -> bool:
    """Deprecated compatibility alias; use is_observe_mode_enabled()."""
    return is_observe_mode_enabled()


def get_reconcile_symbols() -> list[str]:
    raw = os.getenv("EXECUTOR_RECONCILE_SYMBOLS", "BTC/EUR")
    return [normalize_symbol(symbol.strip()) for symbol in raw.split(",") if symbol.strip()]


def get_exchange() -> ccxt.Exchange:
    mode = get_execution_mode()
    if mode == "observe":
        raise ExchangeConfigurationError("Observation mode does not create an exchange client")

    exchange_id = os.getenv("EXECUTION_EXCHANGE_ID", "").strip().lower()
    if not exchange_id and (os.getenv("BINANCE_API_KEY") or os.getenv("BINANCE_SECRET_KEY")):
        exchange_id = "binance"
        logger.warning(
            "Using deprecated BINANCE_* credential variables; "
            "migrate to generic EXCHANGE_* settings."
        )
    if not exchange_id:
        raise ExchangeConfigurationError(
            "EXECUTION_EXCHANGE_ID is required for sandbox or live execution"
        )

    if mode == "live" and os.getenv("LIVE_TRADING_CONFIRMATION") != LIVE_TRADING_CONFIRMATION:
        raise ExchangeConfigurationError(
            f"Live execution requires LIVE_TRADING_CONFIRMATION={LIVE_TRADING_CONFIRMATION}"
        )

    api_key = os.getenv("EXCHANGE_API_KEY")
    secret_key = os.getenv("EXCHANGE_SECRET")
    if exchange_id == "binance":
        api_key = api_key or os.getenv("BINANCE_API_KEY")
        secret_key = secret_key or os.getenv("BINANCE_SECRET_KEY")
    if not api_key or not secret_key:
        raise ExchangeConfigurationError(
            "EXCHANGE_API_KEY and EXCHANGE_SECRET are required for sandbox or live execution"
        )

    exchange_type = getattr(ccxt, exchange_id, None)
    if exchange_id not in ccxt.exchanges or exchange_type is None or not callable(exchange_type):
        raise ExchangeConfigurationError(f"Unsupported CCXT exchange id: {exchange_id!r}")

    config: dict[str, Any] = {
        "apiKey": api_key,
        "secret": secret_key,
        "enableRateLimit": True,
    }
    password = os.getenv("EXCHANGE_PASSWORD")
    if password:
        config["password"] = password

    exchange = exchange_type(config)
    if mode == "sandbox":
        try:
            exchange.set_sandbox_mode(True)
        except Exception as exc:
            raise ExchangeConfigurationError(
                f"Exchange {exchange_id!r} does not provide a usable CCXT sandbox"
            ) from exc
    else:
        logger.critical(
            "EXECUTION_MODE=live — executor is targeting %s with REAL FUNDS.",
            exchange_id,
        )
    return exchange


async def fetch_open_orders(symbol: str | None = None) -> list[dict[str, Any]]:
    exchange = get_exchange()

    try:
        return await exchange.fetch_open_orders(normalize_symbol(symbol) if symbol else None)
    finally:
        await exchange.close()


def _quote_assets_from_symbols(symbols: list[str]) -> list[str]:
    quotes: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        _base, quote = split_symbol(normalize_symbol(raw_symbol))
        if quote and quote not in seen:
            seen.add(quote)
            quotes.append(quote)
    return quotes


def _balance_free(balance: dict[str, Any], asset: str) -> float:
    free_bucket = balance.get("free")
    if isinstance(free_bucket, dict):
        value = _as_float(free_bucket.get(asset))
        if value is not None:
            return max(0.0, value)

    asset_bucket = balance.get(asset)
    if isinstance(asset_bucket, dict):
        value = _as_float(asset_bucket.get("free"))
        if value is not None:
            return max(0.0, value)

    return 0.0


async def fetch_portfolio_snapshot(
    symbols: list[str],
    persisted_positions: Mapping[str, Mapping[str, Any]] | None = None,
) -> PortfolioSnapshot:
    exchange = get_exchange()

    try:
        balance = await exchange.fetch_balance()
        symbol_states: dict[str, SymbolPortfolioState] = {}
        current_exposure = 0.0
        position_pnl = 0.0

        for raw_symbol in symbols:
            symbol = normalize_symbol(raw_symbol)
            state = await _fetch_symbol_portfolio_state(
                exchange,
                balance,
                symbol,
                _coerce_persisted_position_state(persisted_positions, symbol),
            )
            symbol_states[symbol] = state
            current_exposure += state.market_value
            position_pnl += state.unrealized_pnl

        quote_free = {
            asset: _balance_free(balance, asset) for asset in _quote_assets_from_symbols(symbols)
        }

        return PortfolioSnapshot(
            symbols=symbol_states,
            current_exposure=current_exposure,
            position_pnl=position_pnl,
            quote_free=quote_free,
        )
    finally:
        await exchange.close()


def _is_filled_order(order: dict[str, Any]) -> bool:
    status = str(order.get("status") or "").strip().lower()
    if status in {"closed", "filled"}:
        return True

    filled = _as_float(order.get("filled"))
    remaining = _as_float(order.get("remaining"))
    amount = _as_float(order.get("amount"))

    if filled is None or filled <= 0:
        return False
    if remaining is not None and remaining <= 0:
        return True
    if amount is not None and filled >= amount:
        return True
    return False


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


async def _confirm_order_fill(
    exchange: ccxt.Exchange, order: dict[str, Any], symbol: str
) -> dict[str, Any]:
    if _is_filled_order(order):
        return order

    order_id = order.get("id")
    if order_id is None:
        raise RuntimeError("Exchange order has no id; cannot confirm fill.")

    for attempt in range(ORDER_CONFIRM_ATTEMPTS):
        if attempt > 0:
            await asyncio.sleep(ORDER_CONFIRM_DELAY_SECONDS)

        confirmed = await exchange.fetch_order(str(order_id), symbol)
        if _is_filled_order(confirmed):
            return confirmed

    raise RuntimeError(f"Order {order_id} was not confirmed as filled on {exchange.id}.")


async def execute_trade(signal: TradeSignal) -> ExecutionResult:
    """
    Executes a trade on the exchange using CCXT.
    """
    if is_observe_mode_enabled():
        logger.info(
            "Observed %s %.6f %s; no exchange client or order was created.",
            signal.action,
            signal.quantity,
            signal.symbol,
        )
        return ExecutionResult(status="observed")

    exchange = get_exchange()
    mode = get_execution_mode()
    logger.info(
        "Executing %s %.6f %s on %s (%s)...",
        signal.action,
        signal.quantity,
        signal.symbol,
        exchange.id,
        mode.upper(),
    )

    try:
        if signal.action == "BUY":
            order = await exchange.create_market_buy_order(signal.symbol, signal.quantity)
        else:
            order = await exchange.create_market_sell_order(signal.symbol, signal.quantity)

        confirmed_order = await _confirm_order_fill(exchange, order, signal.symbol)
        order_id = (
            str(confirmed_order.get("id"))
            if confirmed_order.get("id") is not None
            else str(order.get("id"))
            if order.get("id") is not None
            else None
        )
        logger.info("Order filled on %s with id=%s", exchange.id, order_id)
        return ExecutionResult(status="filled", order_id=order_id, order=confirmed_order)
    except Exception:
        logger.exception("Failed to execute trade")
        raise
    finally:
        await exchange.close()


async def _fetch_symbol_portfolio_state(
    exchange: ccxt.Exchange,
    balance: dict[str, Any],
    symbol: str,
    persisted_state: PersistedPositionState | None = None,
) -> SymbolPortfolioState:
    normalized = normalize_symbol(symbol)
    base_asset, _quote_asset = split_symbol(normalized)
    quantity = _balance_total(balance, base_asset)
    ticker = await exchange.fetch_ticker(normalized)
    mark_price = _ticker_price(ticker)
    cost_basis, last_trade_timestamp = await _cost_basis_from_exchange_trades(
        exchange,
        normalized,
        quantity,
        persisted_state,
    )
    market_value = quantity * mark_price
    unrealized_pnl = market_value - cost_basis if quantity > 0 else 0.0

    return SymbolPortfolioState(
        symbol=normalized,
        quantity=quantity,
        mark_price=mark_price,
        cost_basis=cost_basis,
        market_value=market_value,
        unrealized_pnl=unrealized_pnl,
        last_trade_timestamp=last_trade_timestamp,
    )


def _balance_total(balance: dict[str, Any], asset: str) -> float:
    total_bucket = balance.get("total")
    if isinstance(total_bucket, dict):
        total = _as_float(total_bucket.get(asset))
        if total is not None:
            return max(0.0, total)

    asset_bucket = balance.get(asset)
    if isinstance(asset_bucket, dict):
        total = _as_float(asset_bucket.get("total"))
        if total is not None:
            return max(0.0, total)
        free = _as_float(asset_bucket.get("free")) or 0.0
        used = _as_float(asset_bucket.get("used")) or 0.0
        return max(0.0, free + used)

    return 0.0


def _ticker_price(ticker: dict[str, Any]) -> float:
    return (
        _as_float(ticker.get("last"))
        or _as_float(ticker.get("close"))
        or _as_float(ticker.get("bid"))
        or _as_float(ticker.get("ask"))
        or 0.0
    )


async def _cost_basis_from_exchange_trades(
    exchange: ccxt.Exchange,
    symbol: str,
    balance_qty: float,
    persisted_state: PersistedPositionState | None,
) -> tuple[float, int | None]:
    if balance_qty <= 0:
        return 0.0, persisted_state.last_trade_timestamp if persisted_state else None

    net_qty = persisted_state.quantity if persisted_state is not None else 0.0
    cost_basis = persisted_state.cost_basis if persisted_state is not None else 0.0
    trades = await _fetch_trade_history(
        exchange,
        symbol,
        since_timestamp=persisted_state.last_trade_timestamp if persisted_state else None,
        min_quantity=balance_qty,
        initial_quantity=net_qty,
    )
    net_qty, cost_basis = _apply_trades_to_position(
        trades,
        initial_quantity=net_qty,
        initial_cost_basis=cost_basis,
    )
    last_trade_timestamp = _latest_trade_timestamp(trades)
    if last_trade_timestamp is None and persisted_state is not None:
        last_trade_timestamp = persisted_state.last_trade_timestamp

    if balance_qty <= 0 or net_qty <= 0:
        return 0.0, last_trade_timestamp

    if abs(balance_qty - net_qty) <= 1e-9:
        return cost_basis, last_trade_timestamp

    return cost_basis * max(0.0, min(1.0, balance_qty / net_qty)), last_trade_timestamp


async def _fetch_trade_history(
    exchange: ccxt.Exchange,
    symbol: str,
    since_timestamp: int | None,
    min_quantity: float,
    initial_quantity: float,
) -> list[dict[str, Any]]:
    all_trades: list[dict[str, Any]] = []

    if since_timestamp is not None:
        trades = await exchange.fetch_my_trades(
            symbol,
            since=since_timestamp + 1,
            limit=TRADE_HISTORY_PAGE_LIMIT,
        )
        return _dedupe_sorted_trades(trades)

    cursor_since = 0
    reconstructed_quantity = initial_quantity

    for _page in range(TRADE_HISTORY_MAX_PAGES):
        trades = await exchange.fetch_my_trades(
            symbol,
            since=cursor_since,
            limit=TRADE_HISTORY_PAGE_LIMIT,
        )
        if not trades:
            break

        page = _dedupe_sorted_trades(trades)
        all_trades = _merge_trade_history(all_trades, page)
        reconstructed_quantity, _ = _apply_trades_to_position(all_trades)
        if reconstructed_quantity >= max(min_quantity - 1e-9, 0.0):
            break

        newest_timestamp = max(int(trade.get("timestamp") or 0) for trade in page)
        if newest_timestamp < cursor_since:
            break
        cursor_since = newest_timestamp + 1

    return all_trades


def _coerce_persisted_position_state(
    persisted_positions: Mapping[str, Mapping[str, Any]] | None,
    symbol: str,
) -> PersistedPositionState | None:
    if persisted_positions is None:
        return None

    raw = persisted_positions.get(symbol)
    if raw is None:
        return None

    quantity = _as_float(raw.get("quantity"))
    cost_basis = _as_float(raw.get("cost_basis"))
    if quantity is None or cost_basis is None:
        return None

    raw_timestamp = raw.get("last_trade_timestamp")
    timestamp = int(raw_timestamp) if raw_timestamp is not None else None
    return PersistedPositionState(
        symbol=symbol,
        quantity=max(0.0, quantity),
        cost_basis=max(0.0, cost_basis),
        last_trade_timestamp=timestamp,
    )


def _dedupe_sorted_trades(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    fallback_index = 0

    for trade in trades:
        trade_id = trade.get("id")
        if trade_id is None:
            trade_id = f"fallback-{fallback_index}"
            fallback_index += 1
        unique[str(trade_id)] = trade

    return sorted(unique.values(), key=lambda item: int(item.get("timestamp") or 0))


def _merge_trade_history(
    older: list[dict[str, Any]],
    newer: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for trade in older + newer:
        trade_id = trade.get("id")
        if trade_id is None:
            trade_id = f"fallback-{int(trade.get('timestamp') or 0)}-{len(merged)}"
        merged[str(trade_id)] = trade

    return sorted(merged.values(), key=lambda item: int(item.get("timestamp") or 0))


def _latest_trade_timestamp(trades: list[dict[str, Any]]) -> int | None:
    timestamps = [
        int(trade.get("timestamp") or 0) for trade in trades if trade.get("timestamp") is not None
    ]
    if not timestamps:
        return None
    return max(timestamps)


def build_persisted_position_state(
    symbol_state: SymbolPortfolioState,
) -> dict[str, Any]:
    return {
        "symbol": symbol_state.symbol,
        "quantity": symbol_state.quantity,
        "cost_basis": symbol_state.cost_basis,
        "last_trade_timestamp": symbol_state.last_trade_timestamp,
    }


def _cost_basis_from_trades(trades: list[dict[str, Any]], balance_qty: float) -> float:
    net_qty, cost_basis = _apply_trades_to_position(trades)

    if balance_qty <= 0 or net_qty <= 0:
        return 0.0

    if abs(balance_qty - net_qty) <= 1e-9:
        return cost_basis

    return cost_basis * max(0.0, min(1.0, balance_qty / net_qty))


def _apply_trades_to_position(
    trades: list[dict[str, Any]],
    initial_quantity: float = 0.0,
    initial_cost_basis: float = 0.0,
) -> tuple[float, float]:
    net_qty = max(0.0, initial_quantity)
    cost_basis = max(0.0, initial_cost_basis)

    for trade in sorted(trades, key=lambda item: int(item.get("timestamp") or 0)):
        side = str(trade.get("side") or "").strip().lower()
        amount = _as_float(trade.get("amount")) or 0.0
        trade_cost = _as_float(trade.get("cost"))
        price = _as_float(trade.get("price")) or 0.0

        if amount <= 0:
            continue

        cost = trade_cost if trade_cost is not None else amount * price
        if side == "buy":
            net_qty += amount
            cost_basis += cost
        elif side == "sell" and net_qty > 0:
            reduce_qty = min(amount, net_qty)
            avg_cost = cost_basis / net_qty if net_qty > 0 else 0.0
            cost_basis = max(0.0, cost_basis - reduce_qty * avg_cost)
            net_qty = max(0.0, net_qty - reduce_qty)

    return net_qty, cost_basis
