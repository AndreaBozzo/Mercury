import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any, Literal

import ccxt.async_support as ccxt  # type: ignore[import-untyped]
from dotenv import load_dotenv

from core.schemas import TradeSignal

load_dotenv()
logger = logging.getLogger("exchange_client")
ORDER_CONFIRM_ATTEMPTS = int(os.getenv("ORDER_CONFIRM_ATTEMPTS", "5"))
ORDER_CONFIRM_DELAY_SECONDS = float(os.getenv("ORDER_CONFIRM_DELAY_SECONDS", "1.0"))


@dataclass(frozen=True)
class ExecutionResult:
    status: Literal["dry_run", "filled"]
    order_id: str | None = None
    order: dict[str, Any] | None = None


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


def is_dry_run_enabled() -> bool:
    return _env_flag("DRY_RUN", default=True)


def get_exchange():
    api_key = os.getenv("BINANCE_API_KEY")
    secret_key = os.getenv("BINANCE_SECRET_KEY")

    exchange = ccxt.binance(
        {
            "apiKey": api_key,
            "secret": secret_key,
            "enableRateLimit": True,
        }
    )

    # Always use testnet/sandbox mode initially
    exchange.set_sandbox_mode(True)
    return exchange


async def fetch_open_orders() -> list[dict[str, Any]]:
    exchange = get_exchange()

    try:
        return await exchange.fetch_open_orders()
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


async def _confirm_order_fill(exchange, order: dict[str, Any], symbol: str) -> dict[str, Any]:
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
    exchange = get_exchange()
    dry_run = is_dry_run_enabled()
    logger.info(
        "Executing %s %.6f %s on %s (%s)...",
        signal.action,
        signal.quantity,
        signal.symbol,
        exchange.id,
        "DRY_RUN" if dry_run else "SANDBOX",
    )

    try:
        if dry_run:
            return ExecutionResult(status="dry_run")

        if signal.action == "BUY":
            order = await exchange.create_market_buy_order(signal.symbol, signal.quantity)
        else:
            order = await exchange.create_market_sell_order(signal.symbol, signal.quantity)

        confirmed_order = await _confirm_order_fill(exchange, order, signal.symbol)
        order_id = (
            str(confirmed_order.get("id"))
            if confirmed_order.get("id") is not None
            else str(order.get("id")) if order.get("id") is not None else None
        )
        logger.info("Order filled on %s with id=%s", exchange.id, order_id)
        return ExecutionResult(status="filled", order_id=order_id, order=confirmed_order)
    except Exception:
        logger.exception("Failed to execute trade")
        raise
    finally:
        await exchange.close()
