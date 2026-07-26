"""
Position sizer for Mercury executor.

Strategies emit intent (side + confidence) with a placeholder quantity
(`INTENT_PLACEHOLDER_QTY` in core/schemas.py). This module converts that
intent into a real order quantity using:

  BUY: quote cash -> base quantity
  SELL: free base inventory -> base quantity

Both sides are capped by ``MAX_NOTIONAL_EUR`` and require an EUR-quoted symbol
until Mercury has a timestamped FX conversion service.

Fail-safe behaviour:
  - Vol modulator missing or stale -> skip (caller must NOT trade).
  - Balance missing or stale -> skip.
  - Computed notional below exchange min-notional -> skip.

Current sandbox scope:
  - Mark price is fetched inline.
  - Volatility state is required by default and fails closed when missing or stale.
  - Min-notional limits are loaded from CCXT markets and cached per process.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from json import loads
from math import isfinite
from typing import Any, Literal

from nats.js import JetStreamContext

from core.config import ConfigurationError, env_bool, env_positive_float
from core.schemas import TradeSignal
from core.symbols import normalize_symbol, split_symbol
from executor.exchange_client import get_exchange
from executor.risk_manager import KV_BUCKET

logger = logging.getLogger("sizing")


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if isfinite(number) else None


SIZING_FIXED_FRACTION = env_positive_float("SIZING_FIXED_FRACTION", 0.25)
if SIZING_FIXED_FRACTION > 1:
    raise ConfigurationError("SIZING_FIXED_FRACTION must be less than or equal to 1")
MAX_NOTIONAL_EUR = env_positive_float("MAX_NOTIONAL_EUR", 5.0)
SIZING_BALANCE_MAX_AGE_SECONDS = env_positive_float("SIZING_BALANCE_MAX_AGE_SECONDS", 60)
VOL_MODULATOR_MAX_AGE_SECONDS = env_positive_float("VOL_MODULATOR_MAX_AGE_SECONDS", 60)
SIZING_REQUIRE_VOL_MODULATOR = env_bool("SIZING_REQUIRE_VOL_MODULATOR", True)

VOL_MODULATOR_KEY = "vol_modulator"
AVAILABLE_BALANCE_KEY = "available_balance"

SkipReason = Literal[
    "ok",
    "vol_modulator_stale",
    "vol_modulator_missing",
    "balance_stale",
    "balance_missing",
    "balance_zero",
    "mark_price_unavailable",
    "min_notional_unavailable",
    "below_min_notional",
    "invalid_quote",
]


@dataclass(frozen=True)
class SizingDecision:
    sized_signal: TradeSignal | None
    reason: SkipReason
    target_notional_quote: float = 0.0
    mark_price: float = 0.0


_market_min_cost: dict[str, float] = {}


async def _load_min_cost(symbol: str) -> float | None:
    """Min-notional cost (in quote asset) for `symbol`, cached per process."""
    if symbol in _market_min_cost:
        return _market_min_cost[symbol]

    exchange = get_exchange()
    try:
        await exchange.load_markets()
        market = exchange.markets.get(symbol)
        if market is None:
            return None
        limits = market.get("limits") or {}
        cost = limits.get("cost") or {}
        min_cost = _as_float(cost.get("min"))
        if min_cost is None or min_cost <= 0:
            return None
        _market_min_cost[symbol] = min_cost
        return min_cost
    finally:
        await exchange.close()


async def _fetch_mark_price(symbol: str) -> float | None:
    exchange = get_exchange()
    try:
        ticker = await exchange.fetch_ticker(symbol)
        price = (
            _as_float(ticker.get("last"))
            or _as_float(ticker.get("close"))
            or _as_float(ticker.get("bid"))
            or _as_float(ticker.get("ask"))
        )
        if price is None or price <= 0:
            return None
        return price
    finally:
        await exchange.close()


def _is_stale(computed_at_iso: Any, max_age_seconds: float) -> bool:
    if not computed_at_iso:
        return True
    try:
        ts = datetime.fromisoformat(computed_at_iso)
    except (TypeError, ValueError):
        return True
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    age = (datetime.now(UTC) - ts).total_seconds()
    return age < -5 or age > max_age_seconds


async def _read_kv_json(js: JetStreamContext, key: str) -> dict[str, Any] | None:
    try:
        kv = await js.key_value(KV_BUCKET)
        entry = await kv.get(key)
        if entry.value is None:
            return None
        raw = loads(entry.value.decode())
        if not isinstance(raw, dict):
            return None
        return raw
    except Exception:
        return None


async def _read_vol_multiplier(js: JetStreamContext) -> tuple[float | None, SkipReason]:
    raw = await _read_kv_json(js, VOL_MODULATOR_KEY)
    if raw is None:
        if SIZING_REQUIRE_VOL_MODULATOR:
            return None, "vol_modulator_missing"
        return 1.0, "ok"

    computed_at = raw.get("computed_at")
    if _is_stale(computed_at, VOL_MODULATOR_MAX_AGE_SECONDS):
        return None, "vol_modulator_stale"

    multiplier = _as_float(raw.get("multiplier"))
    if multiplier is None:
        return None, "vol_modulator_stale"
    return max(0.0, min(1.0, multiplier)), "ok"


async def _read_free_balance(
    js: JetStreamContext,
    asset: str,
    bucket_name: Literal["quote_free", "asset_free"],
) -> tuple[float | None, SkipReason]:
    raw = await _read_kv_json(js, AVAILABLE_BALANCE_KEY)
    if raw is None:
        return None, "balance_missing"

    if _is_stale(raw.get("computed_at"), SIZING_BALANCE_MAX_AGE_SECONDS):
        return None, "balance_stale"

    free_by_asset = raw.get(bucket_name)
    if not isinstance(free_by_asset, dict):
        return None, "balance_missing"

    free = _as_float(free_by_asset.get(asset))
    if free is None:
        return None, "balance_missing"
    if free <= 0:
        return None, "balance_zero"
    return free, "ok"


async def size_signal(signal: TradeSignal, js: JetStreamContext) -> SizingDecision:
    """
    Resize a strategy intent signal into a real order quantity.

    Returns a SizingDecision; if sized_signal is None the caller must NOT trade
    (the reason tells why). Otherwise sized_signal is a new TradeSignal with
    `quantity` set to the computed amount in base asset.
    """
    symbol = normalize_symbol(signal.symbol)
    base, quote = split_symbol(symbol)
    if not base or quote != "EUR":
        logger.warning(
            "Sizer: rejecting %s (only EUR-quoted sandbox symbols are supported)",
            symbol,
        )
        return SizingDecision(None, "invalid_quote")

    multiplier, vol_reason = await _read_vol_multiplier(js)
    if multiplier is None:
        logger.warning("Sizer: rejecting %s (%s)", symbol, vol_reason)
        return SizingDecision(None, vol_reason)

    balance_asset = quote if signal.action == "BUY" else base
    balance_bucket: Literal["quote_free", "asset_free"] = (
        "quote_free" if signal.action == "BUY" else "asset_free"
    )
    balance, balance_reason = await _read_free_balance(
        js,
        balance_asset,
        balance_bucket,
    )
    if balance is None:
        logger.warning(
            "Sizer: rejecting %s (%s, asset=%s)",
            symbol,
            balance_reason,
            balance_asset,
        )
        return SizingDecision(None, balance_reason)

    mark_price = await _fetch_mark_price(symbol)
    if mark_price is None:
        logger.warning("Sizer: rejecting %s (mark_price_unavailable)", symbol)
        return SizingDecision(None, "mark_price_unavailable")

    if signal.action == "BUY":
        target_quote = balance * SIZING_FIXED_FRACTION * signal.confidence_score * multiplier
    else:
        target_base = balance * SIZING_FIXED_FRACTION * signal.confidence_score * multiplier
        target_quote = target_base * mark_price
    target_quote = min(target_quote, MAX_NOTIONAL_EUR)

    if target_quote <= 0:
        return SizingDecision(None, "below_min_notional", target_quote, mark_price)

    min_cost = await _load_min_cost(symbol)
    if min_cost is None:
        # Fail-closed: an unavailable min-notional means we cannot verify the floor.
        logger.warning("Sizer: %s rejected — exchange min-notional unavailable", symbol)
        return SizingDecision(None, "min_notional_unavailable", target_quote, mark_price)
    if target_quote < min_cost:
        logger.warning(
            "Sizer: %s target=%.4f %s below exchange min=%.4f",
            symbol,
            target_quote,
            quote,
            min_cost,
        )
        return SizingDecision(None, "below_min_notional", target_quote, mark_price)

    qty = target_quote / mark_price
    # Fix: preserve the mark price used for sizing so downstream exposure tracking
    # uses the same price we computed quantity against (not a stale strategy price).
    sized = signal.model_copy(update={"quantity": qty, "price": mark_price})
    logger.info(
        "Sized %s %s: conf=%.2f mult=%.2f balance=%.2f%s -> qty=%.8f @ %.2f (notional=%.2f%s)",
        signal.action,
        symbol,
        signal.confidence_score,
        multiplier,
        balance,
        balance_asset,
        qty,
        mark_price,
        target_quote,
        quote,
    )
    return SizingDecision(sized, "ok", target_quote, mark_price)
