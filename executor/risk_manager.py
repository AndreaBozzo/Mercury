import logging
import os
from collections.abc import Mapping
from json import dumps, loads
from typing import Any

from nats.js import JetStreamContext

from core.schemas import TradeSignal

logger = logging.getLogger("risk_manager")

VIRTUAL_BALANCE = 10000.0  # Max exposure for safety
DEFAULT_MAX_EXPOSURE_DOLLARS = float(os.getenv("MAX_EXPOSURE_DOLLARS", str(VIRTUAL_BALANCE)))
DEFAULT_MAX_TRADE_NOTIONAL_DOLLARS = float(os.getenv("MAX_TRADE_NOTIONAL_DOLLARS", "1000"))
KV_BUCKET = "mercury_wallet_state"
EXPOSURE_KEY = "current_exposure"
HALT_KEY = "HALT"
POSITION_STATE_PREFIX = "position_state:"
RISK_LIMITS_KEY = "risk_limits"
AVAILABLE_BALANCE_KEY = "available_balance"


async def load_exposure(js: JetStreamContext) -> float:
    try:
        kv = await js.key_value(KV_BUCKET)
        entry = await kv.get(EXPOSURE_KEY)
        if entry.value is None:
            return 0.0
        return float(entry.value.decode())
    except Exception:
        return 0.0


async def save_exposure(js: JetStreamContext, new_exposure: float):
    kv = await js.key_value(KV_BUCKET)
    await kv.put(EXPOSURE_KEY, str(new_exposure).encode())


def default_risk_limits() -> dict[str, float | int | str]:
    return {
        "max_exposure_dollars": DEFAULT_MAX_EXPOSURE_DOLLARS,
        "max_trade_notional_dollars": DEFAULT_MAX_TRADE_NOTIONAL_DOLLARS,
        "sentiment_score": 0.0,
        "source": "default",
        "rationale": "Default static limits.",
    }


async def load_risk_limits(js: JetStreamContext) -> dict[str, float | int | str]:
    try:
        kv = await js.key_value(KV_BUCKET)
        entry = await kv.get(RISK_LIMITS_KEY)
        if entry.value is None:
            return default_risk_limits()

        raw = loads(entry.value.decode())
        if not isinstance(raw, dict):
            return default_risk_limits()
        return _coerce_risk_limits(raw)
    except Exception:
        return default_risk_limits()


async def save_risk_limits(
    js: JetStreamContext,
    limits: Mapping[str, float | int | str | None],
) -> None:
    kv = await js.key_value(KV_BUCKET)
    await kv.put(RISK_LIMITS_KEY, dumps(_coerce_risk_limits(limits), sort_keys=True).encode())


async def load_position_states(
    js: JetStreamContext,
    symbols: list[str],
) -> dict[str, dict[str, float | int | str | None]]:
    kv = await js.key_value(KV_BUCKET)
    states: dict[str, dict[str, float | int | str | None]] = {}

    for symbol in symbols:
        try:
            entry = await kv.get(_position_state_key(symbol))
        except Exception:
            continue

        if entry.value is None:
            continue

        try:
            raw = loads(entry.value.decode())
        except Exception:
            logger.warning("Failed to decode persisted position state for %s", symbol)
            continue

        if isinstance(raw, dict):
            states[symbol] = raw

    return states


async def save_position_states(
    js: JetStreamContext,
    states: dict[str, dict[str, float | int | str | None]],
) -> None:
    kv = await js.key_value(KV_BUCKET)

    for symbol, state in states.items():
        await kv.put(_position_state_key(symbol), dumps(state, sort_keys=True).encode())


async def save_available_balance(
    js: JetStreamContext,
    quote_free: Mapping[str, float],
    computed_at_iso: str,
) -> None:
    kv = await js.key_value(KV_BUCKET)
    payload = {
        "quote_free": {asset: float(value) for asset, value in quote_free.items()},
        "computed_at": computed_at_iso,
    }
    await kv.put(AVAILABLE_BALANCE_KEY, dumps(payload, sort_keys=True).encode())


async def load_available_balance(
    js: JetStreamContext,
) -> dict[str, Any] | None:
    try:
        kv = await js.key_value(KV_BUCKET)
        entry = await kv.get(AVAILABLE_BALANCE_KEY)
        if entry.value is None:
            return None
        raw = loads(entry.value.decode())
        if not isinstance(raw, dict):
            return None
        return raw
    except Exception:
        return None


async def load_halt_flag(js: JetStreamContext) -> bool:
    try:
        kv = await js.key_value(KV_BUCKET)
        entry = await kv.get(HALT_KEY)
        if entry.value is None:
            return False
        return entry.value.decode().strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


async def check_risk(signal: TradeSignal, js: JetStreamContext) -> bool:
    """
    Evaluates whether a trade signal passes strict risk parameters.
    """
    if await load_halt_flag(js):
        logger.warning("Risk check failed: HALT flag is set.")
        return False

    current_exposure = await load_exposure(js)
    risk_limits = await load_risk_limits(js)

    estimated_cost = signal.quantity * (signal.price if signal.price else 50000.0)
    max_trade_notional = float(risk_limits["max_trade_notional_dollars"])
    max_exposure = float(risk_limits["max_exposure_dollars"])

    if signal.action == "BUY" and estimated_cost > max_trade_notional:
        logger.warning(
            "Risk check failed: Trade notional %.2f exceeds max trade notional %.2f.",
            estimated_cost,
            max_trade_notional,
        )
        return False

    if signal.action == "BUY" and (current_exposure + estimated_cost > max_exposure):
        logger.warning(
            "Risk check failed: Exposure %.2f would exceed max exposure %.2f.",
            current_exposure,
            max_exposure,
        )
        return False

    return True


async def record_trade_execution(signal: TradeSignal, js: JetStreamContext) -> float:
    current_exposure = await load_exposure(js)
    estimated_cost = signal.quantity * (signal.price if signal.price else 50000.0)

    if signal.action == "BUY":
        new_exposure = current_exposure + estimated_cost
    else:
        new_exposure = max(0.0, current_exposure - estimated_cost)

    await save_exposure(js, new_exposure)
    logger.info("Updated exposure to %.2f after executed %s trade.", new_exposure, signal.action)
    return new_exposure


def _position_state_key(symbol: str) -> str:
    return f"{POSITION_STATE_PREFIX}{symbol.replace('/', '_')}"


def _coerce_risk_limits(
    raw: Mapping[str, float | int | str | None],
) -> dict[str, float | int | str]:
    defaults = default_risk_limits()

    max_exposure = _positive_float(
        raw.get("max_exposure_dollars"),
        fallback=float(defaults["max_exposure_dollars"]),
    )
    max_trade_notional = _positive_float(
        raw.get("max_trade_notional_dollars"),
        fallback=float(defaults["max_trade_notional_dollars"]),
    )
    sentiment_score = _bounded_float(
        raw.get("sentiment_score"),
        fallback=0.0,
        minimum=-1.0,
        maximum=1.0,
    )
    source = str(raw.get("source") or defaults["source"])
    rationale = str(raw.get("rationale") or defaults["rationale"])

    return {
        "max_exposure_dollars": max_exposure,
        "max_trade_notional_dollars": max_trade_notional,
        "sentiment_score": sentiment_score,
        "source": source,
        "rationale": rationale,
    }


def _positive_float(value, fallback: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return fallback
    if parsed <= 0:
        return fallback
    return parsed


def _bounded_float(value, fallback: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return fallback
    return max(minimum, min(maximum, parsed))
