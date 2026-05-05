import logging
from json import dumps, loads

from nats.js import JetStreamContext

from core.schemas import TradeSignal

logger = logging.getLogger("risk_manager")

VIRTUAL_BALANCE = 10000.0  # Max exposure for safety
KV_BUCKET = "mercury_wallet_state"
EXPOSURE_KEY = "current_exposure"
HALT_KEY = "HALT"
POSITION_STATE_PREFIX = "position_state:"


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

    estimated_cost = signal.quantity * (signal.price if signal.price else 50000.0)

    if signal.action == "BUY" and (current_exposure + estimated_cost > VIRTUAL_BALANCE):
        logger.warning(
            "Risk check failed: Insufficient virtual balance. Exposure: %.2f",
            current_exposure,
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
