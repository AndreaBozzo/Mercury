from core.schemas import TradeSignal
import logging
from nats.js import JetStreamContext

logger = logging.getLogger("risk_manager")

VIRTUAL_BALANCE = 10000.0  # Max exposure for safety

async def load_exposure(js: JetStreamContext) -> float:
    try:
        kv = await js.key_value("mercury_wallet_state")
        entry = await kv.get("current_exposure")
        return float(entry.value.decode())
    except Exception:
        return 0.0

async def save_exposure(js: JetStreamContext, new_exposure: float):
    kv = await js.key_value("mercury_wallet_state")
    await kv.put("current_exposure", str(new_exposure).encode())

async def check_risk(signal: TradeSignal, js: JetStreamContext) -> bool:
    """
    Evaluates whether a trade signal passes strict risk parameters.
    """
    current_exposure = await load_exposure(js)
    
    estimated_cost = signal.quantity * (signal.price if signal.price else 50000.0)
    
    if signal.action == "BUY" and (current_exposure + estimated_cost > VIRTUAL_BALANCE):
        logger.warning(f"Risk check failed: Insufficient virtual balance. Exposure: {current_exposure}")
        return False
        
    if signal.action == "BUY":
        new_exposure = current_exposure + estimated_cost
        await save_exposure(js, new_exposure)
        logger.info(f"Updated exposure to {new_exposure}.")
        
    return True
