from core.schemas import TradeSignal
import logging

logger = logging.getLogger("risk_manager")

VIRTUAL_BALANCE = 10000.0  # Max exposure for safety
CURRENT_EXPOSURE = 0.0

def check_risk(signal: TradeSignal) -> bool:
    """
    Evaluates whether a trade signal passes strict risk parameters.
    """
    global CURRENT_EXPOSURE
    
    estimated_cost = signal.quantity * (signal.price if signal.price else 50000.0)
    
    if signal.action == "BUY" and (CURRENT_EXPOSURE + estimated_cost > VIRTUAL_BALANCE):
        logger.warning("Risk check failed: Insufficient virtual balance.")
        return False
        
    if signal.action == "BUY":
        CURRENT_EXPOSURE += estimated_cost
        
    return True
