from pydantic import BaseModel
from typing import Optional, Any, Dict
from datetime import datetime

class MarketEvent(BaseModel):
    source: str
    type: str
    timestamp: int
    payload: Dict[str, Any]

class TradeSignal(BaseModel):
    strategy_id: str
    symbol: str
    action: str  # "BUY" or "SELL"
    order_type: str  # "MARKET" or "LIMIT"
    quantity: float
    price: Optional[float] = None
    confidence_score: float = 1.0
    allocation_percentage: float = 0.0
    timestamp: datetime
