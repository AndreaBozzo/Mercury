from pydantic import BaseModel, ConfigDict
from typing import Optional, Any, Dict
from datetime import datetime

class StreamEvent(BaseModel):
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
    timestamp: datetime
