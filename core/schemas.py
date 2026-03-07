from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class TradeSignal(BaseModel):
    strategy_id: str
    symbol: str
    action: str  # "BUY" or "SELL"
    order_type: str  # "MARKET" or "LIMIT"
    quantity: float
    price: Optional[float] = None
    timestamp: datetime
