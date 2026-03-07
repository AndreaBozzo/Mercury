from core.schemas import TradeSignal, MarketEvent
from datetime import datetime
from collections import deque
import statistics

# Store the last N prices
PRICE_HISTORY = deque(maxlen=20)

def evaluate_moving_average(event: MarketEvent) -> TradeSignal | None:
    """
    Evaluates market data stream event using a simple moving average.
    """
    payload = event.payload
    symbol = payload.get("symbol", "BTC/USDT")
    price = payload.get("price", 0.0)
    
    if price <= 0:
        return None
        
    PRICE_HISTORY.append(price)
    
    # We need at least a few data points to calculate a meaningful average
    if len(PRICE_HISTORY) < PRICE_HISTORY.maxlen:
        return None
        
    moving_average = statistics.mean(PRICE_HISTORY)
    
    # Simple logic: If current price drops 1% below the 20-tick moving average, BUY.
    # If it goes 1% above, SELL.
    action = None
    if price < moving_average * 0.99:
        action = "BUY"
    elif price > moving_average * 1.01:
        action = "SELL"
        
    if action:
        # Calculate a basic confidence score based on how far it deviated
        deviation = abs(price - moving_average) / moving_average
        confidence = min(1.0, deviation * 10)  # scale deviation securely
        
        # Decide allocation percentage depending on confidence (capped at 10% of virtual wallet)
        allocation = 0.1 * confidence * 100 
        
        return TradeSignal(
            strategy_id="moving_average_v2",
            symbol=symbol,
            action=action,
            order_type="MARKET",
            quantity=0.01,  # In a real bot, we'd base quantity on allocation % and available balance
            price=price,
            confidence_score=confidence,
            allocation_percentage=allocation,
            timestamp=datetime.now()
        )
    return None
