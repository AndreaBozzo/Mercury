from core.schemas import TradeSignal, StreamEvent
from datetime import datetime

def evaluate_moving_average(event: StreamEvent) -> TradeSignal | None:
    """
    Evaluates market data stream event and returns a TradeSignal if conditions are met.
    """
    # Assuming the Nephtys payload contains symbol and price after our transformation
    payload = event.payload
    symbol = payload.get("symbol", "BTC/USDT")
    price = payload.get("price", 0.0)
    
    # In a real moving average strategy we would keep state of previous prices here
    
    if price > 50000:  # arbitrary benchmark for template
        return TradeSignal(
            strategy_id="moving_average_v1",
            symbol=symbol,
            action="SELL",
            order_type="MARKET",
            quantity=0.01,
            timestamp=datetime.now()
        )
    return None
