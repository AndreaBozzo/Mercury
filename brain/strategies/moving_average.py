from core.schemas import TradeSignal
from datetime import datetime

def evaluate_moving_average(market_data: dict) -> TradeSignal | None:
    """
    Evaluates market data and returns a TradeSignal if conditions are met.
    """
    symbol = market_data.get("symbol", "BTC/USDT")
    price = market_data.get("price", 0.0)
    
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
