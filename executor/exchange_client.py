import os
import ccxt
import logging
from core.schemas import TradeSignal
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("exchange_client")

def get_exchange():
    api_key = os.getenv("BINANCE_API_KEY")
    secret_key = os.getenv("BINANCE_SECRET_KEY")
    
    exchange = ccxt.binance({
        'apiKey': api_key,
        'secret': secret_key,
        'enableRateLimit': True,
    })
    
    # Always use testnet/sandbox mode initially
    exchange.set_sandbox_mode(True)
    return exchange

def execute_trade(signal: TradeSignal):
    """
    Executes a trade on the exchange using CCXT.
    """
    exchange = get_exchange()
    logger.info(f"Executing {signal.action} {signal.quantity} {signal.symbol} on {exchange.id} (SANDBOX)...")
    
    # Example execution:
    # try:
    #     if signal.action == "BUY":
    #         order = exchange.create_market_buy_order(signal.symbol, signal.quantity)
    #     elif signal.action == "SELL":
    #         order = exchange.create_market_sell_order(signal.symbol, signal.quantity)
    #     logger.info(f"Order executed: {order}")
    # except Exception as e:
    #     logger.error(f"Failed to execute trade: {e}")
