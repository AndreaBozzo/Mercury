import asyncio
import json
import logging
from core.nats_utils import get_jetstream
from core.schemas import TradeSignal
from executor.risk_manager import check_risk
from executor.exchange_client import execute_trade

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("executor")

async def message_handler(msg):
    try:
        data = json.loads(msg.data.decode())
        signal = TradeSignal(**data)
        logger.info(f"Received trade signal: {signal}")
        
        if check_risk(signal):
            execute_trade(signal)
            logger.info("Trade valid and processed by exchange client.")
        else:
            logger.warning("Trade rejected by risk manager.")
            
        await msg.ack()
    except Exception as e:
        logger.error(f"Error processing message: {e}")

async def main():
    js = await get_jetstream()
    logger.info("Executor service started. Listening for trade signals...")
    
    # In a real scenario, subscribe to brain signals here
    # await js.add_stream(name="trade_signals", subjects=["trade.*"])
    # sub = await js.subscribe("trade.signals", cb=message_handler)

    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
