import asyncio
import json
import logging
from core.nats_utils import get_jetstream
from core.schemas import TradeSignal
from executor.risk_manager import check_risk
from executor.exchange_client import execute_trade

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("executor")

async def get_message_handler(js):
    async def message_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            signal = TradeSignal(**data)
            logger.info(f"Received trade signal (Confidence: {signal.confidence_score:.2f}): {signal.action} {signal.symbol}")
            
            # evaluate risk
            if await check_risk(signal, js):
                # 4.2 Execute and Acknowledge: Execute on exchange first
                await execute_trade(signal)
                logger.info("Trade valid and processed by exchange client.")
                
                # Acknowledge ONLY after successful execution AND NATS KV update (handled inside check_risk)
                await msg.ack()
            else:
                logger.warning("Trade rejected by risk manager.")
                # We ack rejected messages so we don't infinitely retry trades we intentionally deny
                await msg.ack()
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # If exchange execution fails (for non-risk reasons), we might not want to ack to retry. 
            # Or we ack to avoid poison pills. It depends on system design. For now, letting it time out or NAK could be an option.
    return message_handler

async def main():
    js = await get_jetstream()
    logger.info("Executor service started. Listening for trade signals...")
    
    try:
        await js.key_value("mercury_wallet_state")
    except Exception:
        await js.create_key_value(bucket="mercury_wallet_state")
        
    try:
        await js.add_stream(name="trade_signals", subjects=["trade.*"])
    except Exception:
        pass
        
    handler = await get_message_handler(js)
    # 2.1 Durable Subscription already implemented here
    sub = await js.subscribe("trade.signals", durable="executor_service", cb=handler)

    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
