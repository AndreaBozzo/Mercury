import asyncio
import json
import logging
from core.nats_utils import get_jetstream
from core.schemas import StreamEvent
from brain.strategies.moving_average import evaluate_moving_average

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("brain")

async def main():
    js = await get_jetstream()
    logger.info("Brain service started. Ready to think.")
    
    try:
        await js.add_stream(name="market_data", subjects=["market.*"])
    except Exception:
        pass
        
    async def market_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            # Parse into StreamEvent schema expected from Nephtys
            event = StreamEvent(**data)
            
            signal = evaluate_moving_average(event)
            if signal:
                await js.publish("trade.signals", signal.model_dump_json().encode())
                logger.info(f"Published signal: {signal}")
                
            await msg.ack()
        except Exception as e:
            logger.error(f"Error processing market data: {e}")

    # Subscribe to market feed using a durable consumer (assuming Nephtys publishes to market.prices or similar)
    # The actual Nephtys subject depends on its stream config, but "market.*" is a good default
    sub = await js.subscribe("market.*", durable="brain_service", cb=market_handler)

    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
