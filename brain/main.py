import asyncio
import json
import logging
from core.nats_utils import get_jetstream
from brain.strategies.moving_average import evaluate_moving_average

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("brain")

async def main():
    js = await get_jetstream()
    logger.info("Brain service started. Ready to think.")
    
    # In a real scenario, subscribe to market data feed here (e.g. Nephtys)
    # sub = await js.subscribe("market.prices")
    # async for msg in sub.messages:
    #     data = json.loads(msg.data.decode())
    #     signal = evaluate_moving_average(data)
    #     if signal:
    #         await js.publish("trade.signals", signal.model_dump_json().encode())
    #         logger.info(f"Published signal: {signal}")

    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
