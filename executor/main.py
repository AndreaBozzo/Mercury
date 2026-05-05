import asyncio
import json
import logging

from core.nats_utils import get_jetstream
from core.schemas import TradeSignal
from executor.exchange_client import execute_trade
from executor.risk_manager import check_risk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("executor")


async def get_message_handler(js):
    async def message_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            signal = TradeSignal(**data)
            logger.info(
                "Received trade signal (Confidence: %.2f): %s %s",
                signal.confidence_score,
                signal.action,
                signal.symbol,
            )

            if await check_risk(signal, js):
                await execute_trade(signal)
                logger.info("Trade valid and processed by exchange client.")
                # Acknowledge only after successful execution + NATS KV update
                await msg.ack()
            else:
                logger.warning("Trade rejected by risk manager.")
                # Ack rejected signals to avoid infinite retry
                await msg.ack()

        except Exception as e:
            logger.error("Error processing message: %s", e)
            # Non-risk execution failures: let the message time out / NAK externally

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
    await js.subscribe("trade.signals", durable="executor_service", cb=handler)

    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
