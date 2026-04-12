import lance
import pyarrow as pa
import pandas as pd
import asyncio
import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import nats
from nats.js import JetStreamContext

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("plutarch")

# Configuration
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
STREAM_SUBJECT = "nephtys.stream.crypto.>"
BATCH_SIZE = 10000
DATA_DIR = "data/history"

os.makedirs(DATA_DIR, exist_ok=True)

async def run_archiver():
    # Connect to NATS
    nc = await nats.connect(servers=[NATS_URL])
    js = nc.jetstream()
    
    logger.info(f"Plutarch connected to NATS at {NATS_URL}. Listening to {STREAM_SUBJECT}...")

    # We use a durable consumer so if Plutarch crashes, it picks up exactly where it left off
    sub = await js.subscribe(STREAM_SUBJECT, durable="plutarch_archiver")
    
    buffer = []
    
    try:
        while True:
            # Fetch one message at a time (you can also use fetch() for batching)
            msg = await sub.next_msg(timeout=None)
            payload = json.loads(msg.data.decode())
            
            # Add NATS metadata (like the exact subject it came from)
            payload['_subject'] = msg.subject
            payload['_archived_at'] = datetime.utcnow().isoformat()
            
            buffer.append(payload)
            
            # Acknowledge the message so NATS knows we have it securely in memory
            await msg.ack()

            # When the buffer is full, flush to disk
            if len(buffer) >= BATCH_SIZE:
                flush_buffer_to_lance(buffer)
                buffer.clear()

    except asyncio.CancelledError:
        logger.info("Plutarch shutting down. Flushing remaining buffer...")
        if buffer:
            flush_buffer_to_lance(buffer)
    finally:
        await nc.close()

def flush_buffer_to_lance(buffer):
    if not buffer:
        return
        
    df = pd.DataFrame(buffer)
    table = pa.Table.from_pandas(df)
    
    # If the dataset exists, it appends. If not, it creates it.
    lance.write_dataset(
        table, 
        "data/history/market_history.lance", 
        mode="append"
    )