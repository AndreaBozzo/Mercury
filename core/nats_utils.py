import os
import nats
from nats.js import JetStreamContext
from dotenv import load_dotenv

load_dotenv()

async def get_nats_connection():
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    nc = await nats.connect(servers=[nats_url])
    return nc

async def get_jetstream() -> JetStreamContext:
    nc = await nats.connect(servers=[os.getenv("NATS_URL", "nats://localhost:4222")])
    return nc.jetstream()
