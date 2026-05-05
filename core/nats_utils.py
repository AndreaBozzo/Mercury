"""
NATS / JetStream helpers for Mercury.

Changes from v0.1:
  - get_nats_connection() is the single connection factory; get_jetstream()
    calls it instead of opening a second connection (was a latent resource leak).
  - publish_arrow() stamps Content-Type so consumers can detect binary frames
    without a separate schema registry.
  - publish_json() is a thin convenience wrapper kept for Nephtys-facing subjects
    that still expect plain JSON (until Nephtys adds Arrow IPC support).
"""

from __future__ import annotations

import json
import os

import nats
from dotenv import load_dotenv
from nats.aio.client import Client as NatsClient
from nats.js import JetStreamContext

load_dotenv()

_NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")


async def get_nats_connection() -> NatsClient:
    return await nats.connect(servers=[_NATS_URL])


async def get_jetstream() -> JetStreamContext:
    nc = await get_nats_connection()
    return nc.jetstream()


async def publish_arrow(js: JetStreamContext, subject: str, arrow_bytes: bytes) -> None:
    """Publish Arrow IPC bytes on an internal Mercury subject."""
    hdrs = {"Content-Type": "application/vnd.apache.arrow.stream"}
    await js.publish(subject, arrow_bytes, headers=hdrs)


async def publish_json(js: JetStreamContext, subject: str, data: dict) -> None:
    """Publish JSON on a Nephtys-compatible subject."""
    await js.publish(subject, json.dumps(data).encode())
