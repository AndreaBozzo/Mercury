"""
NATS / JetStream helpers for Mercury.

Connections are returned as an owned session so every service has an explicit
shutdown path. Stream setup distinguishes "missing" from broker/permission
failures; services must not silently continue after an arbitrary NATS error.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from dotenv import load_dotenv
from nats.aio.client import Client as NatsClient
from nats.js import JetStreamContext
from nats.js.errors import NotFoundError

import nats

load_dotenv()

_NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")


@dataclass(slots=True)
class NatsSession:
    """A JetStream context and the client connection that owns it."""

    client: NatsClient
    js: JetStreamContext

    async def close(self) -> None:
        if self.client.is_closed:
            return
        try:
            await self.client.drain()
        except Exception:
            # Shutdown still needs to release sockets if a broker disappeared
            # before subscriptions could be drained.
            await self.client.close()


async def connect_jetstream() -> NatsSession:
    client = await nats.connect(servers=[_NATS_URL], drain_timeout=5)
    return NatsSession(client=client, js=client.jetstream())


async def ensure_stream(
    js: JetStreamContext,
    *,
    name: str,
    subjects: list[str],
) -> None:
    """Create a missing stream and reject incompatible existing definitions."""
    try:
        info = await js.stream_info(name)
    except NotFoundError:
        await js.add_stream(name=name, subjects=subjects)
        return

    configured_subjects = set(info.config.subjects or [])
    if configured_subjects != set(subjects):
        raise RuntimeError(
            f"NATS stream {name!r} subjects are {sorted(configured_subjects)!r}; "
            f"expected {sorted(subjects)!r}"
        )


async def publish_arrow(js: JetStreamContext, subject: str, arrow_bytes: bytes) -> None:
    """Publish Arrow IPC bytes on an internal Mercury subject."""
    hdrs = {"Content-Type": "application/vnd.apache.arrow.stream"}
    await js.publish(subject, arrow_bytes, headers=hdrs)


async def publish_json(js: JetStreamContext, subject: str, data: dict) -> None:
    """Publish JSON on an upstream-compatible subject."""
    await js.publish(subject, json.dumps(data).encode())
