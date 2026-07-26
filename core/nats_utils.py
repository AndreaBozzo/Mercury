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

import nats
from dotenv import load_dotenv
from nats.aio.client import Client as NatsClient
from nats.js import JetStreamContext
from nats.js.api import AckPolicy
from nats.js.errors import NotFoundError

load_dotenv()

_NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
MARKET_STREAM_NAME = os.getenv("MERCURY_MARKET_STREAM_NAME", "NEPHTYS")
MARKET_STREAM_SUBJECT = os.getenv("MERCURY_MARKET_STREAM_SUBJECT", "nephtys.stream.>")


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


async def validate_push_consumer(
    js: JetStreamContext,
    *,
    stream_name: str,
    durable_name: str,
    filter_subject: str,
    max_deliver: int,
) -> None:
    """Reject correctness-relevant drift in an existing durable push consumer."""
    try:
        info = await js.consumer_info(stream_name, durable_name)
    except NotFoundError:
        return

    config = info.config
    drift: list[str] = []
    if config.ack_policy != AckPolicy.EXPLICIT:
        drift.append(f"ack_policy={config.ack_policy!r} (expected explicit)")
    if config.max_deliver != max_deliver:
        drift.append(f"max_deliver={config.max_deliver!r} (expected {max_deliver!r})")
    if config.filter_subject != filter_subject:
        drift.append(f"filter_subject={config.filter_subject!r} (expected {filter_subject!r})")
    if config.filter_subjects:
        drift.append(f"filter_subjects={config.filter_subjects!r} (expected none)")
    if config.deliver_subject is None:
        drift.append("delivery mode is pull (expected push)")
    if config.deliver_group:
        drift.append(f"deliver_group={config.deliver_group!r} (expected none)")
    if drift:
        details = "; ".join(drift)
        raise RuntimeError(
            f"NATS durable consumer {durable_name!r} on stream {stream_name!r} has "
            f"incompatible persisted configuration: {details}. Migrate or recreate "
            "the durable deliberately before restarting the service."
        )


async def publish_arrow(js: JetStreamContext, subject: str, arrow_bytes: bytes) -> None:
    """Publish Arrow IPC bytes on an internal Mercury subject."""
    hdrs = {"Content-Type": "application/vnd.apache.arrow.stream"}
    await js.publish(subject, arrow_bytes, headers=hdrs)


async def publish_json(js: JetStreamContext, subject: str, data: dict) -> None:
    """Publish JSON on an upstream-compatible subject."""
    await js.publish(subject, json.dumps(data).encode())
