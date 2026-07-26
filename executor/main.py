import asyncio
import contextlib
import logging
import os
from datetime import UTC, datetime

from nats.aio.client import Client as NatsClient
from nats.js.errors import BucketNotFoundError
from pydantic import ValidationError

from core.nats_utils import (
    connect_jetstream,
    ensure_stream,
    push_consumer_config,
    validate_push_consumer,
)
from core.schemas import TradeSignal
from executor.exchange_client import (
    build_persisted_position_state,
    ensure_execution_mode_supported,
    execute_trade,
    fetch_open_orders,
    fetch_portfolio_snapshot,
    get_reconcile_symbols,
    is_observe_mode_enabled,
)
from executor.observability import (
    health_http_status,
    health_payload,
    json_response,
    metrics_payload,
)
from executor.risk_manager import (
    check_risk,
    load_exposure,
    load_halt_flag,
    load_position_states,
    record_trade_execution,
    save_available_balance,
    save_exposure,
    save_position_states,
)
from executor.sizing import size_signal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("executor")
_trade_counts: dict[tuple[str, str], int] = {}
_current_exposure = 0.0
_position_pnl = 0.0
_nats_client: NatsClient | None = None
_EXECUTOR_HTTP_HOST = os.getenv("EXECUTOR_HTTP_HOST", "127.0.0.1")
_EXECUTOR_HTTP_PORT = int(os.getenv("EXECUTOR_HTTP_PORT", "9109"))
_EXECUTOR_ACCOUNT_REFRESH_SECONDS = float(os.getenv("EXECUTOR_ACCOUNT_REFRESH_SECONDS", "15"))
_CONSUMER_MAX_DELIVER = int(os.getenv("MERCURY_CONSUMER_MAX_DELIVER", "10"))
_RETRY_DELAY_SECONDS = float(os.getenv("MERCURY_CONSUMER_RETRY_DELAY_SECONDS", "5"))
_EXECUTOR_DURABLE_NAME = os.getenv("EXECUTOR_DURABLE_NAME", "executor_service")


def _record_trade_outcome(action: str, outcome: str) -> None:
    key = (action, outcome)
    _trade_counts[key] = _trade_counts.get(key, 0) + 1


def _set_current_exposure(value: float) -> None:
    global _current_exposure
    _current_exposure = value


def _set_position_pnl(value: float) -> None:
    global _position_pnl
    _position_pnl = value


def _is_nats_connected() -> bool:
    return bool(_nats_client and _nats_client.is_connected)


async def _refresh_current_exposure(js) -> None:
    _set_current_exposure(await load_exposure(js))


async def _refresh_runtime_state(js) -> None:
    if is_observe_mode_enabled():
        await _refresh_current_exposure(js)
        _set_position_pnl(0.0)
        return

    symbols = get_reconcile_symbols()
    persisted_positions = await load_position_states(js, symbols)
    snapshot = await fetch_portfolio_snapshot(symbols, persisted_positions=persisted_positions)
    await save_position_states(
        js,
        {
            symbol: build_persisted_position_state(state)
            for symbol, state in snapshot.symbols.items()
        },
    )
    await save_exposure(js, snapshot.current_exposure)
    await save_available_balance(
        js,
        snapshot.quote_free,
        snapshot.asset_free,
        datetime.now(UTC).isoformat(),
    )
    _set_current_exposure(snapshot.current_exposure)
    _set_position_pnl(snapshot.position_pnl)


async def _refresh_runtime_state_safe(js) -> None:
    try:
        await _refresh_runtime_state(js)
    except Exception:
        logger.exception("Failed to refresh executor portfolio state from exchange")


async def _portfolio_refresh_loop(js) -> None:
    while True:
        await asyncio.sleep(_EXECUTOR_ACCOUNT_REFRESH_SECONDS)
        await _refresh_runtime_state_safe(js)


async def _serve_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        request_line = await reader.readline()
        if not request_line:
            return

        parts = request_line.decode("ascii", errors="ignore").strip().split()
        if len(parts) < 2:
            await _write_response(writer, "400 Bad Request", "text/plain", b"bad request\n")
            return

        method, path = parts[0], parts[1]

        while True:
            line = await reader.readline()
            if not line or line in {b"\r\n", b"\n"}:
                break

        if method != "GET":
            await _write_response(
                writer,
                "405 Method Not Allowed",
                "text/plain",
                b"method not allowed\n",
            )
            return

        if path == "/health":
            payload = health_payload(
                _current_exposure,
                _position_pnl,
                nats_connected=_is_nats_connected(),
            )
            body = json_response(payload)
            await _write_response(writer, health_http_status(payload), "application/json", body)
            return

        if path == "/metrics":
            body = metrics_payload(
                _trade_counts,
                _current_exposure,
                _position_pnl,
                nats_connected=_is_nats_connected(),
            ).encode()
            await _write_response(writer, "200 OK", "text/plain; version=0.0.4", body)
            return

        await _write_response(writer, "404 Not Found", "text/plain", b"not found\n")
    finally:
        writer.close()
        await writer.wait_closed()


async def _write_response(
    writer: asyncio.StreamWriter,
    status: str,
    content_type: str,
    body: bytes,
) -> None:
    headers = [
        f"HTTP/1.1 {status}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body)}",
        "Connection: close",
        "",
        "",
    ]
    writer.write("\r\n".join(headers).encode() + body)
    await writer.drain()


async def ensure_wallet_bucket(js):
    try:
        await js.key_value("mercury_wallet_state")
    except BucketNotFoundError:
        await js.create_key_value(bucket="mercury_wallet_state")


async def reconcile_startup_state(js):
    if is_observe_mode_enabled():
        logger.info("Observation mode enabled; skipping exchange reconciliation.")
        return

    current_exposure = await load_exposure(js)
    reconcile_symbols = get_reconcile_symbols()
    open_orders: list[dict] = []
    for symbol in reconcile_symbols:
        open_orders.extend(await fetch_open_orders(symbol))

    if open_orders:
        logger.error(
            "Startup reconciliation failed: found %d open exchange orders "
            "with current exposure %.2f.",
            len(open_orders),
            current_exposure,
        )
        raise RuntimeError("Open exchange orders found during startup reconciliation")

    logger.info(
        "Startup reconciliation succeeded: no open exchange orders across %d symbols, "
        "current exposure %.2f.",
        len(reconcile_symbols),
        current_exposure,
    )


async def get_message_handler(js):
    async def message_handler(msg):
        signal = None
        try:
            signal = TradeSignal.model_validate_json(msg.data)
            logger.info(
                "Received trade signal (Confidence: %.2f): %s %s",
                signal.confidence_score,
                signal.action,
                signal.symbol,
            )

            # HALT is checked BEFORE sizing — it must short-circuit unconditionally.
            if await load_halt_flag(js):
                logger.warning("Trade rejected: HALT flag is set.")
                _record_trade_outcome(signal.action, "rejected")
                await msg.ack()
                return

            if is_observe_mode_enabled():
                sized_signal = signal
            else:
                decision = await size_signal(signal, js)
                if decision.sized_signal is None:
                    logger.warning(
                        "Trade skipped by sizer (reason=%s): %s %s",
                        decision.reason,
                        signal.action,
                        signal.symbol,
                    )
                    _record_trade_outcome(signal.action, f"skipped_{decision.reason}")
                    await msg.ack()
                    return
                sized_signal = decision.sized_signal

            # Notional + exposure checks run on the SIZED signal, never on the placeholder.
            if not await check_risk(sized_signal, js):
                await _refresh_runtime_state_safe(js)
                _record_trade_outcome(signal.action, "rejected")
                logger.warning("Trade rejected by risk manager.")
                await msg.ack()
                return

            result = await execute_trade(sized_signal)
            if result.status == "filled":
                await record_trade_execution(sized_signal, js)
                await _refresh_runtime_state_safe(js)
            else:
                await _refresh_current_exposure(js)
            _record_trade_outcome(signal.action, result.status)
            logger.info(
                "Trade valid and processed by exchange client with status=%s.",
                result.status,
            )
            # Acknowledge only after successful execution + NATS KV update
            await msg.ack()

        except ValidationError as exc:
            logger.error("Terminating malformed trade signal: %s", exc)
            await msg.term()
        except Exception as e:
            if signal is not None:
                _record_trade_outcome(signal.action, "error")
            logger.error("Transient trade-signal failure: %s", e, exc_info=True)
            await msg.nak(delay=_RETRY_DELAY_SECONDS)

    return message_handler


async def main():
    global _nats_client
    execution_mode = ensure_execution_mode_supported()
    session = await connect_jetstream()
    _nats_client = session.client
    js = session.js
    logger.info(
        "Executor service started in %s mode. Listening for trade signals...",
        execution_mode,
    )

    try:
        await ensure_wallet_bucket(js)
        await reconcile_startup_state(js)
        await _refresh_runtime_state_safe(js)
        await ensure_stream(js, name="trade_signals", subjects=["trade.*"])

        http_server = await asyncio.start_server(
            _serve_http,
            _EXECUTOR_HTTP_HOST,
            _EXECUTOR_HTTP_PORT,
        )
        logger.info(
            "Executor observability listening on http://%s:%d",
            _EXECUTOR_HTTP_HOST,
            _EXECUTOR_HTTP_PORT,
        )

        await subscribe_trade_signals(js)

        refresh_task = asyncio.create_task(_portfolio_refresh_loop(js))

        try:
            async with http_server:
                await http_server.serve_forever()
        finally:
            refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await refresh_task
    finally:
        try:
            await session.close()
        finally:
            _nats_client = None


async def subscribe_trade_signals(js):
    handler = await get_message_handler(js)
    await validate_push_consumer(
        js,
        stream_name="trade_signals",
        durable_name=_EXECUTOR_DURABLE_NAME,
        filter_subject="trade.signals",
        max_deliver=_CONSUMER_MAX_DELIVER,
    )
    return await js.subscribe(
        "trade.signals",
        durable=_EXECUTOR_DURABLE_NAME,
        cb=handler,
        config=push_consumer_config(max_deliver=_CONSUMER_MAX_DELIVER),
        manual_ack=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
