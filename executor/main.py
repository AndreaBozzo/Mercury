import asyncio
import contextlib
import json
import logging
import os

from core.nats_utils import get_jetstream
from core.schemas import TradeSignal
from executor.exchange_client import (
    build_persisted_position_state,
    execute_trade,
    fetch_open_orders,
    fetch_portfolio_snapshot,
    get_reconcile_symbols,
    is_dry_run_enabled,
)
from executor.observability import health_payload, json_response, metrics_payload
from executor.risk_manager import (
    check_risk,
    load_exposure,
    load_position_states,
    record_trade_execution,
    save_exposure,
    save_position_states,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("executor")
_trade_counts: dict[tuple[str, str], int] = {}
_current_exposure = 0.0
_position_pnl = 0.0
_EXECUTOR_HTTP_HOST = os.getenv("EXECUTOR_HTTP_HOST", "127.0.0.1")
_EXECUTOR_HTTP_PORT = int(os.getenv("EXECUTOR_HTTP_PORT", "9109"))
_EXECUTOR_ACCOUNT_REFRESH_SECONDS = float(os.getenv("EXECUTOR_ACCOUNT_REFRESH_SECONDS", "15"))


def _record_trade_outcome(action: str, outcome: str) -> None:
    key = (action, outcome)
    _trade_counts[key] = _trade_counts.get(key, 0) + 1


def _set_current_exposure(value: float) -> None:
    global _current_exposure
    _current_exposure = value


def _set_position_pnl(value: float) -> None:
    global _position_pnl
    _position_pnl = value


async def _refresh_current_exposure(js) -> None:
    _set_current_exposure(await load_exposure(js))


async def _refresh_runtime_state(js) -> None:
    if is_dry_run_enabled():
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
            body = json_response(health_payload(_current_exposure, _position_pnl))
            await _write_response(writer, "200 OK", "application/json", body)
            return

        if path == "/metrics":
            body = metrics_payload(_trade_counts, _current_exposure, _position_pnl).encode()
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
    except Exception:
        await js.create_key_value(bucket="mercury_wallet_state")


async def reconcile_startup_state(js):
    if is_dry_run_enabled():
        logger.info("DRY_RUN enabled; skipping exchange reconciliation.")
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
            data = json.loads(msg.data.decode())
            signal = TradeSignal(**data)
            logger.info(
                "Received trade signal (Confidence: %.2f): %s %s",
                signal.confidence_score,
                signal.action,
                signal.symbol,
            )

            if await check_risk(signal, js):
                result = await execute_trade(signal)
                if result.status == "filled":
                    await record_trade_execution(signal, js)
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
            else:
                await _refresh_runtime_state_safe(js)
                _record_trade_outcome(signal.action, "rejected")
                logger.warning("Trade rejected by risk manager.")
                # Ack rejected signals to avoid infinite retry
                await msg.ack()

        except Exception as e:
            if signal is not None:
                _record_trade_outcome(signal.action, "error")
            logger.error("Error processing message: %s", e)
            # Non-risk execution failures: let the message time out / NAK externally

    return message_handler


async def main():
    js = await get_jetstream()
    logger.info("Executor service started. Listening for trade signals...")

    await ensure_wallet_bucket(js)
    await reconcile_startup_state(js)
    await _refresh_runtime_state_safe(js)

    try:
        await js.add_stream(name="trade_signals", subjects=["trade.*"])
    except Exception:
        pass

    http_server = await asyncio.start_server(_serve_http, _EXECUTOR_HTTP_HOST, _EXECUTOR_HTTP_PORT)
    logger.info(
        "Executor observability listening on http://%s:%d",
        _EXECUTOR_HTTP_HOST,
        _EXECUTOR_HTTP_PORT,
    )

    handler = await get_message_handler(js)
    await js.subscribe("trade.signals", durable="executor_service", cb=handler)

    refresh_task = asyncio.create_task(_portfolio_refresh_loop(js))

    try:
        async with http_server:
            await http_server.serve_forever()
    finally:
        refresh_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await refresh_task


if __name__ == "__main__":
    asyncio.run(main())
