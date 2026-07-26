from __future__ import annotations

import json
from collections.abc import Mapping

from core.order_book import OrderBook


def health_payload(
    books: Mapping[str, OrderBook],
    bootstrap_active: Mapping[str, bool],
    *,
    order_books_required: bool = True,
    nats_connected: bool = True,
) -> dict:
    symbols: dict[str, dict] = {}
    ready_count = 0

    for symbol in sorted(set(books) | set(bootstrap_active)):
        book = books.get(symbol)
        ready = bool(book and book.is_ready)
        bootstrapping = bootstrap_active.get(symbol, False)
        seq = book.levels()["seq"] if book else None

        if ready:
            ready_count += 1

        symbols[symbol] = {
            "ready": ready,
            "bootstrapping": bootstrapping,
            "seq": seq,
        }

    if not nats_connected:
        status = "unavailable"
    elif not order_books_required:
        status = "ok"
    elif not symbols:
        status = "starting"
    elif all(item["ready"] for item in symbols.values()):
        status = "ok"
    elif any(item["ready"] for item in symbols.values()):
        status = "degraded"
    else:
        status = "starting"

    return {
        "status": status,
        "nats_connected": nats_connected,
        "order_books_required": order_books_required,
        "books_ready": ready_count,
        "books_total": len(symbols),
        "symbols": symbols,
    }


def health_http_status(payload: Mapping[str, object]) -> str:
    """Expose readiness failures to container orchestrators."""
    if payload.get("status") == "ok":
        return "200 OK"
    return "503 Service Unavailable"


def metrics_payload(
    books: Mapping[str, OrderBook],
    bootstrap_active: Mapping[str, bool],
    signal_counts: Mapping[tuple[str, str], int] | None = None,
    book_gap_counts: Mapping[str, int] | None = None,
    *,
    nats_connected: bool = True,
) -> str:
    signal_counts = signal_counts or {}
    book_gap_counts = book_gap_counts or {}

    lines = [
        "# HELP mercury_brain_order_book_ready 1 when the local order book is ready.",
        "# TYPE mercury_brain_order_book_ready gauge",
        "# HELP mercury_brain_order_book_seq Current local order book sequence.",
        "# TYPE mercury_brain_order_book_seq gauge",
        "# HELP mercury_brain_order_book_bootstrapping 1 while snapshot bootstrap is active.",
        "# TYPE mercury_brain_order_book_bootstrapping gauge",
        "# HELP mercury_signals_emitted_total Total trade signals emitted by the brain.",
        "# TYPE mercury_signals_emitted_total counter",
        "# HELP mercury_book_gap_total Total order-book sequence gap events seen by the brain.",
        "# TYPE mercury_book_gap_total counter",
        "# HELP mercury_brain_nats_connected 1 while the NATS client is connected.",
        "# TYPE mercury_brain_nats_connected gauge",
        f"mercury_brain_nats_connected {1 if nats_connected else 0}",
    ]

    for symbol in sorted(set(books) | set(bootstrap_active) | set(book_gap_counts)):
        book = books.get(symbol)
        ready = 1 if book and book.is_ready else 0
        seq = book.levels()["seq"] if book else 0
        bootstrapping = 1 if bootstrap_active.get(symbol, False) else 0
        book_gaps = book_gap_counts.get(symbol, 0)
        label = _metric_label(symbol)

        lines.append(f'mercury_brain_order_book_ready{{symbol="{label}"}} {ready}')
        lines.append(f'mercury_brain_order_book_seq{{symbol="{label}"}} {seq}')
        lines.append(f'mercury_brain_order_book_bootstrapping{{symbol="{label}"}} {bootstrapping}')
        lines.append(f'mercury_book_gap_total{{symbol="{label}"}} {book_gaps}')

    for (action, symbol), count in sorted(signal_counts.items()):
        action_label = _metric_label(action)
        symbol_label = _metric_label(symbol)
        lines.append(
            "mercury_signals_emitted_total"
            f'{{action="{action_label}",symbol="{symbol_label}"}} {count}'
        )

    lines.append("")
    return "\n".join(lines)


def json_response(payload: dict) -> bytes:
    return json.dumps(payload, sort_keys=True).encode()


def _metric_label(symbol: str) -> str:
    return symbol.replace("\\", "\\\\").replace('"', '\\"')
