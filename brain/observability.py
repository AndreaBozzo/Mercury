from __future__ import annotations

import json
from collections.abc import Mapping

from core.order_book import OrderBook


def health_payload(
    books: Mapping[str, OrderBook],
    bootstrap_active: Mapping[str, bool],
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

    if not symbols:
        status = "starting"
    elif all(item["ready"] for item in symbols.values()):
        status = "ok"
    elif any(item["ready"] for item in symbols.values()):
        status = "degraded"
    else:
        status = "starting"

    return {
        "status": status,
        "books_ready": ready_count,
        "books_total": len(symbols),
        "symbols": symbols,
    }


def metrics_payload(
    books: Mapping[str, OrderBook],
    bootstrap_active: Mapping[str, bool],
) -> str:
    lines = [
        "# HELP mercury_brain_order_book_ready 1 when the local order book is ready.",
        "# TYPE mercury_brain_order_book_ready gauge",
        "# HELP mercury_brain_order_book_seq Current local order book sequence.",
        "# TYPE mercury_brain_order_book_seq gauge",
        "# HELP mercury_brain_order_book_bootstrapping 1 while snapshot bootstrap is active.",
        "# TYPE mercury_brain_order_book_bootstrapping gauge",
    ]

    for symbol in sorted(set(books) | set(bootstrap_active)):
        book = books.get(symbol)
        ready = 1 if book and book.is_ready else 0
        seq = book.levels()["seq"] if book else 0
        bootstrapping = 1 if bootstrap_active.get(symbol, False) else 0
        label = _metric_label(symbol)

        lines.append(f'mercury_brain_order_book_ready{{symbol="{label}"}} {ready}')
        lines.append(f'mercury_brain_order_book_seq{{symbol="{label}"}} {seq}')
        lines.append(f'mercury_brain_order_book_bootstrapping{{symbol="{label}"}} {bootstrapping}')

    lines.append("")
    return "\n".join(lines)


def json_response(payload: dict) -> bytes:
    return json.dumps(payload, sort_keys=True).encode()


def _metric_label(symbol: str) -> str:
    return symbol.replace("\\", "\\\\").replace('"', '\\"')
