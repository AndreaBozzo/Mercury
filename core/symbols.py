from __future__ import annotations

from typing import Final

__all__ = ["normalize_symbol", "split_symbol"]

_KNOWN_QUOTES: Final[tuple[str, ...]] = (
    "USDT",
    "FDUSD",
    "USDC",
    "BUSD",
    "BTC",
    "ETH",
    "BNB",
    "EUR",
    "TRY",
)


def normalize_symbol(raw: str | None) -> str:
    if not isinstance(raw, str):
        raise ValueError("symbol must be a non-empty string")

    symbol = raw.strip().upper()
    if not symbol:
        raise ValueError("symbol must be a non-empty string")

    for separator in ("/", "-", "_"):
        if separator not in symbol:
            continue
        parts = symbol.split(separator)
        if len(parts) == 2 and all(parts):
            return f"{parts[0]}/{parts[1]}"
        raise ValueError(f"invalid symbol format: {raw!r}")

    for quote in _KNOWN_QUOTES:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return f"{symbol[: -len(quote)]}/{quote}"

    raise ValueError(f"unsupported symbol format: {raw!r}")


def split_symbol(symbol: str) -> tuple[str, str]:
    normalized = normalize_symbol(symbol)
    if "/" not in normalized:
        return normalized, ""
    base, quote = normalized.split("/", 1)
    return base, quote
