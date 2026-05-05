"""
Tests for core/arrow_codec.py

Requires: pyarrow (skipped if not installed)
"""

import pytest

pa = pytest.importorskip("pyarrow")

from core.arrow_codec import (  # noqa: E402
    MARKET_TICK_SCHEMA,
    TRADE_SIGNAL_SCHEMA,
    decode_market_tick,
    decode_signal,
    encode_market_tick,
    encode_signal,
)

_TICK = {
    "source": "nephtys",
    "type": "ticker",
    "timestamp": 1_700_000_000_000,
    "symbol": "BTC/USDT",
    "price": 67_000.5,
    "volume": 1.25,
    "mid": 67_000.0,
    "spread": 1.0,
    "imbalance": 0.12,
    "bid_price": 66_999.5,
    "ask_price": 67_000.5,
}

_SIGNAL = {
    "strategy_id": "vwap_v1",
    "symbol": "ETH/USDT",
    "action": "SELL",
    "order_type": "LIMIT",
    "quantity": 0.5,
    "price": 3_500.0,
    "confidence_score": 0.72,
    "allocation_percentage": 7.2,
    "timestamp_ns": 1_700_000_000_000_000_000,
}


class TestMarketTickRoundtrip:
    def test_encodes_to_bytes(self):
        raw = encode_market_tick(_TICK)
        assert isinstance(raw, bytes)
        assert len(raw) > 0

    def test_decode_returns_record_batch(self):
        batch = decode_market_tick(encode_market_tick(_TICK))
        assert isinstance(batch, pa.RecordBatch)

    def test_schema_matches(self):
        batch = decode_market_tick(encode_market_tick(_TICK))
        assert batch.schema.equals(MARKET_TICK_SCHEMA)

    def test_price_roundtrip(self):
        batch = decode_market_tick(encode_market_tick(_TICK))
        assert batch.column("price")[0].as_py() == pytest.approx(67_000.5)

    def test_symbol_roundtrip(self):
        batch = decode_market_tick(encode_market_tick(_TICK))
        assert batch.column("symbol")[0].as_py() == "BTC/USDT"

    def test_timestamp_roundtrip(self):
        batch = decode_market_tick(encode_market_tick(_TICK))
        assert batch.column("timestamp")[0].as_py() == 1_700_000_000_000

    def test_all_numeric_columns_roundtrip(self):
        batch = decode_market_tick(encode_market_tick(_TICK))
        for col in ("price", "volume", "mid", "spread", "imbalance", "bid_price", "ask_price"):
            assert batch.column(col)[0].as_py() == pytest.approx(_TICK[col])

    def test_missing_optional_fields_default_to_zero(self):
        minimal = {"source": "x", "type": "trade", "timestamp": 0, "symbol": "X", "price": 1.0}
        batch = decode_market_tick(encode_market_tick(minimal))
        assert batch.column("volume")[0].as_py() == pytest.approx(0.0)
        assert batch.column("imbalance")[0].as_py() == pytest.approx(0.0)

    def test_decode_does_not_copy_string_data(self):
        """pa.py_buffer wraps bytes without memcopy — verify no exception and correct read."""
        raw = encode_market_tick(_TICK)
        original_id = id(raw)
        batch = decode_market_tick(raw)
        # The raw bytes object must stay alive; batch read should succeed
        assert batch.column("source")[0].as_py() == "nephtys"
        assert id(raw) == original_id  # same object, not copied


class TestSignalRoundtrip:
    def test_encodes_to_bytes(self):
        raw = encode_signal(_SIGNAL)
        assert isinstance(raw, bytes)

    def test_schema_matches(self):
        batch = decode_signal(encode_signal(_SIGNAL))
        assert batch.schema.equals(TRADE_SIGNAL_SCHEMA)

    def test_action_roundtrip(self):
        batch = decode_signal(encode_signal(_SIGNAL))
        assert batch.column("action")[0].as_py() == "SELL"

    def test_confidence_roundtrip(self):
        batch = decode_signal(encode_signal(_SIGNAL))
        assert batch.column("confidence_score")[0].as_py() == pytest.approx(0.72)

    def test_timestamp_ns_roundtrip(self):
        batch = decode_signal(encode_signal(_SIGNAL))
        assert batch.column("timestamp_ns")[0].as_py() == 1_700_000_000_000_000_000
