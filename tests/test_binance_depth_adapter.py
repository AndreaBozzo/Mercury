from brain.adapters.binance_depth import parse_binance_depth
from core.schemas import MarketEvent


def test_parse_binance_depth_update():
    event = MarketEvent(
        source="binance_btc",
        type="depthUpdate",
        timestamp=1,
        seq=102,
        payload={
            "e": "depthUpdate",
            "E": 1_700_000_000_000,
            "s": "BTCUSDT",
            "U": 100,
            "u": 102,
            "b": [["67000.0", "1.5"], ["66999.0", "0"]],
            "a": [["67001.0", "2.0"]],
        },
    )

    update = parse_binance_depth(event)

    assert update is not None
    assert update.symbol == "BTC/USDT"
    assert update.first_seq == 100
    assert update.final_seq == 102
    assert update.timestamp == 1_700_000_000_000
    assert update.bids == [[67000.0, 1.5], [66999.0, 0.0]]
    assert update.asks == [[67001.0, 2.0]]


def test_parse_binance_depth_ignores_non_depth_event():
    event = MarketEvent(source="binance_btc", type="trade", timestamp=1, payload={})

    assert parse_binance_depth(event) is None


def test_parse_binance_depth_uses_envelope_seq_when_payload_u_missing():
    event = MarketEvent(
        source="binance_btc",
        type="depthUpdate",
        timestamp=1,
        seq=12,
        payload={"e": "depthUpdate", "s": "ETHUSDT", "U": 10, "b": [], "a": []},
    )

    update = parse_binance_depth(event)

    assert update is not None
    assert update.final_seq == 12
