import pytest

from scripts.provision_nephtys import selected_streams


def test_default_market_data_profile_is_coinbase_btc_eur(monkeypatch):
    monkeypatch.delenv("MERCURY_MARKET_DATA_PROFILE", raising=False)

    streams = selected_streams()

    assert len(streams) == 1
    assert streams[0]["id"] == "coinbase_btc_eur_ticker"
    assert "BTC-EUR" in streams[0]["websocket"]["on_connect_send"]


def test_legacy_binance_profile_must_be_selected_explicitly():
    streams = selected_streams("binance_btc_usdt")

    assert {stream["id"] for stream in streams} == {
        "binance_btc",
        "binance_btc_depth",
    }


def test_unknown_market_data_profile_is_rejected():
    with pytest.raises(ValueError, match="Unknown MERCURY_MARKET_DATA_PROFILE"):
        selected_streams("unknown")
