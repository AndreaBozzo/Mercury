import pytest

from executor import exchange_client
from executor.exchange_client import (
    LIVE_TRADING_CONFIRMATION,
    ExchangeConfigurationError,
    get_exchange,
    get_execution_mode,
)

_EXECUTION_ENV = (
    "EXECUTION_MODE",
    "EXECUTION_EXCHANGE_ID",
    "EXCHANGE_API_KEY",
    "EXCHANGE_SECRET",
    "EXCHANGE_PASSWORD",
    "LIVE_TRADING_CONFIRMATION",
    "DRY_RUN",
    "BINANCE_SANDBOX",
    "BINANCE_API_KEY",
    "BINANCE_SECRET_KEY",
)


@pytest.fixture(autouse=True)
def _clear_execution_environment(monkeypatch):
    for name in _EXECUTION_ENV:
        monkeypatch.delenv(name, raising=False)


class FakeExchange:
    id = "fake"

    def __init__(self, config):
        self.config = config
        self.sandbox_calls: list[bool] = []

    def set_sandbox_mode(self, enabled: bool):
        self.sandbox_calls.append(enabled)


def test_execution_mode_defaults_to_observe():
    assert get_execution_mode() == "observe"


def test_execution_mode_rejects_unknown_or_malformed_values(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "production")
    with pytest.raises(ExchangeConfigurationError, match="EXECUTION_MODE"):
        get_execution_mode()

    monkeypatch.delenv("EXECUTION_MODE")
    monkeypatch.setenv("DRY_RUN", "flase")
    with pytest.raises(ExchangeConfigurationError, match="DRY_RUN"):
        get_execution_mode()


def test_observe_mode_never_constructs_an_exchange():
    with pytest.raises(ExchangeConfigurationError, match="Observation mode"):
        get_exchange()


def test_sandbox_requires_an_explicit_exchange_and_credentials(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "sandbox")

    with pytest.raises(ExchangeConfigurationError, match="EXECUTION_EXCHANGE_ID"):
        get_exchange()

    monkeypatch.setenv("EXECUTION_EXCHANGE_ID", "kraken")
    with pytest.raises(ExchangeConfigurationError, match="EXCHANGE_API_KEY"):
        get_exchange()


def test_sandbox_builds_named_ccxt_exchange_and_enables_sandbox(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "sandbox")
    monkeypatch.setenv("EXECUTION_EXCHANGE_ID", "kraken")
    monkeypatch.setenv("EXCHANGE_API_KEY", "test-key")
    monkeypatch.setenv("EXCHANGE_SECRET", "test-secret")
    monkeypatch.setenv("EXCHANGE_PASSWORD", "test-password")
    monkeypatch.setattr(exchange_client.ccxt, "kraken", FakeExchange)

    exchange = get_exchange()

    assert exchange.config == {
        "apiKey": "test-key",
        "secret": "test-secret",
        "password": "test-password",
        "enableRateLimit": True,
    }
    assert exchange.sandbox_calls == [True]


def test_live_mode_requires_second_explicit_confirmation(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("EXECUTION_EXCHANGE_ID", "kraken")
    monkeypatch.setenv("EXCHANGE_API_KEY", "live-key")
    monkeypatch.setenv("EXCHANGE_SECRET", "live-secret")

    with pytest.raises(ExchangeConfigurationError, match="LIVE_TRADING_CONFIRMATION"):
        get_exchange()


def test_live_mode_constructs_exchange_without_sandbox_after_confirmation(monkeypatch):
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("EXECUTION_EXCHANGE_ID", "kraken")
    monkeypatch.setenv("EXCHANGE_API_KEY", "live-key")
    monkeypatch.setenv("EXCHANGE_SECRET", "live-secret")
    monkeypatch.setenv("LIVE_TRADING_CONFIRMATION", LIVE_TRADING_CONFIRMATION)
    monkeypatch.setattr(exchange_client.ccxt, "kraken", FakeExchange)

    exchange = get_exchange()

    assert exchange.config["apiKey"] == "live-key"
    assert exchange.sandbox_calls == []
