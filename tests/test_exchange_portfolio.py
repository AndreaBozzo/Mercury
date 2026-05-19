import pytest

from executor.exchange_client import build_persisted_position_state, fetch_portfolio_snapshot


class FakeExchange:
    id = "binance"

    def __init__(self):
        self.closed = False
        self.calls: list[tuple[str, int | None, int, dict | None]] = []

    async def fetch_balance(self):
        return {"total": {"BTC": 0.5, "USDT": 1000.0}}

    async def fetch_ticker(self, symbol: str):
        assert symbol == "BTC/USDT"
        return {"last": 120.0}

    async def fetch_my_trades(
        self,
        symbol: str,
        since: int | None = None,
        limit: int = 200,
        params=None,
    ):
        assert symbol == "BTC/USDT"
        assert limit == 200
        self.calls.append((symbol, since, limit, params))
        return [
            {"timestamp": 1, "side": "buy", "amount": 1.0, "cost": 100.0},
            {"timestamp": 2, "side": "sell", "amount": 0.5, "cost": 60.0},
        ]

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_fetch_portfolio_snapshot_uses_exchange_state_for_exposure_and_pnl(monkeypatch):
    exchange = FakeExchange()

    monkeypatch.setattr("executor.exchange_client.get_exchange", lambda: exchange)

    snapshot = await fetch_portfolio_snapshot(["BTCUSDT"])

    assert snapshot.current_exposure == 60.0
    assert snapshot.position_pnl == 10.0
    assert snapshot.symbols["BTC/USDT"].quantity == 0.5
    assert snapshot.symbols["BTC/USDT"].mark_price == 120.0
    assert snapshot.symbols["BTC/USDT"].cost_basis == 50.0
    assert snapshot.symbols["BTC/USDT"].market_value == 60.0
    assert snapshot.symbols["BTC/USDT"].unrealized_pnl == 10.0
    assert exchange.calls == [("BTC/USDT", 0, 200, None)]
    assert exchange.closed is True


class PaginatedExchange(FakeExchange):
    def __init__(self):
        super().__init__()
        self.pages = {
            0: [
                {"id": "buy-1", "timestamp": 1, "side": "buy", "amount": 0.4, "cost": 40.0},
            ],
            2: [
                {"id": "buy-2", "timestamp": 2, "side": "buy", "amount": 0.6, "cost": 60.0},
            ],
        }

    async def fetch_balance(self):
        return {"total": {"BTC": 1.0, "USDT": 1000.0}}

    async def fetch_my_trades(
        self,
        symbol: str,
        since: int | None = None,
        limit: int = 200,
        params=None,
    ):
        assert symbol == "BTC/USDT"
        self.calls.append((symbol, since, limit, params))
        return list(self.pages.get(since, []))


@pytest.mark.asyncio
async def test_fetch_portfolio_snapshot_pages_trade_history_for_carried_positions(monkeypatch):
    exchange = PaginatedExchange()

    monkeypatch.setattr("executor.exchange_client.get_exchange", lambda: exchange)

    snapshot = await fetch_portfolio_snapshot(["BTC/USDT"])

    assert snapshot.symbols["BTC/USDT"].cost_basis == 100.0
    assert exchange.calls == [
        ("BTC/USDT", 0, 200, None),
        ("BTC/USDT", 2, 200, None),
    ]


class PersistedStateExchange(FakeExchange):
    async def fetch_balance(self):
        return {"total": {"BTC": 0.999, "USDT": 1000.0}}

    async def fetch_my_trades(
        self,
        symbol: str,
        since: int | None = None,
        limit: int = 200,
        params=None,
    ):
        assert symbol == "BTC/USDT"
        self.calls.append((symbol, since, limit, params))
        assert since == 11
        return [
            {"id": "sell-2", "timestamp": 12, "side": "sell", "amount": 0.001, "cost": 81.0},
        ]


@pytest.mark.asyncio
async def test_fetch_portfolio_snapshot_updates_from_persisted_position_state(monkeypatch):
    exchange = PersistedStateExchange()

    monkeypatch.setattr("executor.exchange_client.get_exchange", lambda: exchange)

    snapshot = await fetch_portfolio_snapshot(
        ["BTC/USDT"],
        persisted_positions={
            "BTC/USDT": {
                "symbol": "BTC/USDT",
                "quantity": 1.0,
                "cost_basis": 100.0,
                "last_trade_timestamp": 10,
            }
        },
    )

    assert snapshot.symbols["BTC/USDT"].quantity == 0.999
    assert snapshot.symbols["BTC/USDT"].cost_basis == pytest.approx(99.9)
    assert exchange.calls == [("BTC/USDT", 11, 200, None)]


def test_build_persisted_position_state_tracks_latest_trade_timestamp():
    state = build_persisted_position_state(
        symbol_state=type(
            "S",
            (),
            {
                "symbol": "BTC/USDT",
                "quantity": 0.5,
                "cost_basis": 50.0,
                "last_trade_timestamp": 3,
            },
        )(),
    )

    assert state == {
        "symbol": "BTC/USDT",
        "quantity": 0.5,
        "cost_basis": 50.0,
        "last_trade_timestamp": 3,
    }


class FakeSnapshot:
    def __init__(
        self,
        current_exposure: float,
        position_pnl: float,
        quote_free: dict[str, float] | None = None,
    ):
        self.current_exposure = current_exposure
        self.position_pnl = position_pnl
        self.symbols: dict[str, object] = {}
        self.quote_free = quote_free if quote_free is not None else {}


@pytest.mark.asyncio
async def test_refresh_runtime_state_uses_exchange_snapshot(monkeypatch):
    import executor.main as executor_main

    monkeypatch.setattr(executor_main, "_current_exposure", 0.0)
    monkeypatch.setattr(executor_main, "_position_pnl", 0.0)
    monkeypatch.setattr(executor_main, "is_dry_run_enabled", lambda: False)
    monkeypatch.setattr(executor_main, "get_reconcile_symbols", lambda: ["BTC/USDT"])

    async def fake_fetch_portfolio_snapshot(_symbols, persisted_positions=None):
        assert persisted_positions == {}
        return FakeSnapshot(
            current_exposure=75.0,
            position_pnl=12.5,
            quote_free={"USDT": 100.0, "EUR": 9.0},
        )

    saved: list[float] = []
    saved_states: list[dict] = []
    saved_balances: list[dict] = []

    async def fake_save_exposure(_js, value: float):
        saved.append(value)

    async def fake_load_position_states(_js, _symbols):
        return {}

    async def fake_save_position_states(_js, states):
        saved_states.append(states)

    async def fake_save_available_balance(_js, quote_free, computed_at_iso):
        saved_balances.append({"quote_free": dict(quote_free), "computed_at": computed_at_iso})

    monkeypatch.setattr(executor_main, "fetch_portfolio_snapshot", fake_fetch_portfolio_snapshot)
    monkeypatch.setattr(executor_main, "load_position_states", fake_load_position_states)
    monkeypatch.setattr(executor_main, "save_exposure", fake_save_exposure)
    monkeypatch.setattr(executor_main, "save_position_states", fake_save_position_states)
    monkeypatch.setattr(executor_main, "save_available_balance", fake_save_available_balance)

    await executor_main._refresh_runtime_state(js=object())

    assert executor_main._current_exposure == 75.0
    assert executor_main._position_pnl == 12.5
    assert saved == [75.0]
    assert saved_states == [{}]
    assert len(saved_balances) == 1
    assert saved_balances[0]["quote_free"] == {"USDT": 100.0, "EUR": 9.0}
    assert saved_balances[0]["computed_at"]  # ISO timestamp present
