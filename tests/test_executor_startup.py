import pytest

from executor.main import reconcile_startup_state


@pytest.mark.asyncio
async def test_reconcile_startup_state_skips_exchange_checks_in_dry_run(monkeypatch):
    called = False

    async def fake_fetch_open_orders():
        nonlocal called
        called = True
        return []

    monkeypatch.setattr("executor.main.is_dry_run_enabled", lambda: True)
    monkeypatch.setattr("executor.main.fetch_open_orders", fake_fetch_open_orders)

    await reconcile_startup_state(js=object())

    assert called is False


@pytest.mark.asyncio
async def test_reconcile_startup_state_accepts_clean_exchange_state(monkeypatch):
    async def fake_fetch_open_orders():
        return []

    async def fake_load_exposure(_js):
        return 250.0

    monkeypatch.setattr("executor.main.is_dry_run_enabled", lambda: False)
    monkeypatch.setattr("executor.main.fetch_open_orders", fake_fetch_open_orders)
    monkeypatch.setattr("executor.main.load_exposure", fake_load_exposure)

    await reconcile_startup_state(js=object())


@pytest.mark.asyncio
async def test_reconcile_startup_state_fails_closed_when_open_orders_exist(monkeypatch):
    async def fake_fetch_open_orders():
        return [{"id": "open-1"}]

    async def fake_load_exposure(_js):
        return 250.0

    monkeypatch.setattr("executor.main.is_dry_run_enabled", lambda: False)
    monkeypatch.setattr("executor.main.fetch_open_orders", fake_fetch_open_orders)
    monkeypatch.setattr("executor.main.load_exposure", fake_load_exposure)

    with pytest.raises(RuntimeError, match="Open exchange orders"):
        await reconcile_startup_state(js=object())