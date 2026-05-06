from executor.observability import health_payload, metrics_payload


def test_executor_health_payload_reports_exposure_and_pnl():
    payload = health_payload(current_exposure=250.0, position_pnl=12.5)

    assert payload == {
        "status": "ok",
        "current_exposure": 250.0,
        "position_pnl": 12.5,
    }


def test_executor_metrics_payload_contains_trade_counters_and_gauges():
    payload = metrics_payload(
        trade_counts={("BUY", "filled"): 2, ("SELL", "rejected"): 1},
        current_exposure=250.0,
        position_pnl=12.5,
    )

    assert 'mercury_trades_executed_total{action="BUY",outcome="filled"} 2' in payload
    assert 'mercury_trades_executed_total{action="SELL",outcome="rejected"} 1' in payload
    assert "mercury_executor_current_exposure_dollars 250.0" in payload
    assert "mercury_position_pnl_dollars 12.5" in payload
