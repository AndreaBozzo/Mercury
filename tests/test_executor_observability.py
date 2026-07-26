from executor.observability import health_http_status, health_payload, metrics_payload


def test_executor_health_payload_reports_exposure_and_pnl():
    payload = health_payload(current_exposure=250.0, position_pnl=12.5)

    assert payload == {
        "status": "ok",
        "nats_connected": True,
        "current_exposure": 250.0,
        "position_pnl": 12.5,
    }
    assert health_http_status(payload) == "200 OK"


def test_executor_health_reports_nats_disconnect():
    payload = health_payload(
        current_exposure=250.0,
        position_pnl=12.5,
        nats_connected=False,
    )

    assert payload["status"] == "unavailable"
    assert payload["nats_connected"] is False
    assert health_http_status(payload) == "503 Service Unavailable"


def test_executor_metrics_payload_contains_trade_counters_and_gauges():
    payload = metrics_payload(
        trade_counts={("BUY", "filled"): 2, ("SELL", "rejected"): 1},
        current_exposure=250.0,
        position_pnl=12.5,
        nats_connected=True,
    )

    assert 'mercury_trades_executed_total{action="BUY",outcome="filled"} 2' in payload
    assert 'mercury_trades_executed_total{action="SELL",outcome="rejected"} 1' in payload
    assert "mercury_executor_current_exposure_dollars 250.0" in payload
    assert "mercury_position_pnl_dollars 12.5" in payload
    assert "mercury_executor_nats_connected 1" in payload
