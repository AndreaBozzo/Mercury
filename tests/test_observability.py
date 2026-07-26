from brain.observability import health_http_status, health_payload, metrics_payload
from core.order_book import OrderBook


def test_health_payload_reports_ready_and_bootstrap_state():
    ready_book = OrderBook("BTC/USDT")
    ready_book.apply_snapshot([[100.0, 1.0]], [[101.0, 1.0]], seq=42)

    payload = health_payload(
        books={"BTC/USDT": ready_book},
        bootstrap_active={"BTC/USDT": False, "ETH/USDT": True},
    )

    assert payload["status"] == "degraded"
    assert payload["order_books_required"] is True
    assert payload["books_ready"] == 1
    assert payload["books_total"] == 2
    assert payload["symbols"]["BTC/USDT"] == {"ready": True, "bootstrapping": False, "seq": 42}
    assert payload["symbols"]["ETH/USDT"] == {"ready": False, "bootstrapping": True, "seq": None}


def test_l1_health_is_ready_without_an_order_book():
    payload = health_payload({}, {}, order_books_required=False)

    assert payload == {
        "status": "ok",
        "order_books_required": False,
        "books_ready": 0,
        "books_total": 0,
        "symbols": {},
    }
    assert health_http_status(payload) == "200 OK"


def test_unready_l2_health_returns_service_unavailable():
    payload = health_payload({}, {}, order_books_required=True)

    assert payload["status"] == "starting"
    assert health_http_status(payload) == "503 Service Unavailable"


def test_metrics_payload_contains_prometheus_gauges():
    ready_book = OrderBook("BTC/USDT")
    ready_book.apply_snapshot([[100.0, 1.0]], [[101.0, 1.0]], seq=42)

    payload = metrics_payload(
        books={"BTC/USDT": ready_book},
        bootstrap_active={"BTC/USDT": False, "ETH/USDT": True},
        signal_counts={("BUY", "BTC/USDT"): 3},
        book_gap_counts={"BTC/USDT": 2},
    )

    assert 'mercury_brain_order_book_ready{symbol="BTC/USDT"} 1' in payload
    assert 'mercury_brain_order_book_seq{symbol="BTC/USDT"} 42' in payload
    assert 'mercury_brain_order_book_bootstrapping{symbol="ETH/USDT"} 1' in payload
    assert 'mercury_signals_emitted_total{action="BUY",symbol="BTC/USDT"} 3' in payload
    assert 'mercury_book_gap_total{symbol="BTC/USDT"} 2' in payload
