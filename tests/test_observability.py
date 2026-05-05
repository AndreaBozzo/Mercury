from brain.observability import health_payload, metrics_payload
from core.order_book import OrderBook


def test_health_payload_reports_ready_and_bootstrap_state():
    ready_book = OrderBook("BTC/USDT")
    ready_book.apply_snapshot([[100.0, 1.0]], [[101.0, 1.0]], seq=42)

    payload = health_payload(
        books={"BTC/USDT": ready_book},
        bootstrap_active={"BTC/USDT": False, "ETH/USDT": True},
    )

    assert payload["status"] == "degraded"
    assert payload["books_ready"] == 1
    assert payload["books_total"] == 2
    assert payload["symbols"]["BTC/USDT"] == {"ready": True, "bootstrapping": False, "seq": 42}
    assert payload["symbols"]["ETH/USDT"] == {"ready": False, "bootstrapping": True, "seq": None}


def test_metrics_payload_contains_prometheus_gauges():
    ready_book = OrderBook("BTC/USDT")
    ready_book.apply_snapshot([[100.0, 1.0]], [[101.0, 1.0]], seq=42)

    payload = metrics_payload(
        books={"BTC/USDT": ready_book},
        bootstrap_active={"BTC/USDT": False, "ETH/USDT": True},
    )

    assert 'mercury_brain_order_book_ready{symbol="BTC/USDT"} 1' in payload
    assert 'mercury_brain_order_book_seq{symbol="BTC/USDT"} 42' in payload
    assert 'mercury_brain_order_book_bootstrapping{symbol="ETH/USDT"} 1' in payload
