from executor import news_risk_agent
from executor.news_risk_agent import (
    analyze_news_with_heuristic,
    extract_bridge_texts,
    limits_for_sentiment,
)
from executor.risk_manager import default_risk_limits


def test_extract_bridge_texts_parses_json_query_output():
    output = (
        '[{"text": "Exchange hack drains hot wallet", "source_id": "rss_news"}, '
        '{"text": "ETF inflow accelerates", "source_id": "rss_news"}]'
    )

    texts = extract_bridge_texts(output)

    assert texts == ["Exchange hack drains hot wallet", "ETF inflow accelerates"]


def test_extract_bridge_texts_falls_back_to_legacy_human_output():
    output = (
        "Ricerca per: 'btc news'\n"
        "Testo: Exchange hack drains hot wallet\n"
        "--------\n"
        "Testo: ETF inflow accelerates\n"
    )

    texts = extract_bridge_texts(output)

    assert texts == ["Exchange hack drains hot wallet", "ETF inflow accelerates"]


def test_extract_bridge_texts_ignores_non_list_json():
    assert extract_bridge_texts('{"text": "not a list"}') == []


def test_query_bridge_texts_uses_http_service(monkeypatch):
    class Response:
        text = '[{"text": "Exchange hack drains hot wallet", "source_id": "rss_news"}]'

        def raise_for_status(self):
            return None

    captured = {}

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(news_risk_agent, "NEWS_RISK_BRIDGE_SERVICE_URL", "http://bridge:8787")
    monkeypatch.setattr(news_risk_agent, "NEWS_RISK_QUERY_SOURCE", "rss_news")
    monkeypatch.setattr(news_risk_agent, "NEWS_RISK_QUERY_EVENT_TYPE", "article")
    monkeypatch.setattr(news_risk_agent, "NEWS_RISK_QUERY_SYMBOL", "BTCUSDT")
    monkeypatch.setattr(news_risk_agent.requests, "get", fake_get)

    texts = news_risk_agent.query_bridge_texts()

    assert texts == ["Exchange hack drains hot wallet"]
    assert captured["url"] == "http://bridge:8787/query"
    assert ("source", "rss_news") in captured["params"]
    assert ("event_type", "article") in captured["params"]
    assert ("symbol", "BTCUSDT") in captured["params"]


def test_bridge_query_cli_command_prefers_bridge_venv(monkeypatch, tmp_path):
    bridge_root = tmp_path / "bridge"
    python_path = bridge_root / ".venv" / "bin" / "python"
    python_path.parent.mkdir(parents=True)
    python_path.write_text("#!/usr/bin/env python\n", encoding="utf-8")

    monkeypatch.setattr(news_risk_agent, "NEWS_RISK_BRIDGE_PATH", str(bridge_root))
    monkeypatch.setattr(news_risk_agent, "NEWS_RISK_BRIDGE_PYTHON", "")
    monkeypatch.setattr(news_risk_agent, "NEWS_RISK_QUERY_SYMBOL", "BTCUSDT")

    command = news_risk_agent._bridge_query_cli_command()

    assert command[0] == str(python_path)
    assert command[1] == "query.py"
    assert "--symbol" in command


def test_analyze_news_with_heuristic_scores_negative_news_lower():
    analysis = analyze_news_with_heuristic(
        [
            "Exchange hack and insolvency fears spread across crypto desks",
            "Major outage hits trading venue during liquidation event",
        ]
    )

    assert float(analysis["sentiment_score"]) < 0
    assert analysis["source"] == "heuristic_news_sentiment"


def test_limits_for_sentiment_reduces_exposure_on_bad_news():
    limits = limits_for_sentiment(
        {
            "sentiment_score": -0.8,
            "source": "heuristic_news_sentiment",
            "rationale": "hack, insolvency",
        },
        default_risk_limits(),
    )

    assert float(limits["max_exposure_dollars"]) < float(
        default_risk_limits()["max_exposure_dollars"]
    )
    assert float(limits["max_trade_notional_dollars"]) <= float(
        default_risk_limits()["max_trade_notional_dollars"]
    )