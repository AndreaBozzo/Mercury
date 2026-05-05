from executor.news_risk_agent import (
    analyze_news_with_heuristic,
    extract_bridge_texts,
    limits_for_sentiment,
)
from executor.risk_manager import default_risk_limits


def test_extract_bridge_texts_parses_query_output():
    output = (
        "Ricerca per: 'btc news'\n"
        "Testo: Exchange hack drains hot wallet\n"
        "--------\n"
        "Testo: ETF inflow accelerates\n"
    )

    texts = extract_bridge_texts(output)

    assert texts == ["Exchange hack drains hot wallet", "ETF inflow accelerates"]


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