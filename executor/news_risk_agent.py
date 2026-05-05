from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import time
from collections.abc import Iterable

import requests  # type: ignore[import-untyped]

from core.nats_utils import get_jetstream
from executor.risk_manager import KV_BUCKET, default_risk_limits, save_risk_limits

logger = logging.getLogger("news_risk_agent")

NEWS_RISK_AGENT_INTERVAL_SECONDS = float(os.getenv("NEWS_RISK_AGENT_INTERVAL_SECONDS", "300"))
NEWS_RISK_BRIDGE_PATH = os.getenv(
    "NEWS_RISK_BRIDGE_PATH",
    "/home/andrea/Documenti/lancedb-nephtys-bridge",
)
NEWS_RISK_QUERY = os.getenv(
    "NEWS_RISK_QUERY",
    "bitcoin exchange hack insolvency liquidation fraud sanctions regulation outage",
)
NEWS_RISK_QUERY_LIMIT = int(os.getenv("NEWS_RISK_QUERY_LIMIT", "8"))
NEWS_RISK_ANALYZER = os.getenv("NEWS_RISK_ANALYZER", "heuristic").strip().lower()
NEWS_RISK_LLM_API_KEY = os.getenv("NEWS_RISK_LLM_API_KEY")
NEWS_RISK_LLM_BASE_URL = os.getenv("NEWS_RISK_LLM_BASE_URL", "https://api.openai.com/v1")
NEWS_RISK_LLM_MODEL = os.getenv("NEWS_RISK_LLM_MODEL", "gpt-4o-mini")

_NEGATIVE_KEYWORDS = {
    "hack": 0.8,
    "exploit": 0.9,
    "insolvency": 1.0,
    "bankruptcy": 1.0,
    "liquidation": 0.8,
    "fraud": 0.9,
    "lawsuit": 0.5,
    "ban": 0.7,
    "sanctions": 0.6,
    "outage": 0.4,
    "breach": 0.8,
    "war": 0.7,
    "tariff": 0.3,
}
_POSITIVE_KEYWORDS = {
    "approval": 0.4,
    "inflow": 0.4,
    "partnership": 0.3,
    "adoption": 0.3,
    "upgrade": 0.2,
    "launch": 0.2,
}


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    js = await get_jetstream()
    await _ensure_wallet_bucket(js)
    logger.info("News risk agent started.")

    while True:
        try:
            texts = await asyncio.to_thread(query_bridge_texts)
            if not texts:
                logger.info("No bridge results available; keeping existing/default risk limits.")
            else:
                analysis = await asyncio.to_thread(analyze_news_sentiment, texts)
                limits = limits_for_sentiment(analysis, default_risk_limits())
                await save_risk_limits(js, limits)
                logger.info(
                    "Updated risk limits from %s sentiment score %.2f: exposure=%.2f trade=%.2f",
                    limits["source"],
                    float(limits["sentiment_score"]),
                    float(limits["max_exposure_dollars"]),
                    float(limits["max_trade_notional_dollars"]),
                )
        except Exception:
            logger.exception("News risk agent iteration failed")

        await asyncio.sleep(NEWS_RISK_AGENT_INTERVAL_SECONDS)


def query_bridge_texts() -> list[str]:
    if not os.path.isdir(NEWS_RISK_BRIDGE_PATH):
        logger.warning("Bridge path %s does not exist", NEWS_RISK_BRIDGE_PATH)
        return []

    command = [
        "uv",
        "run",
        "query.py",
        NEWS_RISK_QUERY,
        "--limit",
        str(NEWS_RISK_QUERY_LIMIT),
        "--all-namespaces",
    ]
    completed = subprocess.run(
        command,
        cwd=NEWS_RISK_BRIDGE_PATH,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        logger.warning("Bridge query failed: %s", completed.stderr.strip())
        return []
    return extract_bridge_texts(completed.stdout)


def extract_bridge_texts(output: str) -> list[str]:
    texts: list[str] = []
    for line in output.splitlines():
        if line.startswith("Testo:"):
            text = line.split("Testo:", 1)[1].strip()
            if text:
                texts.append(text)
    return texts


def analyze_news_sentiment(texts: Iterable[str]) -> dict[str, float | str]:
    text_list = [text for text in texts if text]
    if not text_list:
        return {"sentiment_score": 0.0, "source": "heuristic", "rationale": "No texts available."}

    if NEWS_RISK_ANALYZER == "openai" and NEWS_RISK_LLM_API_KEY:
        try:
            return analyze_news_with_openai(text_list)
        except Exception:
            logger.exception("Falling back to heuristic sentiment analysis")

    return analyze_news_with_heuristic(text_list)


def analyze_news_with_heuristic(texts: list[str]) -> dict[str, float | str]:
    score = 0.0
    matched: list[str] = []

    for text in texts:
        lower = text.lower()
        for keyword, weight in _NEGATIVE_KEYWORDS.items():
            if keyword in lower:
                score -= weight
                matched.append(f"-{keyword}")
        for keyword, weight in _POSITIVE_KEYWORDS.items():
            if keyword in lower:
                score += weight
                matched.append(f"+{keyword}")

    normalized = max(-1.0, min(1.0, score / max(len(texts), 1)))
    rationale = ", ".join(matched[:8]) if matched else "No risk keywords matched."
    return {
        "sentiment_score": normalized,
        "source": "heuristic_news_sentiment",
        "rationale": rationale,
    }


def analyze_news_with_openai(texts: list[str]) -> dict[str, float | str]:
    prompt = {
        "model": NEWS_RISK_LLM_MODEL,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a risk analyst for a crypto trading system. "
                    "Return strict JSON with keys sentiment_score and rationale. "
                    "sentiment_score must be between -1 and 1, where lower means more risk-off."
                ),
            },
            {
                "role": "user",
                "content": "\n".join(f"- {text}" for text in texts[:NEWS_RISK_QUERY_LIMIT]),
            },
        ],
    }
    response = requests.post(
        f"{NEWS_RISK_LLM_BASE_URL.rstrip('/')}/chat/completions",
        headers={
            "Authorization": f"Bearer {NEWS_RISK_LLM_API_KEY}",
            "Content-Type": "application/json",
        },
        data=json.dumps(prompt),
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    content = payload["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    return {
        "sentiment_score": max(-1.0, min(1.0, float(parsed.get("sentiment_score", 0.0)))),
        "source": "openai_news_sentiment",
        "rationale": str(parsed.get("rationale") or "LLM analysis"),
    }


def limits_for_sentiment(
    analysis: dict[str, float | str],
    base_limits: dict[str, float | int | str],
) -> dict[str, float | int | str]:
    sentiment_score = float(analysis.get("sentiment_score", 0.0))

    if sentiment_score <= -0.7:
        multiplier = 0.2
    elif sentiment_score <= -0.4:
        multiplier = 0.4
    elif sentiment_score <= -0.15:
        multiplier = 0.7
    else:
        multiplier = 1.0

    return {
        "max_exposure_dollars": float(base_limits["max_exposure_dollars"]) * multiplier,
        "max_trade_notional_dollars": float(base_limits["max_trade_notional_dollars"]) * max(
            multiplier,
            0.25,
        ),
        "sentiment_score": sentiment_score,
        "source": str(analysis.get("source") or "news_risk_agent"),
        "rationale": str(analysis.get("rationale") or "News sentiment adjustment."),
        "updated_at": int(time.time()),
    }


async def _ensure_wallet_bucket(js) -> None:
    try:
        await js.key_value(KV_BUCKET)
    except Exception:
        await js.create_key_value(bucket=KV_BUCKET)


if __name__ == "__main__":
    asyncio.run(main())