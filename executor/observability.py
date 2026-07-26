from __future__ import annotations

import json
from collections.abc import Mapping


def health_payload(
    current_exposure: float,
    position_pnl: float,
    *,
    nats_connected: bool = True,
) -> dict:
    return {
        "status": "ok" if nats_connected else "unavailable",
        "nats_connected": nats_connected,
        "current_exposure": current_exposure,
        "position_pnl": position_pnl,
    }


def health_http_status(payload: Mapping[str, object]) -> str:
    if payload.get("status") == "ok":
        return "200 OK"
    return "503 Service Unavailable"


def metrics_payload(
    trade_counts: Mapping[tuple[str, str], int],
    current_exposure: float,
    position_pnl: float,
    *,
    nats_connected: bool = True,
) -> str:
    lines = [
        "# HELP mercury_trades_executed_total "
        "Total trade execution outcomes observed by the executor.",
        "# TYPE mercury_trades_executed_total counter",
        "# HELP mercury_executor_current_exposure_dollars "
        "Current tracked exposure in the executor.",
        "# TYPE mercury_executor_current_exposure_dollars gauge",
        "# HELP mercury_position_pnl_dollars Current tracked position P&L in dollars.",
        "# TYPE mercury_position_pnl_dollars gauge",
        "# HELP mercury_executor_nats_connected 1 while the NATS client is connected.",
        "# TYPE mercury_executor_nats_connected gauge",
        f"mercury_executor_nats_connected {1 if nats_connected else 0}",
    ]

    for (action, outcome), count in sorted(trade_counts.items()):
        lines.append(
            "mercury_trades_executed_total"
            f'{{action="{_metric_label(action)}",outcome="{_metric_label(outcome)}"}} {count}'
        )

    lines.append(f"mercury_executor_current_exposure_dollars {current_exposure}")
    lines.append(f"mercury_position_pnl_dollars {position_pnl}")
    lines.append("")
    return "\n".join(lines)


def json_response(payload: dict) -> bytes:
    return json.dumps(payload, sort_keys=True).encode()


def _metric_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')
