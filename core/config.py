from __future__ import annotations

import os
from math import isfinite


class ConfigurationError(ValueError):
    """Raised when an environment variable has an unsafe or invalid value."""


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} must be a boolean value")


def env_positive_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    try:
        value = default if raw is None else float(raw)
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a positive finite number") from exc

    if not isfinite(value) or value <= 0:
        raise ConfigurationError(f"{name} must be a positive finite number")
    return value
