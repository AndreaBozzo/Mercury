"""
Ray-based distributed parameter sweep for Mercury.

Why Ray over Dask or multiprocessing:
  - Ray spawns worker processes that each import Mercury as a normal Python
    package; no special serialisation of code is required.
  - `ray.init(runtime_env={"working_dir": "."})` ships the local working
    directory to every worker, making a distributed Ray cluster work without
    a separate package installation step.
  - Futures are fire-and-forget; we can saturate 100+ CPU cores with a single
    ray.get() call, then collect results in one round trip.

Fallback:
  If Ray is not installed (e.g. local dev without the full dependency set),
  the sweep runs sequentially in the current process.  Results are identical.

Example
-------
    from brain.strategies.moving_average import MovingAverageStrategy
    from backtesting.ray_runner import parameter_sweep, grid_from_ranges

    grid    = grid_from_ranges(window=(10, 61, 10))
    results = parameter_sweep(MovingAverageStrategy, grid,
                              "data/history/market_history_v2.lance")

    for r in results[:5]:
        print(r)
"""

from __future__ import annotations

import itertools
import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("ray_runner")

try:
    import ray

    _RAY_AVAILABLE = True
except ImportError:
    _RAY_AVAILABLE = False
    logger.warning("Ray not installed — parameter_sweep will run sequentially")


# ---------------------------------------------------------------------------
# Inner task — must be a plain top-level function so Ray/pickle can find it
# ---------------------------------------------------------------------------


def _run_single(
    strategy_factory: Callable,
    params: dict,
    dataset_path: str,
    initial_balance: float,
    fee_bps: float,
    slippage_bps: float,
    max_position_fraction: float,
) -> dict:
    from backtesting.engine import BacktestEngine

    return (
        BacktestEngine(
            strategy_factory,
            params,
            initial_balance,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
            max_position_fraction=max_position_fraction,
        )
        .run(dataset_path)
        .to_dict()
    )


if _RAY_AVAILABLE:

    @ray.remote
    def _ray_task(
        strategy_factory: Callable,
        params: dict,
        dataset_path: str,
        initial_balance: float,
        fee_bps: float,
        slippage_bps: float,
        max_position_fraction: float,
    ) -> dict:
        return _run_single(
            strategy_factory,
            params,
            dataset_path,
            initial_balance,
            fee_bps,
            slippage_bps,
            max_position_fraction,
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parameter_sweep(
    strategy_factory: Callable[..., Any],
    param_grid: dict[str, list],
    dataset_path: str,
    initial_balance: float = 10_000.0,
    max_concurrent: int = 100,
    *,
    fee_bps: float = 10.0,
    slippage_bps: float = 5.0,
    max_position_fraction: float = 0.10,
) -> list[dict]:
    """
    Full grid search over every combination in `param_grid`.

    Parameters
    ----------
    strategy_factory
        A class (or callable) that returns a strategy instance when called
        with **params.  Must be importable by Ray worker processes.
    param_grid
        e.g. {"window": [10, 20, 50], "buy_threshold": [0.98, 0.99, 1.00]}
        All combinations are enumerated via itertools.product.
    dataset_path
        Path to a Lance dataset produced by Plutarch.
    initial_balance
        Virtual starting balance for each isolated backtest run.
    max_concurrent
        Maximum number of Ray tasks in flight at once.  Keeps memory bounded
        when the grid has tens of thousands of combinations.
    fee_bps, slippage_bps, max_position_fraction
        Fill-model assumptions forwarded to each isolated BacktestEngine.

    Returns
    -------
    list[dict]
        BacktestResult.to_dict() for every combination, sorted by total_pnl
        descending (best-performing first).
    """
    keys = list(param_grid.keys())
    combos = [dict(zip(keys, vals)) for vals in itertools.product(*param_grid.values())]
    logger.info(
        "Parameter sweep: %d combinations, max_concurrent=%d, Ray=%s",
        len(combos),
        max_concurrent,
        _RAY_AVAILABLE,
    )

    if _RAY_AVAILABLE:
        if not ray.is_initialized():
            ray.init(
                ignore_reinit_error=True,
                # Distribute the local working directory to remote workers so
                # Mercury's modules are importable without a package install.
                runtime_env={"working_dir": "."},
            )

        results: list[dict] = []
        for i in range(0, len(combos), max_concurrent):
            batch = combos[i : i + max_concurrent]
            futures = [
                _ray_task.remote(
                    strategy_factory,
                    p,
                    dataset_path,
                    initial_balance,
                    fee_bps,
                    slippage_bps,
                    max_position_fraction,
                )
                for p in batch
            ]
            results.extend(ray.get(futures))
    else:
        results = [
            _run_single(
                strategy_factory,
                p,
                dataset_path,
                initial_balance,
                fee_bps,
                slippage_bps,
                max_position_fraction,
            )
            for p in combos
        ]

    results.sort(key=lambda r: r["total_pnl"], reverse=True)
    return results


def grid_from_ranges(
    window: tuple[int, int, int] = (10, 61, 10),
    buy_threshold: tuple[float, float, float] = (0.97, 1.00, 0.01),
    sell_threshold: tuple[float, float, float] = (1.01, 1.04, 0.01),
) -> dict[str, list]:
    """
    Build a parameter grid from (start, stop, step) range tuples.

    Defaults produce 3 × 3 × 3 = 27 combinations — a fast smoke-test grid.
    For a real sweep widen the ranges, e.g.:
        grid_from_ranges(window=(5, 100, 5))  → 19 × 3 × 3 = 171 combinations
    """
    import numpy as np

    return {
        "window": list(range(*window)),
        "buy_threshold": [round(v, 4) for v in np.arange(*buy_threshold)],
        "sell_threshold": [round(v, 4) for v in np.arange(*sell_threshold)],
    }
