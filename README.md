<div align="center">
  <img src="docs/assets/logo.png" alt="Mercury Logo" width="800" height="auto"/>
  <h1>Mercury</h1>
  <p><strong>Autonomous Trading Agent</strong></p>
</div>

---
 
 > *Named after the Roman god of commerce, financial gain, and messages — the swift messenger who bridges the gap between intelligence and action.*
 
Mercury is an autonomous cryptocurrency trading agent built using Python. It relies on a microservice architecture communicating exclusively over a NATS JetStream message broker. Mercury is designed to securely decouple the intelligence layer (which analyzes data and makes decisions) from the execution layer (which holds sensitive API keys and touches funds).

Mercury is the sister project to [Nephtys](https://github.com/AndreaBozzo/Nephtys) (the data pipeline connector). Nephtys handles connecting to exchange streams, preserving the raw event payload plus sequence metadata, and publishing it to NATS. Mercury subscribes to those streams, bootstraps a local Binance order book from REST + `depthUpdate`, and turns that market state into typed trading signals.

## Architecture

Mercury is split into two completely isolated microservices:

1. **The Brain (Intelligence Layer)**
   - Listens to tick-by-tick `MarketEvent` streams published to `nephtys.stream.crypto.*` wrappers via JetStream durable subscriptions.
   - Evaluates a configurable rolling state strategy (e.g., Simple Moving Average logic inside `brain/strategies/moving_average.py`).
   - Maintains a local BTC/USDT L2 order book by buffering live `depthUpdate` events, fetching the Binance REST depth snapshot, and applying atomic `[U, u]` update batches once the snapshot is aligned.
   - Normalizes live Binance compact symbols such as `BTCUSDT` into one internal form (`BTC/USDT`) before emitting `TradeSignal` objects, so book state, metrics, and execution all use the same symbol labels.
   - Publishes normalized `TradeSignal` objects back to NATS, enriched with `confidence_score` and `allocation_percentage`.
   - Has **NO** knowledge of any exchange API keys.

2. **The Executor (Risk & Execution Layer)**
   - Subscribes to NATS `trade.signals` via a durable consumer (so it never misses a trade, even if it goes offline temporarily).
   - Passes every incoming signal through the `risk_manager.py` risk vault.
   - Uses persistent state managed natively inside a NATS Key-Value bucket (`mercury_wallet_state`) to enforce maximum exposure, store the `HALT` circuit-breaker flag, and retain exposure state across executor restarts.
   - If a signal passes risk checks, executes the trade asynchronously against the Binance Sandbox via `ccxt.async_support`, confirms the order reached a filled/closed state, and only then records exposure.
   - Reconciles configured symbols on startup and refreshes account-derived exposure plus unrealized P&L from exchange balances, fills, and mark prices.

## Technologies Used

- **Python 3.11+**
- **uv** (Fast Python package and project manager)
- **NATS JetStream** (Message broker, durability, pub/sub, and KV store)
- **CCXT** (Cryptocurrency exchange trading library)
- **Pydantic** (Data validation and payload modeling)
- **Docker & Docker Compose**

## Setup and Running

Mercury requires `uv` to manage its dependencies and a running instance of NATS JetStream (which can be the one provided by Nephtys).

### Local Quickstart

1. **Install Dependencies**
   ```bash
   uv sync
   ```

2. **Configure Environment**
   Populate your `.env` file with your API keys:
   ```env
   BINANCE_API_KEY=your_sandbox_api_key
   BINANCE_SECRET_KEY=your_sandbox_secret_key
   NATS_URL=nats://localhost:4222
   DRY_RUN=true
   EXECUTOR_RECONCILE_SYMBOLS=BTC/USDT
   EXECUTOR_ACCOUNT_REFRESH_SECONDS=15
   ```

   Notes:
   - Keep `DRY_RUN=true` until you are ready to place Binance Spot Testnet orders.
   - `EXECUTOR_RECONCILE_SYMBOLS` accepts either `BTCUSDT` or `BTC/USDT`; Mercury normalizes both to `BTC/USDT` internally.

3. **Orchestration (Auto-provisioning Nephtys)**
   Ensure Nephtys is running. By default its admin API is on `http://localhost:3002/v1/streams`. If Nephtys is started with `NEPHTYS_ADMIN_TOKEN`, export the same value in Mercury before provisioning. Then register the required trade and depth streams via its REST API:
   ```bash
   export NEPHTYS_ADMIN_TOKEN=your-local-admin-token
   uv run python -m scripts.provision_nephtys
   ```

4. **Run Services (Terminal 1)**
   Start the Brain:
   ```bash
   uv run python -m brain.main
   ```

   The Brain exposes a small HTTP surface for live verification:
   - `GET /health` returns JSON with per-symbol book readiness, bootstrap state, and current sequence.
   - `GET /metrics` returns Prometheus-style gauges for readiness, sequence, and bootstrap activity.
   - Defaults: `http://127.0.0.1:9108/health` and `http://127.0.0.1:9108/metrics`

5. **Run Services (Terminal 2)**
   Start the Executor:
   ```bash
   uv run python -m executor.main
   ```

   The Executor also exposes a small HTTP surface:
   - `GET /health` returns JSON with `current_exposure` and `position_pnl`.
   - `GET /metrics` returns Prometheus-style trade outcome counters plus:
     - `mercury_executor_current_exposure_dollars`
     - `mercury_position_pnl_dollars`
   - Defaults: `http://127.0.0.1:9109/health` and `http://127.0.0.1:9109/metrics`

### Docker
Alternatively, you can run the entire cluster using Docker Compose:
```bash
docker-compose up -d
```
*(Note: Be sure your NATS network topology is configured correctly if you are sharing a NATS container between the Nephtys docker cluster and the Mercury docker cluster).*

## Data Contracts (Schemas)

All communication between Nephtys `->` Brain `->` Executor is strictly typed using Pydantic models defined in `core/schemas.py`.

For market data, Mercury currently consumes:
- Binance trade events for the basic signal path.
- Binance `depthUpdate` batches for the local L2 book.

Nephtys stays intentionally generic here: it classifies the event, preserves timestamp and sequence metadata, and forwards the raw payload. Mercury owns the Binance-specific depth adapter and snapshot bootstrap logic.

Across those paths, Mercury normalizes symbols at the schema boundary. A live trade event carrying `BTCUSDT` and an L2 book event carrying `BTC/USDT` will converge to the same internal symbol, which keeps metrics labels, book state, and execution reconciliation aligned.

## Plutarch — Market-Data Archive

Plutarch is the silent third service. It subscribes to every Nephtys event and persists it to a Lance dataset for replay by [backtesting/engine.py](backtesting/engine.py). It is what makes live-incident post-mortems possible.

**Durability contract:**
- Messages are acked ONLY after a successful Lance write. A crash between ack and write would have silently lost data; Plutarch prefers duplicates over gaps.
- The buffer flushes whichever fires first: `PLUTARCH_BATCH_SIZE` (default 500 events) or `PLUTARCH_FLUSH_INTERVAL_SEC` (default 30s).
- If a Lance write raises, the unflushed payloads are written to `PLUTARCH_QUARANTINE_DIR/plutarch_quarantine_<ts>.ndjson` and the consumer continues. M2 alerting will fire when `plutarch_quarantine_writes_total > 0`.

**Schema (fixed, never drifts):**

| column | type | source |
|---|---|---|
| `source` | string | Nephtys envelope |
| `type` | string | Nephtys envelope |
| `timestamp` | int64 | Nephtys envelope (epoch nanos) |
| `seq` | int64 | Nephtys broker sequence (-1 if absent) |
| `payload_json` | string | JSON-encoded inner payload |
| `_subject` | string | NATS subject the event arrived on |
| `_archived_at_ns` | int64 | Plutarch wall-clock at archive time |

**Observability:** `GET http://localhost:9110/healthz` returns `{status, events_written, write_errors, quarantine_writes, buffer_depth, last_flush_age_seconds}`. `GET /metrics` exposes Prometheus-style counters. `status=stalled` (HTTP 503) when the buffer is non-empty and no flush succeeded in 3× the flush interval.

## Position Sizing (M1)

Mercury strategies emit **intent** (side + confidence) with a placeholder quantity (`INTENT_PLACEHOLDER_QTY = 1e-8` in `core/schemas.py`). The executor's sizer (`executor/sizing.py`) converts that intent into a real order size:

```
target_quote = balance_quote × SIZING_FIXED_FRACTION × confidence × vol_multiplier
target_quote = min(target_quote, MAX_NOTIONAL_EUR_in_quote)
qty          = target_quote / mark_price
```

Fail-safe behaviour — the sizer returns `None` (and the executor skips the trade) when:

| Reason                    | Meaning                                                              |
|---------------------------|----------------------------------------------------------------------|
| `vol_modulator_stale`     | `vol_modulator` KV entry older than `VOL_MODULATOR_MAX_AGE_SECONDS`  |
| `vol_modulator_missing`   | KV key absent AND `SIZING_REQUIRE_VOL_MODULATOR=true`                |
| `balance_stale`           | `available_balance` KV entry older than `SIZING_BALANCE_MAX_AGE_SECONDS` |
| `balance_missing`         | KV key absent, or quote asset not credited                           |
| `balance_zero`            | Free balance for the quote asset is `<= 0`                           |
| `mark_price_unavailable`  | `fetch_ticker` returned no usable price                              |
| `below_min_notional`      | Computed notional below Binance exchange min                         |

The sizer is bypassed in `DRY_RUN=true` mode — the placeholder quantity goes straight through. Flip `DRY_RUN=false` to engage real sizing (sandbox or live).

### M1 smoke checklist (manual)

After `make run-executor` with `DRY_RUN=false` on testnet:

1. Wait ~30s for the portfolio refresh loop, then:
   `nats kv get mercury_wallet_state available_balance` should return `{"quote_free": {...}, "computed_at": "..."}`.
2. Inject a smoke signal:
   ```bash
   nats pub trade.signals '{"strategy_id":"smoke","symbol":"BTC/USDT","action":"BUY","order_type":"MARKET","quantity":1e-8,"confidence_score":0.5,"timestamp":"2026-05-19T12:00:00Z"}'
   ```
   Expect executor logs: `check_risk` pass → `Sized BUY BTC/USDT ...` → `Order filled on binance with id=...`.
3. Delete the modulator key to verify the fail-safe:
   `nats kv del mercury_wallet_state vol_modulator` (after setting `SIZING_REQUIRE_VOL_MODULATOR=true`) → next signal should log `skipped_vol_modulator_missing`.
4. Drop `MAX_NOTIONAL_EUR=0.5` and re-publish → expect `skipped_below_min_notional`.

The full smoke wrapper (`make smoke-m1`) is deferred to M2 alongside CI and Telegram alerting.
