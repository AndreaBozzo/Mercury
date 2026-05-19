# Mercury — Roadmap

This document tracks what it would take to turn Mercury from a working proof-of-concept into something worth running with real capital. It is intentionally honest about what is simulated vs. real at each stage.

---

## Current state (v0.2)

| Component | Status | Gap |
|---|---|---|
| Nephtys L1 feed (BTC trade/ticker) | ✅ Working | Only top-of-book price, no depth |
| NATS JetStream pub/sub | ✅ Working | — |
| Moving average strategy | ✅ Working | Toy signal, no edge |
| Risk manager | ✅ Working | Virtual balance only |
| Exchange client (CCXT) | ✅ Testnet-validated | Spot testnet only; no live venue diversification |
| L2 order book | ✅ Live on BTC/USDT with snapshot bootstrap | Snapshot refresh/recovery still needs hardening |
| Arrow IPC internal transport | ✅ Built, tested | Binary path is ready; not yet the default live path |
| Bytewax stream processor | ✅ Built | Not yet replacing brain/main.py in prod |
| Backtester (Lance + Ray) | ✅ Built, tested | No historical data yet |
| Plutarch archiver | ✅ Working | ACK durability still needs tightening before long unattended runs |

**Honest summary:** Mercury can now receive a live Binance price feed, generate a moving average signal, bootstrap and maintain a BTC/USDT L2 book, pass signals through a circuit-breaker-aware risk check, and execute confirmed paper trades on Binance Spot Testnet with startup reconciliation and live executor metrics. The infrastructure path works end-to-end; the strategy still has no demonstrated edge.

---

## Active plan: 2026 Q2 (May–Jul)

The phases below remain the long-arc view. The next 2–3 months are scoped tightly around **getting to a small live-capital test safely**, on a ~5 hrs/week budget. Single theme per month, no parallel tracks, live flip is gated on a checklist (not a date).

| Month | Theme | Headline deliverables |
|---|---|---|
| **M1 — Risk & Sizing Hardening** | Replace dead code, make sizing capital-aware | Delete `executor/news_risk_agent.py`. Add `brain/risk/volatility_modulator.py` (rolling stdev → multiplier in `[0,1]`, stale → trip HALT). Add `executor/sizing.py` (fixed-fractional, balance-aware, `MAX_NOTIONAL_EUR` cap, modulator-stale → qty=0 — never last-known-value). Strategies emit *intent* (side+confidence); executor sizes. |
| **M2 — Durability & Observability** | Survive crashes, make humans hear breakage | `executor/state_store.py` (SQLite mirror of NATS KV exposure, exchange truth wins on disagreement). `core/alerts.py` (Telegram, 1 msg/min/category). Tests for `plutarch/archiver.py` and `brain/stream_processor.py` (currently 0% coverage). GitHub Actions CI (uv + ruff + pytest). |
| **M3 — Drill, Cushion, Live Flip** | Prove safety net under stress, then flip the smallest possible amount | `scripts/drill_halt.py` (synthetic vol spike + broken depth + rejected order → HALT must trip, alerts must fire, sizer must go to 0). `scripts/halt.py` (first-class kill switch). 14-day testnet soak. Mainnet via env flag, `MAX_NOTIONAL_EUR=2`, single strategy = moving_average. Compare first 10 live fills to backtester predictions. |

### M1 progress (as of 2026-05-19)

Live deposit confirmed at 9 EUR; `MAX_NOTIONAL_EUR` default raised from 2.0 to 5.5 to sit above Binance min-notional (~4.6 EUR). News risk agent kept (not deleted) and runs in parallel — writes to `risk_limits` KV; the vol modulator (Week 2) will write to a separate `vol_modulator` KV key so the two staleness budgets do not collide.

**Week 1 — done**
- `INTENT_PLACEHOLDER_QTY = 1e-8` in `core/schemas.py`; the three strategies (`brain/strategies/moving_average.py`, `brain/strategies/order_book_vwap_imbalance.py`, `brain/stream_processor.py`) emit the placeholder instead of `quantity=0.01`.
- `_refresh_runtime_state` now writes `available_balance` to the `mercury_wallet_state` KV bucket every 15 s (free balance per quote asset + ISO timestamp).
- `executor/sizing.py` MVP: reads `available_balance` + `vol_modulator` + inline `fetch_ticker`, caches Binance min-notional, returns a typed `SizingDecision` with explicit skip reasons.
- Sizer wired into `executor/main.py` between `check_risk` and `execute_trade`. `DRY_RUN` bypasses it; live mode acks and records `skipped_<reason>` on rejection.

**Plutarch hardening (M2 work pulled forward, 2026-05-19)** — review surfaced three critical bugs that would make live-incident post-mortems impossible:
- Ack-before-write replaced with ack-after-flush. SIGKILL between ack and Lance commit no longer creates silent data gaps.
- Periodic flush every `PLUTARCH_FLUSH_INTERVAL_SEC` (default 30s) in addition to batch-size (default 500). Quarantine-NDJSON fallback when Lance writes fail — consumer never wedges on a poison batch.
- Fixed Arrow schema (`source, type, timestamp, seq, payload_json, _subject, _archived_at_ns`). The inner payload is JSON-encoded so mixed event types (trade + l2_snapshot) no longer drift the schema. Backtester (`backtesting/engine.py:_payload_from_row`) decodes both new and legacy rows.
- Pull-consumer for natural backpressure. `/healthz` + `/metrics` on port 9110. Dockerfile + docker-compose entry with `plutarch-data` and `nats-data` volumes (NATS JetStream now persists to disk too — previously in-memory only).
- 5 regression tests in [tests/test_plutarch.py](tests/test_plutarch.py) covering ack ordering, quarantine path, schema drift, payload round-trip, and envelope-missing-fields.

**Week 2 — next**
- `brain/risk/volatility_modulator.py` as a coroutine task spawned by `brain/main.py`. Rolling stdev → multiplier ∈ [0, 1] published to `vol_modulator` KV.
- Flip `SIZING_REQUIRE_VOL_MODULATOR=true` once the modulator is running.

### Live Capital Flip Gate (must all be true before flipping)

- 14 consecutive days of testnet uptime, no manual interventions
- Telegram alerts verified for HALT, drift, depth desync, vol stall, order reject (5/5)
- SQLite reconciliation tested via `kill -9` drill, 3/3 passes
- Position sizer: unit tests + testnet integration test (cap, floor, modulator, balance)
- `scripts/drill_halt.py` executed within last 7 days, HALT tripped as expected
- CI green on `main`
- Binance API key: spot-trade only, withdraw disabled, IP allowlisted
- `MAX_NOTIONAL_EUR ≤ 2` for first 3 sessions
- Manual kill-switch (`scripts/halt.py`) tested end-to-end
- Wall-clock session cap enforced (auto-exit after N minutes regardless of P&L)
- NTP time-sync verified on host
- Runbook exists in `docs/runbook.md`

### Nephtys-side work (must stay generic)

Mercury depends on Nephtys but Nephtys serves more than one consumer profile, so any change must still make sense for non-crypto streams.

**Committed this window:**
- Optional WebSocket post-connect subscribe-message hook (`internal/connector/websocket.go`) — generic `on_connect_send` config field. Mercury uses it for combined Binance subscriptions; any future venue requiring auth/subscribe frames benefits.
- `UpdatePipeline` persists to KV (`internal/server/manager.go` ~L151–184) — currently transient, lost on restart. Trivial fix, high value.

**Deferred (rationale = stay generic):**
- Backpressure / slow-consumer policy — needs a 2nd consumer profile to design without coupling.
- Schema/version metadata in envelope — wait until at least one non-crypto stream lands so the envelope isn't crypto-shaped.
- End-to-end latency histograms + tracing hooks — would impose OTel unilaterally; Mercury can measure ingress→signal in `brain/observability.py` for now.

### Explicitly out of scope this window

Arrow-as-default live path, Bytewax replacing `brain/main.py` in prod, multi-symbol, multi-venue arbitrage, ML signal generation, distributed brain. These remain in Phase 4 below.

---

## Phase 1 — Make it real (4–6 weeks)

Goal: first live paper trade executed end-to-end with no human intervention.

### 1.1 Uncomment and validate exchange execution
- ✅ `create_market_buy_order` / `create_market_sell_order` are enabled behind a `DRY_RUN` flag
- ✅ Testnet round-trips verified: order placed → fill confirmed → exposure updated in NATS KV
- ✅ Startup reconciliation checks configured symbols for open exchange orders before the executor begins consuming signals

### 1.2 Add L2 market-data support
The order book module now applies raw Binance depth batches atomically:
```
wss://stream.binance.com:9443/ws/btcusdt@depth@100ms
```
Nephtys should stay generic: classify the event as `depthUpdate`, preserve the exchange timestamp and sequence, and publish the raw payload. Mercury owns the Binance-specific adapter and applies each `[U, u]` update range as one atomic order-book mutation. Bootstrap now follows Binance's local-book procedure: buffer stream updates, fetch the REST depth snapshot, discard stale buffered updates, then apply the first buffered event where `U <= lastUpdateId + 1 <= u`.

### 1.3 Prometheus metrics
The Brain and Executor now expose lightweight `/health` and `/metrics` HTTP surfaces.
- ✅ `mercury_signals_emitted_total{action, symbol}`
- ✅ `mercury_trades_executed_total{action, outcome}`
- ✅ `mercury_book_gap_total{symbol}` — tracks sequence gap events
- ✅ `mercury_position_pnl_dollars` — derived from exchange balances, fills, and mark prices
- ✅ `mercury_executor_current_exposure_dollars`
- ✅ Symbol labels are normalized to one internal form (`BTC/USDT`) across live trade, L2, and execution paths

### 1.4 Circuit breaker
✅ A `HALT` flag now lives in NATS KV. Any service can set it (e.g. on unexpected execution error or drawdown limit breach). The executor checks it before every trade. Reset still requires a manual key deletion.

---

## Phase 2 — Build an edge (6–12 weeks after Phase 1)

Goal: a strategy with a positive Sharpe ratio validated on at least 30 days of Plutarch data.

### 2.1 Accumulate data first
Let Plutarch archive for 2–4 weeks before doing any real backtesting. The current Lance schema captures raw L1 ticks. Once Nephtys L2 is live, add a separate Plutarch consumer that archives order book snapshots at 1-second intervals (much smaller than tick-by-tick L2 deltas).

### 2.2 Order book imbalance signal
This is the first genuinely microstructure-based signal:
- Compute rolling OBI from `OrderBook.imbalance(depth=5)`
- Features: OBI, OBI slope (momentum), spread normalised by mid-price
- Signal: OBI > 0.4 AND spread < 1 bps → short-term bullish; fade on reversal
- Backtest with `backtesting.ray_runner.parameter_sweep` across depth and threshold params

### 2.3 VWAP reversion
The Bytewax stream processor already has a VWAP operator. Formalise it:
- Anchor VWAP to session open (00:00 UTC for crypto)
- Signal: price > 2σ from VWAP AND volume declining → mean reversion SELL
- Exit: price returns to within 0.5σ of VWAP
- This class of signal has published evidence of edge on liquid pairs

### 2.4 Strategy ensemble
Run OBI + VWAP signals as independent voters. Only execute when both agree. This cuts trade frequency but raises precision — important while position sizing is still small.

### 2.5 Ray parameter sweep
Once 30 days of Lance data is available:
```python
from backtesting.ray_runner import parameter_sweep, grid_from_ranges
from brain.strategies.moving_average import MovingAverageStrategy

results = parameter_sweep(
    MovingAverageStrategy,
    grid_from_ranges(window=(5, 60, 5)),
    "data/history/market_history.lance",
)
# results[0] is the best-performing param set
```
Run this weekly as Plutarch accumulates more data.

---

## Phase 3 — Production hardening (3–6 months)

Goal: comfortable running Mercury unattended overnight with real (small) capital.

### 3.1 Multi-symbol
Extend Nephtys provisioning to `eth`, `sol`, `bnb`. The order book and strategy layer already key by symbol. The executor needs per-symbol position tracking and a portfolio-level exposure cap.

### 3.2 Position sizing
Replace the hardcoded `quantity=0.01` with Kelly criterion or fixed-fractional sizing based on `confidence_score`, available balance, and recent volatility.

### 3.3 Execution quality
- Track slippage: expected fill price (signal price) vs. actual fill
- Add limit order support: use LIMIT when spread > threshold, MARKET otherwise
- TWAP execution for larger orders: split into N child orders over T seconds

### 3.4 State persistence across restarts
Current NATS KV exposure tracking is volatile — a NATS restart wipes it. Back it up to a local SQLite file on every write. On startup, reconcile KV vs. SQLite vs. open exchange positions.

### 3.5 Alerting
- Telegram or email alert on: circuit breaker trigger, drawdown > 5%, any unhandled exception in executor
- Daily P&L digest

---

## Phase 4 — Scale (6+ months, if Phase 3 works)

These only make sense if Phase 2 delivered demonstrable edge.

### 4.1 Multi-venue arbitrage
Nephtys can ingest from multiple exchanges simultaneously. If OKX and Binance price the same pair differently by > fee threshold, execute a cross-venue arb. Requires CCXT multi-exchange support (already available) and a venue-aware risk manager.

### 4.2 ML signal generation
The Plutarch Lance archive is Arrow-native, which means it loads directly into PyTorch / XGBoost without conversion. Candidate model: gradient boosted tree trained on OBI, VWAP deviation, spread, tick imbalance → next-5-minute direction. Use the Ray backtester to run walk-forward validation.

### 4.3 Distributed Bytewax
When processing more symbols or adding order flow analysis, the single-process Bytewax setup becomes a bottleneck. The NATS bridge + `NatsSource` design already supports one bridge per process — add a Kubernetes Deployment with `replicas: N` and set `BRAIN_SUBJECT` per pod.

### 4.4 Direct market access
Replace CCXT with a direct FIX or native WebSocket connection to reduce execution latency from ~100ms to ~5ms. Worth doing only if the strategy's edge is sensitive to latency (most mean-reversion signals at 1-5 second horizon are not).

---

## What to never do

- **Don't automate sizing increases.** Keep position size increases manual and deliberate. Compound losses happen fast.
- **Don't skip Plutarch data collection to get to Phase 2 faster.** The edge must be validated on real data, not a toy dataset.
- **Don't add more strategies before the first one is profitable.** Two unprofitable strategies just lose money faster.
- **Don't deploy without the circuit breaker.** The overnight crash scenario is real.
