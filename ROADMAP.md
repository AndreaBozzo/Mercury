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
| Exchange client (CCXT) | 🟡 Wired, commented out | Needs testnet validation |
| L2 order book | ✅ Live on BTC/USDT with snapshot bootstrap | Snapshot refresh/recovery still needs hardening |
| Arrow IPC internal transport | ✅ Built, tested | Binary path is ready; not yet the default live path |
| Bytewax stream processor | ✅ Built | Not yet replacing brain/main.py in prod |
| Backtester (Lance + Ray) | ✅ Built, tested | No historical data yet |
| Plutarch archiver | ✅ Working | ACK durability still needs tightening before long unattended runs |

**Honest summary:** Mercury can receive a live Binance price feed, generate a moving average signal, pass it through a risk check, and route it to the exchange client — but the final execution step is commented out and the strategy has no demonstrated edge. The infrastructure is solid; the alpha is still a TODO.

---

## Phase 1 — Make it real (4–6 weeks)

Goal: first live paper trade executed end-to-end with no human intervention.

### 1.1 Uncomment and validate exchange execution
- Enable `create_market_buy_order` / `create_market_sell_order` behind a `DRY_RUN` flag
- Verify testnet round-trips: order placed → fill confirmed → exposure updated in NATS KV
- Add order-state reconciliation: check open orders on startup to recover from crashes mid-trade

### 1.2 Add L2 market-data support
The order book module now applies raw Binance depth batches atomically:
```
wss://stream.binance.com:9443/ws/btcusdt@depth@100ms
```
Nephtys should stay generic: classify the event as `depthUpdate`, preserve the exchange timestamp and sequence, and publish the raw payload. Mercury owns the Binance-specific adapter and applies each `[U, u]` update range as one atomic order-book mutation. Bootstrap now follows Binance's local-book procedure: buffer stream updates, fetch the REST depth snapshot, discard stale buffered updates, then apply the first buffered event where `U <= lastUpdateId + 1 <= u`.

### 1.3 Prometheus metrics
The Brain now exposes a lightweight `/health` and `/metrics` HTTP surface for order-book readiness and sequence visibility. The next step is to round that out into service-wide Prometheus coverage:
- `mercury_signals_emitted_total{action, symbol}`
- `mercury_trades_executed_total{action, outcome}`
- `mercury_book_gap_total{symbol}` — tracks sequence gap events
- `mercury_position_pnl_dollars` — live unrealised P&L gauge

### 1.4 Circuit breaker
Add a `HALT` flag to NATS KV. Any service can set it (e.g. on unexpected execution error or drawdown limit breach). The executor checks it before every trade. Reset requires a manual key deletion.

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
