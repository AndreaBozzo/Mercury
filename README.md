<div align="center">
  <img src="docs/assets/logo.png" alt="Mercury Logo" width="800" height="auto"/>
  <h1>Mercury</h1>
  <p><strong>Crypto market research and observation system</strong></p>
</div>

Mercury is an early-stage, event-driven system for ingesting crypto market data, generating
typed strategy signals, archiving events, and experimenting with execution controls.

> **Project status: research alpha.** The default configuration is observation-only: it creates
> no exchange client, makes no private exchange calls, and submits no orders. Mercury has no
> demonstrated trading edge and is not ready for unattended or live-capital use. Nothing in this
> repository is financial, legal, or investment advice.

Mercury is the sister project to [Nephtys](https://github.com/AndreaBozzo/Nephtys), the generic
real-time connector layer. Nephtys connects to upstream feeds and publishes durable events to
NATS JetStream; Mercury owns the trading-domain schemas, strategies, risk controls, archive, and
execution experiments.

## Current safety boundary

| Mode | Exchange client | Orders | Intended use |
|---|---:|---:|---|
| `observe` (default) | No | No | Public alpha, feed and signal verification |
| `sandbox` | Yes, explicitly named | Test venue only | Integration experiments where the venue has a genuine CCXT sandbox |
| `live` | Yes, explicitly named | Real | Implemented behind two explicit gates, but **not release-ready** |

The existence of `live` code is not evidence that it is safe to use. Open blockers include
execution idempotency, atomic risk accounting, correct sell-side sizing, realistic backtesting,
and operational kill-switch drills. See [ROADMAP.md](ROADMAP.md) and
[docs/ASSESSMENT.md](docs/ASSESSMENT.md).

## EU and Italy venue context

MiCA's remaining transitional periods ended by July 2026. An execution venue must be checked
against the current [ESMA MiCA register](https://www.esma.europa.eu/esmas-activities/digital-finance-and-innovation/markets-crypto-assets-regulation-mica)
and its authorisation must cover the required services and jurisdiction. Italy's transition and
cross-border rules are summarized by
[Banca d'Italia](https://www.bancaditalia.it/compiti/vigilanza/accesso-mercato/soggetti-mercato-cripto-attivita/casp/faq-casp/index.html?com.dotmarketing.htmlpage.language=1).

Mercury therefore does not treat “supported by CCXT” as “legally available.” Venue authorisation
can change and is deliberately an operator decision. The default public market-data profile uses
an unauthenticated Coinbase BTC/EUR ticker; the old Binance BTC/USDT trade and depth feeds remain
available only as an explicitly selected legacy research profile.

## Architecture

```text
upstream feed
    │
    ▼
Nephtys ──► NATS JetStream ──► Brain ──► trade.signals ──► Executor
                 │                                           │
                 └────────────► Plutarch archive             └─ observe by default
                                      │
                                      ▼
                                  Backtester
```

- **Brain** consumes normalized `MarketEvent` envelopes and emits `TradeSignal` intent. The
  default moving-average strategy is illustrative, not profitable evidence.
- **Executor** checks HALT and risk state before handling signals. Observation mode records the
  outcome without constructing a CCXT exchange. Sandbox/live modes require explicit configuration.
- **Plutarch** archives raw event envelopes to Lance with ack-after-write behavior and an NDJSON
  quarantine fallback.
- **Backtesting** replays Lance rows through the same strategy interface used by the Brain. Its
  current fill/P&L model is intentionally basic and is not suitable for validating an edge.

The services communicate through NATS rather than sharing API keys or process memory. Exchange
credentials belong only in the Executor environment.

## Quickstart: observation alpha

Requirements:

- Python 3.11
- [uv](https://docs.astral.sh/uv/)
- NATS with JetStream
- a running [Nephtys](https://github.com/AndreaBozzo/Nephtys) admin API connected to the same NATS

1. Install the locked environment:

   ```bash
   uv sync --locked
   ```

2. Create local configuration:

   ```bash
   cp .env.example .env
   ```

   On PowerShell, use `Copy-Item .env.example .env`.

   Keep `EXECUTION_MODE=observe`. No exchange credentials are needed.

3. Start NATS if you do not already have one:

   ```bash
   docker compose up -d nats
   ```

4. Start Nephtys against `nats://localhost:4222`, then register Mercury's default public feed:

   ```bash
   uv run python -m scripts.provision_nephtys
   ```

5. Run the services in separate terminals:

   ```bash
   uv run python -m brain.main
   uv run python -m executor.main
   uv run python -m plutarch.archiver
   ```

6. Inspect health and metrics:

   - Brain: `http://127.0.0.1:9108/health` and `/metrics`
   - Executor: `http://127.0.0.1:9109/health` and `/metrics`
   - Plutarch: `http://127.0.0.1:9110/healthz` and `/metrics`

The first moving-average signal appears only after its rolling window is populated. In observation
mode that signal is risk-checked and marked `observed`; it is never sent to an exchange.

## Market-data profiles

Set `MERCURY_MARKET_DATA_PROFILE` before running the provisioning script.

| Profile | Default | Feed | Purpose |
|---|---:|---|---|
| `coinbase_btc_eur` | Yes | Public BTC/EUR ticker | EU-oriented L1 observation path |
| `binance_btc_usdt` | No | Public trade + `depthUpdate` | Legacy L1/L2 research and regression path |

Provisioning is idempotent by stream ID. A `409` means the existing Nephtys stream is retained;
it does not prove that its configuration matches the current profile. Remove or rename stale
streams deliberately when migrating profiles.

## Execution configuration

Observation mode needs only:

```env
EXECUTION_MODE=observe
```

Sandbox/live additionally require:

```env
EXECUTION_EXCHANGE_ID=<ccxt-id>
EXCHANGE_API_KEY=...
EXCHANGE_SECRET=...
EXCHANGE_PASSWORD=...  # only when required by the venue
```

`sandbox` fails closed if CCXT cannot enable a sandbox for that venue. Some exchange “sandboxes”
return static mock responses rather than simulated matching, so a successful API response is not
equivalent to a realistic test.

`live` also requires this exact acknowledgement:

```env
LIVE_TRADING_CONFIRMATION=I_UNDERSTAND_REAL_FUNDS_ARE_AT_RISK
```

Do not enable it for the current alpha. The repository still lacks safeguards listed in the
roadmap.

## Development checks

```bash
make check
```

CI enforces:

- Ruff lint and formatting
- mypy across all runtime packages
- the full pytest suite
- source and wheel builds
- a vulnerability audit of locked runtime dependencies

Useful individual commands:

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy core brain executor plutarch backtesting
uv run pip-audit
uv build
```

## Known limitations

- Strategy output has no demonstrated positive expectancy.
- Backtests omit realistic fees, spread, slippage, latency, and capital-aware fills.
- A signal redelivered after a partially completed live order can still create an ambiguous or
  duplicate execution.
- Sell sizing currently lacks authoritative free-base-asset accounting.
- Portfolio cost-basis reconstruction can be wrong for long or same-timestamp trade histories.
- The optional Bytewax path acknowledges input before checkpointed processing and does not await
  signal publication; it is experimental.
- Public-release governance files and a project licence still require an owner decision.

## Licence

No open-source licence has been selected yet. Until the owner chooses and adds one, normal
copyright restrictions apply. Licence selection is a blocker for a deliberate public release.
