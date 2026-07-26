# Mercury technical and release assessment

**Assessment date:** 2026-07-26  
**Repository baseline:** `master` at the commits recorded in Git history  
**Target:** a credible public observation alpha, followed by a research beta

## Executive assessment

Mercury has a useful event-driven skeleton, typed domain models, a real archive path, observable
services, and substantially better tests than a typical trading proof of concept. It is suitable
to become a public **observation alpha** once the remaining public-project and deployment gates are
closed.

It is not a live-trading system. The most important reason is not venue selection: a successfully
submitted order can be followed by a process, NATS, or KV failure, after which JetStream may
redeliver the signal and cause a second order. No exchange substitution fixes that lifecycle
problem. Risk accounting is also non-atomic, SELL sizing does not use an authoritative base-asset
balance, and portfolio reconstruction and backtesting can report misleading results.

The recommended product boundary is therefore:

1. Make public market observation, signal generation, and archiving reproducible.
2. Establish data quality and realistic research methodology.
3. Treat sandbox execution as a later integration-testing capability.
4. Keep live execution outside the supported release until a separate readiness review.

## What was inspected

The review covered:

- package, dependency, CI, container, and public-repository configuration;
- schemas, symbol normalization, NATS/JetStream setup, Arrow transport, and order-book logic;
- the direct Brain consumer and the optional Bytewax processor;
- risk checks, sizing, exchange interaction, portfolio reconciliation, and observations;
- Plutarch buffering, acknowledgement, Lance writes, quarantine, health, and metrics;
- replay/backtesting and Ray parameter sweeps;
- Nephtys integration assumptions and its current generic WebSocket envelope behavior;
- current EU/Italy crypto-asset service-provider context.

The source review was paired with the automated test suite, lint, formatting, mypy, build, branch
coverage, and a locked runtime dependency audit. Automated checks are evidence against known
regressions, not proof of trading safety.

### Measured baseline

| Check | Result on 2026-07-26 |
|---|---|
| Tests | 182 passed |
| Branch-aware coverage | 80% overall |
| Lowest runtime-module coverage | provisioning 34%, Ray runner 36%, Plutarch 41%, Brain entrypoint 46% |
| Ruff lint/format | Passed |
| Mypy configured runtime packages | Passed |
| Source and wheel build | Passed |
| Locked runtime dependency audit | No known advisories reported |

Coverage includes test modules in the denominator and is not an acceptance threshold. The weak
runtime entrypoints are more important than the aggregate percentage and should drive the next
integration tests.

## Regulatory and venue boundary

MiCA transitional operation ended no later than 1 July 2026 across the EU. Banca d'Italia's CASP
FAQ says Italy's transitional period ended no later than 30 June 2026 and describes cross-border
operation through the MiCA notification/passport mechanism.

The authoritative operational check is the current
[ESMA MiCA register](https://www.esma.europa.eu/esmas-activities/digital-finance-and-innovation/markets-crypto-assets-regulation-mica),
including the authorised services and member states for the exact legal entity. Italy-specific
context is available from
[Banca d'Italia](https://www.bancaditalia.it/compiti/vigilanza/accesso-mercato/soggetti-mercato-cripto-attivita/casp/faq-casp/index.html?com.dotmarketing.htmlpage.language=1).
The register is living data; this repository must not encode a statement that a named venue is
permanently authorised, available to a particular user, or suitable for a product.

Consequences for Mercury:

- “Implemented by CCXT” is a technical fact, not a compliance decision.
- Public unauthenticated data ingestion and authenticated order execution are separate choices.
- Product, derivative/spot status, stablecoin availability, residency, account entity, and the
  specific authorised service all matter.
- Venue choice belongs in operator configuration and a dated deployment record, not core strategy
  logic.
- Coinbase BTC/EUR is a reasonable public L1 interoperability default, not an endorsement or a
  pre-approval for execution.
- The Binance BTC/USDT adapter is useful regression/research code, but no longer defines the
  product.

This section records engineering constraints and primary-source context; it is not legal advice.

## Verified strengths

### Safety boundary

- `EXECUTION_MODE=observe` is the default.
- Observation mode constructs no CCXT exchange and performs no private exchange calls.
- Sandbox and live require an explicit exchange ID and credentials.
- Live additionally requires an exact acknowledgement string.
- Missing, corrupt, or unreachable HALT/risk state fails closed.
- Non-finite risk/config values are rejected.
- A volatility modulator is required by default for sizing, while no production publisher exists;
  this intentionally blocks accidental sandbox/live execution.

### Contracts and modularity

- Pydantic models define market events, trade intent, and execution results.
- Symbol normalization is centralized and accepts slash, dash, and underscore forms.
- Nephtys remains the generic connector; exchange-specific depth reconstruction lives in Mercury.
- Brain strategy selection and Executor handling are separated by a durable subject.
- Credentials are scoped to Executor configuration rather than the ingestion service.

### Archive behavior

- Plutarch acknowledges a batch only after a successful Lance write or durable quarantine write.
- Periodic flush prevents quiet feeds from remaining indefinitely in memory.
- The archive schema stores the payload as JSON, avoiding Arrow schema drift across event types.
- The backtester reads both current and legacy archive row shapes.

### Engineering baseline

- Runtime packages build into source and wheel distributions.
- CI runs lint, format, mypy, tests, build, and dependency audit.
- Third-party GitHub actions and the uv version are pinned.
- The reviewed locked runtime dependency set has no known `pip-audit` advisories.
- Executor risk and mode transitions have focused regression tests.

## Findings and disposition

Severity means:

- **Critical:** can duplicate real orders, bypass a safety boundary, or materially misstate safety.
- **High:** can corrupt research/execution state, lose evidence, or stop a core service.
- **Medium:** material reliability, operability, or public-release quality gap.
- **Low:** maintainability or clarity issue with limited immediate impact.

### Execution and risk

| ID | Severity | Finding | Evidence and consequence | Disposition |
|---|---|---|---|---|
| EX-01 | Critical | Live submission is not idempotent across redelivery. | An order may reach the venue before result/KV update/ack. JetStream can redeliver and submit again. | Block supported live use; add a durable execution journal, client-order IDs, and ambiguous-state reconciliation. |
| EX-02 | Critical | Exposure check/update is not an atomic reservation. | Concurrent signal handlers can read the same exposure and both pass the cap. | Reserve risk atomically before submission and settle against fills. |
| EX-03 | High | SELL sizing uses quote-cash logic rather than authoritative free base inventory. | A SELL can be sized from the wrong asset and then reject or overstate executability. | Store per-asset free balances and implement side-aware sizing before sandbox acceptance. |
| EX-04 | High | Cost-basis history can be incomplete. | Initial pagination may stop once early net quantity reaches the present balance, missing later sell/re-buy cycles. | Reconstruct from complete bounded history or persist a fill ledger. |
| EX-05 | High | Incremental fill cursor can skip trades. | `last_timestamp + 1` loses fills sharing the same millisecond. | Cursor by exchange trade ID plus timestamp, with overlap and deduplication. |
| EX-06 | High | Venue result ambiguity is collapsed into generic failure. | A timeout after submission is not equivalent to a rejected order. Blind retry is unsafe. | Model submitted/accepted/partial/filled/rejected/unknown states explicitly. |
| EX-07 | Medium | “Dollar” exposure names actually hold quote-currency values. | EUR and non-EUR quotes can be combined or displayed incorrectly. | Use typed currency amounts and currency-specific metrics/limits. |
| EX-08 | Medium | `RISK_EUR_PER_QUOTE` is one static scalar. | It is not valid for arbitrary quote currencies and can become stale. | Restrict execution to EUR quote or add a timestamped conversion service. |
| EX-09 | Medium | Exchange metadata/market minimum caching needs explicit freshness behavior. | Stale precision or minimum cost can cause avoidable rejects. | Long-lived client, positive/negative cache policy, expiry, and retry limits. |
| EX-10 | Medium | Risk-limit state has no enforced freshness contract. | A previously safe dynamic limit can remain active after its producer dies. | Timestamp, validate, and fail closed on stale dynamic state. |

The previously unsafe HALT behavior—treating KV transport errors as “not halted”—was corrected
during this review.

### Brain, order book, and stream processing

| ID | Severity | Finding | Evidence and consequence | Disposition |
|---|---|---|---|---|
| BR-01 | High | Strategy instances hold state shared across symbols. | A moving-average deque or order-book strategy can mix instruments in a multi-subject consumer. | Allocate strategy state by canonical venue/product key. |
| BR-02 | High | Poison messages have no complete dead-letter policy. | Unparseable or repeatedly failing events can redeliver forever or block useful work. | Bound delivery attempts, NAK deliberately, quarantine with context, alert. |
| BR-03 | High | Experimental Bytewax input acknowledges before checkpointed processing. | A crash after the in-memory handoff can lose an event despite recovery claims. | Keep unsupported, or make source offsets/ack lifecycle checkpoint-aware. |
| BR-04 | High | Experimental Bytewax output does not await publication results. | Failed signal publishes can be silently lost. | Await/collect publish acknowledgements and propagate failure. |
| BR-05 | Medium | Coinbase ticker sequence is not promoted by Nephtys metadata inference. | Mercury can still consume price/product/time, but generic envelope `seq`/`type` semantics are weaker than Binance's. | Define a versioned generic metadata mapping or normalize in a documented adapter. |
| BR-06 | Medium | Binance snapshot task failure can remain cached. | Re-reading a failed completed task can repeatedly raise instead of scheduling recovery. | Clear failed bootstrap tasks and test recovery/reconnect. |
| BR-07 | Medium | Broad JetStream setup exception handling can hide incompatible stream configuration or permissions. | A service can continue with an unusable or misconfigured stream. | Catch only “already exists,” then validate the effective stream/consumer config. |

The direct Brain path should remain the supported alpha path. Bytewax should be labeled
experimental until its acknowledgement and publication semantics match the durability claims.

### Plutarch and data quality

| ID | Severity | Finding | Evidence and consequence | Disposition |
|---|---|---|---|---|
| PL-01 | High | Valid JSON with an invalid timestamp/sequence can escape row conversion and crash the consumer. | JSON parsing succeeds, then unchecked coercion raises outside the poison handling boundary. | Validate the full envelope and quarantine the individual message. |
| PL-02 | High | Malformed JSON is acknowledged and dropped rather than quarantined. | The evidence needed to diagnose an upstream regression is lost. | Persist raw bytes plus subject/error metadata before ack. |
| PL-03 | Medium | Health can look healthy forever before the first flush. | `last_flush_ns == 0` does not distinguish startup/no data from a stalled writer. | Expose readiness separately and track last receive/attempt/success times. |
| PL-04 | Medium | Consumer limits and ack-pending headroom are implicit. | A higher-rate feed can exhaust pending delivery capacity without a useful early signal. | Configure limits and export pending/redelivery/lag metrics. |
| PL-05 | Medium | Lance compaction and retention are not operated. | Fragment/version growth will degrade scans and grow storage. | Add an explicit offline/admin maintenance command and retention policy. |
| PL-06 | Medium | Quarantine writes have no external alert. | The service can remain nominally running while all new data is diverted. | Alert on first occurrence and sustained rate; document recovery. |

### Backtesting and research validity

| ID | Severity | Finding | Evidence and consequence | Disposition |
|---|---|---|---|---|
| BT-01 | High | Strategy intent placeholder quantity is treated as a fill quantity. | P&L magnitude is essentially meaningless and unrelated to configured capital. | Add a portfolio/fill model and separate signal intent from fills. |
| BT-02 | High | Fees, spread, slippage, latency, liquidity, and rejects are omitted. | Apparent performance is systematically optimistic. | Add conservative configurable execution costs and sensitivity reports. |
| BT-03 | High | Engine results/state can accumulate across repeated runs. | The same engine instance may produce non-independent results. | Reset per run or make each run construct immutable state. |
| BT-04 | Medium | Performance statistics are too sparse for strategy claims. | A simple Sharpe-like value can hide drawdown, turnover, tiny sample size, and regime dependence. | Add coverage, trades, exposure, drawdown, turnover, costs, and confidence intervals. |
| BT-05 | Medium | No enforced time-series validation protocol exists. | Parameter sweeps can overfit the same data used for reporting. | Add chronological train/validation/test and walk-forward evaluation. |

Until these are addressed, the backtester is a replay/strategy-development tool, not evidence of
an edge.

### Operations, security, and public release

| ID | Severity | Finding | Evidence and consequence | Disposition |
|---|---|---|---|---|
| OP-01 | High | There is no selected open-source licence. | Public source remains normally copyrighted and contribution terms are unclear. | Owner selects and adds a licence before intentional release. |
| OP-02 | Medium | Security, contribution, conduct, and support policies are absent. | Vulnerability reports and contribution expectations have no defined path. | Add public governance files with owner-controlled contact details. |
| OP-03 | High | Compose uses mutable images and root containers. | Builds can drift; a compromise has unnecessarily broad container permissions. | Pin immutable bases/images, add non-root users and read-only/minimal filesystems. |
| OP-04 | Medium | Compose lacks complete health/dependency readiness and service exposure. | Startup order does not mean NATS is usable; Brain/Executor health is not mapped. | Add health checks and documented readiness semantics. |
| OP-05 | High | NATS has no authentication/TLS in the sample deployment. | Exposing the sample beyond loopback would expose events, risk state, and control subjects. | Make local-only scope explicit; provide a secured deployment profile later. |
| OP-06 | Medium | NATS connection ownership/cleanup is unclear. | Services receive JetStream contexts without consistently closing their underlying connections. | Return an owned client/session abstraction and close on shutdown. |
| OP-07 | Medium | Mypy checks only typed function bodies. | Untyped legacy paths can escape analysis while CI appears fully type-checked. | Incrementally type runtime entrypoints, then enable `check_untyped_defs`. |
| OP-08 | Medium | No automated end-to-end observation test exists. | Unit tests cannot prove Nephtys/NATS/Brain/Executor/Plutarch compatibility. | Add a deterministic integration fixture and smoke workflow. |
| OP-09 | Medium | Nephtys pipeline updates are not currently persisted. | Restart can restore a stale connector configuration; the old roadmap incorrectly said this was fixed. | Track as a generic Nephtys compatibility issue; do not claim persistence. |

## Alpha definition

“Alpha” means the repository is honest, installable, observable, and useful for collecting and
replaying public market data. It does **not** mean:

- safe for funds;
- legally cleared for a particular operator;
- profitable;
- complete market-data fidelity;
- production availability or durability.

The alpha-supported path is the direct Brain service with Executor in observation mode. Coinbase
BTC/EUR is the default example. Other public feeds can be added through generic Nephtys profiles,
with exchange-specific normalization kept at Mercury's boundary.

## Beta definition

“Beta” means a researcher can collect a versioned dataset, quantify its completeness, replay it
deterministically, run a cost-aware experiment without cross-symbol state leakage, and reproduce
the report. It still does not imply supported live execution.

The beta acceptance report should include:

- exact Mercury and Nephtys revisions;
- feed/profile and venue legal entity;
- collection start/end and outage intervals;
- event counts, gap/reconnect/redelivery/quarantine rates, archive lag, and clock assumptions;
- train/validation/test boundaries;
- fee/slippage/latency assumptions and sensitivity;
- trade count, turnover, drawdown, exposure, and out-of-sample performance;
- known deviations from production venue behavior.

## Recommended sequence

1. Close public alpha governance and reproducible quickstart gaps.
2. Fix Plutarch poison handling and service readiness.
3. Isolate Brain strategy state by instrument and define poison-message behavior.
4. Repair the backtest accounting model before collecting performance claims.
5. Add end-to-end restart/redelivery/data-quality tests.
6. Only then repair the sandbox execution lifecycle and side-aware accounting.
7. Conduct a separate live-readiness review if live execution remains a project goal.

## Decisions required from the owner

These should not be guessed in code:

- open-source licence;
- security-reporting and maintainer contact;
- whether live execution remains a long-term goal or becomes a separate private component;
- intended user: personal research tool, research framework, or deployable trading system;
- supported deployment target and operating budget;
- data retention/licensing policy for archived venue data;
- whether Bytewax remains strategic or is removed from the supported architecture.

## Review rule

Every safety-relevant change should be small enough to review, have a regression test that fails
before the fix, state its failure semantics, and be committed separately from unrelated cleanup.
No green test suite should be interpreted as permission to enable live execution.
