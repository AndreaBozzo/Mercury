# Mercury technical and release assessment

**Assessment date:** 2026-07-26  
**Repository baseline:** `master` at the commits recorded in Git history  
**Target:** a credible public observation alpha, followed by a research beta

## Executive assessment

Mercury now has a tested event-driven observation path, typed domain models, a real Lance archive,
observable services, fail-closed execution boundaries, and a materially stronger engineering
baseline than the repository had at the start of this review. The code is a credible
**observation alpha**. Intentional public release still requires enabling private vulnerability
reporting and one clean Docker build/run of the documented profile.

It is not a live-trading system. The most important reason is not venue selection: a successfully
submitted order can be followed by a process, NATS, or KV failure, after which JetStream may
redeliver the signal and cause a second order. No exchange substitution fixes that lifecycle
problem. Risk accounting is also non-atomic, venue timeouts do not have a durable ambiguous-order
state, and the research model still omits liquidity, latency, partial fills, and a time-series
validation protocol.

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
| Unit/component tests | 241 passed; 2 external-service tests skipped without a broker |
| Real JetStream integration | 2 passed against pinned NATS 2.14.3 |
| Runtime-only branch-aware coverage | 71% overall |
| Lowest runtime-module coverage | Bytewax 32%, provisioning 35%, Ray runner 36%, Brain entrypoint 52% |
| Ruff lint/format | Passed |
| Mypy, including unannotated runtime bodies | Passed |
| Source and wheel build | Passed |
| Locked runtime dependency audit | No known advisories reported |
| Compose render | Passed; image build not run because the local Docker daemon was unavailable |

Coverage excludes tests from the numerator/denominator and is not an acceptance threshold. The
experimental Bytewax path and operational entrypoints remain the weakest areas. The real-broker
test covers a representative Nephtys envelope through Brain, Executor observation, and Plutarch;
it does not start a real Nephtys process or prove restart recovery.

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
- Live mode is rejected before an exchange client is constructed.
- Missing, corrupt, or unreachable HALT/risk state fails closed.
- Persisted dynamic risk limits retain their producer timestamp and fail closed when stale or
  implausibly future-dated.
- Non-finite risk/config values are rejected.
- A volatility modulator is required by default for sizing, while no production publisher exists;
  this intentionally blocks accidental sandbox execution and remains defense in depth for any
  future execution work.

### Contracts and modularity

- Pydantic models define market events, trade intent, and execution results.
- Symbol normalization is centralized and accepts slash, dash, and underscore forms.
- Nephtys remains the generic connector; exchange-specific depth reconstruction lives in Mercury.
- Brain strategy selection and Executor handling are separated by a durable subject.
- Credentials are scoped to Executor configuration rather than the ingestion service.
- Explicit ACK/NAK/TERM consumers disable callback auto-ack, bound transient delivery attempts,
  and terminate structurally malformed events.
- Supported services validate existing stream subjects and own/drain their NATS connections.

### Archive behavior

- Plutarch acknowledges a batch only after a successful Lance write or durable quarantine write.
- Periodic flush prevents quiet feeds from remaining indefinitely in memory.
- The archive schema stores the payload as JSON, avoiding Arrow schema drift across event types.
- Schema v2 stores the JetStream stream sequence. The backtester deduplicates redelivery writes by
  that identity and still reads legacy archive row shapes.
- The actual Apache Lance `pylance` distribution is locked and exercised by the real-broker test.

### Engineering baseline

- Runtime packages build into source and wheel distributions.
- CI runs lint, format, mypy, tests, build, and dependency audit.
- Third-party GitHub actions and the uv version are pinned.
- The reviewed locked runtime dependency set has no known `pip-audit` advisories.
- Executor risk and mode transitions have focused regression tests.
- Local Compose bases are digest-pinned; services run unprivileged with dropped capabilities,
  read-only root filesystems, health checks, and loopback-only published ports.
- Public contribution, security, conduct, support, and pull-request policies are present.

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
| EX-03 | High | SELL sizing used quote-cash logic rather than authoritative free base inventory. | A SELL could be sized from the wrong asset and then reject or overstate executability. | **Resolved during review:** snapshots now publish per-asset free balances and sizing is side-aware. |
| EX-04 | High | Cost-basis history stopped once an early net quantity matched the present balance. | Later sell/re-buy cycles could be omitted. | **Resolved during review:** pagination continues to its terminal page and fails closed at the configured ceiling. |
| EX-05 | High | Incremental fill cursor used `last_timestamp + 1`. | A second fill in the same millisecond could be skipped. | **Resolved during review:** the cursor persists timestamp-boundary trade identities and deduplicates an inclusive overlap. |
| EX-06 | High | Venue result ambiguity is collapsed into generic failure. | A timeout after submission is not equivalent to a rejected order. Blind retry is unsafe. | Model submitted/accepted/partial/filled/rejected/unknown states explicitly. |
| EX-07 | Medium | “Dollar” exposure names actually hold quote-currency values. | EUR and non-EUR quotes can be combined or displayed incorrectly. | Use typed currency amounts and currency-specific metrics/limits. |
| EX-08 | Medium | `RISK_EUR_PER_QUOTE` was one static scalar. | It was not valid for arbitrary quote currencies and could become stale. | **Resolved for alpha:** sandbox sizing rejects non-EUR quote symbols pending a timestamped FX service. |
| EX-09 | Medium | Exchange metadata/market minimum caching needs explicit freshness behavior. | Stale precision or minimum cost can cause avoidable rejects. | Long-lived client, positive/negative cache policy, expiry, and retry limits. |
| EX-10 | Medium | Risk-limit state had no enforced freshness contract. | A previously safe dynamic limit could remain active after its producer died. | **Resolved during review:** persisted limits retain `updated_at`; stale, missing-timestamp, and implausibly future state fail closed. |

The previously unsafe HALT behavior—treating KV transport errors as “not halted”—was corrected
during this review.

### Brain, order book, and stream processing

| ID | Severity | Finding | Evidence and consequence | Disposition |
|---|---|---|---|---|
| BR-01 | High | Strategy instances held state shared across symbols. | A moving-average deque or order-book strategy could mix instruments in a multi-subject consumer. | **Resolved during review:** strategy state is keyed by canonical source and symbol. |
| BR-02 | High | Poison messages had no complete delivery policy. | Unparseable or repeatedly failing events could redeliver forever or block useful work. | **Partially resolved:** malformed messages terminate; transient failures NAK with delay and a delivery cap. A durable dead-letter stream and alert remain. |
| BR-03 | High | Experimental Bytewax input acknowledges before checkpointed processing. | A crash after the in-memory handoff can lose an event despite recovery claims. | Keep unsupported, or make source offsets/ack lifecycle checkpoint-aware. |
| BR-04 | High | Experimental Bytewax output does not await publication results. | Failed signal publishes can be silently lost. | Await/collect publish acknowledgements and propagate failure. |
| BR-05 | Medium | Coinbase ticker sequence is not promoted by Nephtys metadata inference. | Mercury can still consume price/product/time, but generic envelope `seq`/`type` semantics are weaker than Binance's. | Define a versioned generic metadata mapping or normalize in a documented adapter. |
| BR-06 | Medium | Binance snapshot task failure could remain cached. | Re-reading a failed completed task could repeatedly raise instead of scheduling recovery. | **Resolved during review:** failed tasks are cleared before propagation and the next delivery starts a fresh snapshot. |
| BR-07 | Medium | Broad JetStream setup exception handling could hide incompatible stream configuration or permissions. | A service could continue with an unusable or misconfigured stream. | **Resolved during review:** only not-found creates; effective subjects are validated in supported and experimental paths. |
| BR-08 | High | Async consumers explicitly ACKed/NAKed while callback auto-ack remained enabled. | A transient failure could be NAKed by Mercury and then auto-ACKed by the client callback wrapper, defeating redelivery. | **Resolved during review:** all explicit-ack async subscriptions set `manual_ack=True`; a real JetStream test proves NAK redelivery. |

The direct Brain path should remain the supported alpha path. Bytewax should be labeled
experimental until its acknowledgement and publication semantics match the durability claims.

### Plutarch and data quality

| ID | Severity | Finding | Evidence and consequence | Disposition |
|---|---|---|---|---|
| PL-01 | High | Valid JSON with an invalid timestamp/sequence could escape row conversion and crash the consumer. | JSON parsing succeeded, then unchecked coercion raised outside the poison boundary. | **Resolved during review:** full projection is inside per-message quarantine handling. |
| PL-02 | High | Malformed JSON was acknowledged and dropped rather than quarantined. | The evidence needed to diagnose an upstream regression was lost. | **Resolved during review:** exact raw bytes, subject, and error metadata are durably quarantined before ACK. |
| PL-03 | Medium | Health could look healthy forever before the first flush. | `last_flush_ns == 0` did not distinguish startup/no data from a stalled first buffer. | **Resolved during review:** health and metrics track the oldest buffered-event age independently of flush history. |
| PL-04 | Medium | Consumer limits and ack-pending headroom were implicit. | A higher-rate feed could exhaust pending delivery capacity or redeliver before a normal flush. | **Mostly resolved:** ACK wait, max delivery, and max ACK pending are explicit and validated; redelivery is exported. Broker pending/lag metrics remain. |
| PL-05 | Medium | Lance compaction and retention are not operated. | Fragment/version growth will degrade scans and grow storage. | Add an explicit offline/admin maintenance command and retention policy. |
| PL-06 | Medium | Quarantine writes have no external alert. | The service can remain nominally running while all new data is diverted. | Alert on first occurrence and sustained rate; document recovery. |
| PL-07 | High | The dependency named `lance` was an unrelated PyPI package. | Real `lance.write_dataset` did not exist, so every production flush was quarantined; mocks hid the failure. | **Resolved during review:** depend on Apache Lance's `pylance`, lock it, and perform real writes in CI. |
| PL-08 | High | At-least-once archive duplicates had no reliable replay identity. | A write-success/ACK-failure redelivery could be counted twice in research results. | **Resolved for schema v2:** archive the JetStream stream sequence and deduplicate it in replay; legacy rows remain inherently unidentified. |

### Backtesting and research validity

| ID | Severity | Finding | Evidence and consequence | Disposition |
|---|---|---|---|---|
| BT-01 | High | Strategy intent placeholder quantity was treated as a fill quantity. | P&L magnitude was meaningless and unrelated to configured capital. | **Resolved for the research model:** a cash ledger sizes from allocation and capital independently of intent placeholder quantity. |
| BT-02 | High | Execution costs were omitted. | Apparent performance was systematically optimistic. | **Partially resolved:** configurable fees and adverse slippage are applied; spread, latency, liquidity, partial fills, and rejects remain. |
| BT-03 | High | Engine results/state could accumulate across repeated runs. | The same engine instance could produce non-independent results. | **Resolved during review:** every run constructs fresh strategy, result, portfolio, marks, and deduplication state. |
| BT-04 | Medium | Performance statistics were too sparse for strategy claims. | A simple Sharpe-like value hid drawdown, turnover, tiny sample size, and data quality. | **Partially resolved:** report trades, exposure rate, drawdown, turnover, costs, event coverage, invalid rows, and duplicates; sensitivity and confidence intervals remain. |
| BT-05 | Medium | No enforced time-series validation protocol exists. | Parameter sweeps can overfit the same data used for reporting. | Add chronological train/validation/test and walk-forward evaluation. |

Until these are addressed, the backtester is a replay/strategy-development tool, not evidence of
an edge.

### Operations, security, and public release

| ID | Severity | Finding | Evidence and consequence | Disposition |
|---|---|---|---|---|
| OP-01 | High | There was no selected open-source licence. | Public source remained normally copyrighted and contribution terms were unclear. | **Resolved:** Mercury is licensed under Apache-2.0 with an owner attribution in `NOTICE`; contribution terms use the same licence. |
| OP-02 | Medium | Security, contribution, conduct, and support policies were absent. | Vulnerability reports and contribution expectations had no defined path. | **Resolved in source:** policies and a PR template are present. The owner must enable GitHub private vulnerability reporting before publication. |
| OP-03 | High | Compose used mutable images and root containers. | Builds could drift; a compromise had unnecessarily broad container permissions. | **Resolved in configuration:** digest-pinned bases, fixed non-root UID, read-only roots, dropped capabilities, no-new-privileges, and reduced build contexts. |
| OP-04 | Medium | Compose lacked complete health/dependency readiness and service exposure. | Startup order did not mean NATS was usable; Brain/Executor health was not mapped. | **Resolved in configuration:** health-based dependencies, service health checks, restarts, and loopback-only ports. |
| OP-05 | High | NATS has no authentication/TLS in the sample deployment. | Exposing the sample beyond loopback would expose events, risk state, and control subjects. | **Contained for alpha:** every host port binds loopback and documentation says local-only. A secured remote profile remains future work. |
| OP-06 | Medium | NATS connection ownership/cleanup was unclear. | Services received JetStream contexts without consistently closing their client. | **Resolved during review:** supported services and the Bytewax bridge own and drain sessions. |
| OP-07 | Medium | Mypy checked only typed function bodies. | Untyped legacy paths could escape analysis while CI appeared fully type-checked. | **Resolved during review:** `check_untyped_defs` is enabled for all runtime packages and provisioning. |
| OP-08 | Medium | No automated end-to-end observation test existed. | Unit tests could not prove NATS/Brain/Executor/Plutarch compatibility. | **Partially resolved:** CI runs a real-NATS representative-envelope smoke and redelivery test. A real Nephtys process and restart drill remain. |
| OP-09 | Medium | Nephtys pipeline updates are not currently persisted. | Restart can restore a stale connector configuration; the old roadmap incorrectly said this was fixed. | Track as a generic Nephtys compatibility issue; do not claim persistence. |
| OP-10 | Medium | Hardened Compose images were not built in the review environment. | Static render and Dockerfile review cannot catch every build/runtime ownership problem. | Run `docker compose build --pull` and the complete quickstart on a clean Docker daemon before tagging. |

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
with exchange-specific normalization kept at Mercury's boundary. New collections use archive
schema v2; legacy v1 archives are readable but cannot be exactly deduplicated because they did not
store a broker identity.

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

1. Enable GitHub private vulnerability reporting and run the
   [public alpha release checklist](RELEASE_CHECKLIST.md) on clean source and Docker environments.
2. Define and test a versioned Nephtys event-envelope contract, including restart persistence.
3. Add archive quarantine alerts, broker-lag metrics, compaction, retention, and provenance.
4. Add chronological splits, walk-forward evaluation, cost sensitivity, and confidence intervals.
5. Exercise Nephtys/NATS/service restart and redelivery recovery end to end.
6. Only then repair atomic sandbox execution reservations and ambiguous order reconciliation.
7. Conduct a separate live-readiness review only if live execution remains a project goal.

## Decisions required from the owner

These should not be guessed in code:

- enabling GitHub private vulnerability reporting and choosing a fallback private contact;
- whether live execution remains a long-term goal or becomes a separate private component;
- intended user: personal research tool, research framework, or deployable trading system;
- supported deployment target and operating budget;
- data retention/licensing policy for archived venue data;
- whether Bytewax remains strategic or is removed from the supported architecture.

## Review rule

Every safety-relevant change should be small enough to review, have a regression test that fails
before the fix, state its failure semantics, and be committed separately from unrelated cleanup.
No green test suite should be interpreted as permission to enable live execution.
