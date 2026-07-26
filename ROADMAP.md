# Mercury roadmap

Mercury is being developed as a research and observation system first. Progress is gated by
evidence, not by dates or by the amount of money available to trade.

The detailed audit behind this roadmap is in [docs/ASSESSMENT.md](docs/ASSESSMENT.md).

## Current stage: observation alpha

The public alpha is useful when a new contributor can reproduce this path without credentials:

```text
public market feed -> Nephtys -> NATS -> Brain -> TradeSignal
                                      \-> Plutarch archive
TradeSignal -> Executor observe mode -> recorded decision, no exchange client or order
```

### Alpha acceptance gate

- [x] Observation is the default and cannot construct an exchange client.
- [x] Public BTC/EUR market data is the default profile.
- [x] Binance-specific ingestion is optional legacy research code.
- [x] Executor configuration distinguishes observation, sandbox, and live modes.
- [x] HALT and unavailable/corrupt risk state fail closed.
- [x] The locked runtime dependency set has no known advisories in `pip-audit`.
- [x] Lint, format, tests, type checks, build, and dependency audit run in CI.
- [x] Project status and known limitations are stated without performance claims.
- [ ] Add a licence chosen by the owner.
- [ ] Add contribution, security, conduct, and support policies.
- [ ] Pin container images, run services as non-root, and add health checks.
- [ ] Exercise the documented quickstart against a clean environment.
- [ ] Add a small deterministic end-to-end observation smoke test.
- [ ] Define a stable event-envelope compatibility/version policy with Nephtys.
- [ ] Quarantine malformed-but-valid JSON in Plutarch without crashing the consumer.

No alpha acceptance item requires sandbox or live trading.

## Next stage: research beta

The beta should support repeatable data research. It is not a live-trading milestone.

### Data integrity

- Detect and surface feed stalls, sequence gaps, reconnects, stale observations, and archive lag.
- Give Plutarch an explicit poison-message policy, bounded redelivery, quarantine alerts, and
  retention/compaction operations.
- Verify that archived data can be replayed deterministically into strategy state.
- Record provenance: venue, product, feed/channel, receive time, source time, sequence semantics,
  schema version, and software version.
- Measure completeness over a fixed observation window and publish the report.

### Strategy isolation and evaluation

- Keep strategy state per symbol and per venue; never share a rolling window across instruments.
- Replace placeholder quantities in backtests with a capital-aware fill model.
- Model fees, spread, slippage, latency, rejected fills, and base/quote balances.
- Reset engine state between runs and make repeated runs deterministic.
- Use train/validation/test time splits and walk-forward evaluation.
- Report drawdown, turnover, exposure, trade count, fee sensitivity, and data coverage alongside
  Sharpe-like metrics.
- Treat the current moving-average and order-book strategies as examples until out-of-sample
  evidence says otherwise.

### Operations and public collaboration

- Publish an architecture decision record for the direct Brain path versus experimental Bytewax.
- Add runbooks for NATS loss, feed loss, archive failure, HALT, recovery, and credential rotation.
- Expose and document health/metrics for every service in the supported deployment path.
- Add integration tests for NATS redelivery, restart recovery, and archive replay.
- Publish release notes and compatibility requirements for Mercury and Nephtys.

## Future stage: execution research

Execution research starts only after the beta data path is trustworthy. A CCXT sandbox is useful
for request-shape testing, but it is not evidence of realistic matching, liquidity, or legal
availability.

### Sandbox gate

- [ ] Select a venue only after checking the current ESMA register, permitted services, target
  jurisdiction, product availability, and the operator's account eligibility.
- [ ] Document the sandbox's actual semantics and limitations.
- [ ] Use one long-lived, rate-limited exchange client with explicit timeouts.
- [ ] Track authoritative free base and quote balances; size BUY and SELL independently.
- [ ] Make risk reservations and exposure updates atomic under concurrent signals.
- [ ] Reconcile balances, open orders, and fills without truncating trade history.
- [ ] Handle multiple fills sharing one exchange timestamp.
- [ ] Reject stale market price, balance, volatility, risk-limit, and exchange metadata inputs.
- [ ] Run failure drills for rejected orders, timeouts, partial fills, NATS loss, and restarts.

## Live execution: out of release scope

The repository recognizes `EXECUTION_MODE=live` so that an unsafe request fails explicitly before
an exchange client is constructed. Real-order execution is disabled in this release.

At minimum, all of the following are required before a separate live-readiness review:

- A durable execution journal keyed by a stable signal ID and exchange client-order ID.
- Exactly-once *decision semantics*: redelivery must never blindly submit a second order.
- An explicit ambiguous-order state that trips HALT and requires reconciliation.
- Durable, atomic risk reservations before submission and settlement after fills.
- Correct partial-fill, cancel, reject, fee, and precision handling.
- Exchange-truth reconciliation tested across process death at every order lifecycle boundary.
- A manual kill switch that has been drilled, plus tested automatic session and loss limits.
- Least-privilege keys, withdrawal disabled, IP restrictions where supported, secret rotation, and
  incident response procedures.
- Independent legal/compliance and operational review for the operator and chosen venue.
- A new threat model and code review dedicated to the live path.

Passing these engineering checks would make the software less unsafe; it would not demonstrate a
profitable strategy or make trading appropriate.

## Deferred until evidence justifies complexity

- Multi-symbol and multi-venue execution
- Arbitrage
- Machine-learned signals
- Distributed Bytewax deployment
- Direct venue/FIX execution
- Automated capital increases

Each adds failure modes and should be proposed with a measured need, an owner, tests, and rollback
criteria.
