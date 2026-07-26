# Public observation alpha release record

- **Status:** release candidate; not yet tagged or published
- **Evidence date:** 2026-07-26
- **Mercury runtime revision:** `c81b965`
- **Nephtys revision:** `485ecbb7beb7f0d46d58744941f21bcb981beef8`
- **Broker:** NATS 2.14.3 with JetStream
- **Envelope contract:** [NME/1](NEPHTYS_COMPATIBILITY.md)

## Supported scope

This candidate supports public market-data observation through Nephtys, direct Brain signal
research, Executor observation records, Lance archiving, and deterministic replay/backtesting.
Coinbase BTC/EUR is the default interoperability profile and needs no exchange credentials.

`EXECUTION_MODE=live` is disabled and fails before an exchange client is created. Sandbox execution
is experimental request-shape testing, not realistic matching or a supported release path. Nothing
in this release is trading, legal, or investment advice.

## Verification record

- The portable source gate passed with Ruff, formatting, mypy over 31 source files, 258
  unit/component tests, a locked-dependency audit with no known advisories, and successful source
  and wheel builds.
- The two real-JetStream tests passed repeatedly against a non-empty Nephtys-owned stream. They use
  one-use subjects, and their durable consumers were removed after each run.
- An exact Nephtys binary from the revision above registered the documented
  `coinbase_btc_eur_ticker` profile, connected to Coinbase's public WebSocket, and published real
  BTC/EUR observations.
- Brain's durable reached the broker's latest stream sequence with zero pending or ACK-pending
  observations.
- A clean Plutarch durable archived 76 rows, including 72 real Coinbase rows, with 76 unique
  JetStream stream identities, zero redeliveries, zero write/quarantine errors, and no duplicate
  identities.
- After an abrupt Nephtys process restart, Nephtys restored one connector from its persisted KV
  configuration without reprovisioning, reconnected to Coinbase, and Plutarch resumed archiving.
- A deliberately drifted Plutarch durable with a two-second persisted ACK wait was rejected at
  startup when the process requested 30 seconds. Brain, Executor, the Bytewax bridge, and Plutarch
  now reject correctness-relevant durable-consumer drift rather than accepting ignored settings.
- The final Brain, Executor, Plutarch, and NATS images were rebuilt from `c81b965` with pull and no
  cache. On a fresh broker, the packaged services created only the `NEPHTYS` stream, archived
  20/20 controlled events with unique broker identities, emitted one BUY, and recorded it as
  `observed`. Brain and Executor persisted the requested 120-second ACK wait and 1,000-message
  pending headroom; Plutarch persisted its requested 30 seconds and 100 messages. All three
  application containers recovered against those consumers after restart. The broader hardened
  Compose drill also covered 60 synthetic events and a NATS outage/recovery cycle.

The live interoperability drill did not wait for a market-dependent strategy threshold. Synthetic
real-broker coverage separately proves Brain-to-Executor observation and guarantees that no exchange
client or order is created in observation mode.

## Nephtys compatibility and migration

Nephtys owns the `NEPHTYS` JetStream stream and its `nephtys.stream.>` subject. Mercury reuses that
stream and filters its consumers to `nephtys.stream.crypto.*`; it must not create a narrower,
overlapping stream. NATS correctly rejects overlapping subjects across streams.

Pre-candidate deployments may contain Mercury's old `nephtys_crypto` stream. Preserve or export any
needed data before a deliberate migration. The old stream must not coexist with the Nephtys-owned
broad stream, and Mercury never deletes it automatically.

Durable consumer settings persist in NATS. Changing a filter, ACK timing, delivery cap, or delivery
mode requires a deliberate durable migration or a new durable name. Services fail closed and name
the incompatible fields when persisted settings differ from requested correctness settings.

## Known alpha limitations

- Nephtys persists registered connector configurations, but runtime pipeline updates remain
  transient. Restart can restore the registered connector while losing a later pipeline update.
- Nephtys currently emits Coinbase ticker envelopes as `type=websocket_message` and omits `seq`
  when no source sequence is inferred. Plutarch uses `-1` as the missing-source-sequence archive
  sentinel. Mercury still receives the normalized `BTC-EUR` symbol, price, source timestamp,
  subject, and non-null broker sequence.
- Feed-stall, reconnect, broker-lag, and archive-lag measurement is not yet complete.
- Lance compaction, retention, quarantine alerting, and published-dataset redistribution policy are
  not operated.
- The optional Bytewax path still acknowledges before a checkpointed handoff and does not await
  output publication; it remains experimental.
- Backtests remain a research aid. They do not yet model latency, liquidity, partial fills, rejects,
  or a mandatory chronological/walk-forward validation protocol.
- The sample NATS deployment has no authentication or TLS and is restricted to loopback use.
- GitHub private vulnerability reporting, branch protection, repository collaboration settings,
  final CI on the tagged revision, artifact checksums, and the release tag remain owner actions.

## EU venue boundary

The default public feed is an interoperability choice, not an execution endorsement. Before using
any authenticated service, check the exact operator, service, member states, product, and account
eligibility in the current
[ESMA MiCA register](https://www.esma.europa.eu/esmas-activities/digital-finance-and-innovation/markets-crypto-assets-regulation-mica).
The register is living data; this release does not claim that Binance, Coinbase, or any other venue
is permanently permitted or forbidden for every EU user or service.
