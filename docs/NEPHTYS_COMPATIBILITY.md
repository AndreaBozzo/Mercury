# Mercury–Nephtys compatibility contract

This document defines the supported alpha boundary between Mercury and Nephtys. It is a wire and
operations contract, not an endorsement of any upstream venue.

## Contract identifier

The current contract is **NME/1** (Nephtys Market Event, major version 1).

NME/1 is identified by the paired software release record rather than an on-wire field. A future
major contract should add an explicit envelope version without changing the meaning of existing
fields silently. Until then, every dataset or deployment record must retain the exact Mercury and
Nephtys revisions.

## Broker ownership and routing

- Nephtys owns JetStream stream `NEPHTYS` with subject `nephtys.stream.>`.
- Mercury may create that exact stream when it starts first, but it must not create an overlapping,
  narrower stream.
- The supported market-data subject is `nephtys.stream.crypto.btc` by default.
- Brain uses an exact subject filter. Plutarch defaults to `nephtys.stream.crypto.*`.
- The JetStream stream sequence is the broker delivery/archive identity. It is not an exchange
  sequence and must not be substituted for one in multi-event order-book logic.

Stream and consumer names are configurable for isolated deployments, but all configured filters
must remain covered by the selected upstream stream.

## JSON envelope

Mercury's supported market boundary is a JSON object compatible with:

```json
{
  "source": "coinbase_btc_eur_ticker",
  "type": "websocket_message",
  "timestamp": 1785091728149,
  "payload": {
    "symbol": "BTC-EUR",
    "price": "56892.4"
  }
}
```

NME/1 fields:

| Field | Requirement | Semantics |
|---|---|---|
| `source` | Required string | Stable registered Nephtys stream ID; part of Mercury's strategy-state key. |
| `type` | Required string | Generic or source-derived event kind. Unknown non-L2 values use Mercury's L1 path. |
| `timestamp` | Required integer | Unix epoch milliseconds. It may be source event time or Nephtys receive time; the profile must document which. |
| `payload` | Required JSON object | Profile-normalized market fields. Non-object payloads are incompatible with the supported market path. |
| `seq` | Optional non-negative integer | Source/exchange sequence only when Nephtys can infer one. Omission means unavailable. |
| `content_type` | Optional string | Omitted or `application/json` for NME/1 market events. Binary events are outside this boundary. |

Additional envelope and payload fields are additive and tolerated. Removing or renaming a required
field, changing its JSON type or timestamp unit, changing the meaning of `seq`, or changing a
profile's required payload mapping is a breaking NME change.

## Coinbase BTC/EUR profile

The alpha-default `coinbase_btc_eur` profile has this tested behavior:

- connector ID/source: `coinbase_btc_eur_ticker`;
- subject: `nephtys.stream.crypto.btc`;
- unauthenticated upstream product: `BTC-EUR` ticker;
- required normalized payload fields: `symbol` and numeric-string-or-number `price`;
- optional normalized payload fields: `qty`, `volume`, `best_bid`, `best_ask`, `sequence`, `venue`,
  and `kind`;
- envelope `type`: currently `websocket_message`, because Nephtys infers type from upstream field
  `e` and Coinbase uses `type`;
- envelope `timestamp`: currently Nephtys receive time, because Coinbase does not supply inferred
  field `E`;
- envelope `seq`: currently omitted because none of Nephtys's inferred sequence keys is present
  after the generic WebSocket boundary. Plutarch represents an omitted source sequence as `-1` in
  archive schema v2; `_stream_sequence` remains the non-null broker identity.

Mercury accepts dash, slash, and underscore symbol forms and normalizes strategy/execution symbols
to `BASE/QUOTE`.

## L2 boundary

L2 events require explicit `l2_snapshot` or `l2_delta` types, a symbol, and a meaningful
per-symbol source sequence. Binance-specific depth reconstruction is a legacy Mercury adapter and
is not part of the default Coinbase NME/1 profile. A generic L2 profile must be contract-tested
before it can be listed as supported.

## Persistence and restart behavior

Nephtys persists registered stream source configurations in its `nephtys_streams` KV bucket and
restores them on process restart. Runtime pipeline updates made through the pipeline update endpoint
are currently transient and are not written back to that persisted source configuration.

Therefore:

- provision the complete desired pipeline in the registered source configuration;
- do not treat a successful transient pipeline update as restart-safe;
- after restart, verify the effective source/profile and receive a new event;
- record the limitation in release evidence until Nephtys persists pipeline updates.

NATS durable-consumer settings also persist. Mercury validates correctness-relevant filters,
delivery mode, ACK policy/timing, delivery cap, and pending headroom before binding. Incompatible
durables require a deliberate migration or a new durable name.

## Compatibility matrix

| NME | Mercury | Nephtys | Evidence |
|---|---|---|---|
| 1 | `c81b965` and later compatible commits | `485ecbb7beb7f0d46d58744941f21bcb981beef8` | Synthetic real-JetStream pipeline/redelivery tests plus real Coinbase connector and restart drill on 2026-07-26 |

No broader Nephtys version range is implied until tagged Nephtys releases run the same compatibility
suite. A later commit is compatible only if it preserves NME/1 and passes the release gate.

## Change policy

- Additive optional fields may remain NME/1 when existing consumers retain their behavior.
- A required-field, type, unit, routing, sequence, or normalized-payload semantic change requires a
  new NME major version and a migration note.
- Every supported pair must pass the deterministic real-JetStream tests.
- Before an alpha/beta tag, one exact pair must also pass the documented real-connector restart
  drill.
- Release notes must record both revisions, NME major, feed profile, and known deviations.
- Archive schema changes are versioned separately; NME compatibility does not make old archive
  rows acquire missing provenance.
