# Contributing to Mercury

Mercury is an observation-first research alpha. Contributions should improve data integrity,
reproducibility, safety boundaries, or clarity without implying that the system is profitable or
safe for live funds.

## Before contributing code

Mercury is licensed under the Apache License 2.0. Unless you explicitly state otherwise, any
contribution intentionally submitted for inclusion in Mercury is provided under the same terms,
as described in section 5 of the licence.

Security and conduct reports do not belong in public issues. Follow [SECURITY.md](SECURITY.md).

## Development workflow

1. Create a focused branch from `master`.
2. Install the locked development environment with `uv sync --locked`.
3. Add a regression test for behavioral changes.
4. Run `make check`.
5. Open a pull request that explains the failure mode, the chosen semantics, and any remaining
   limitation.

For a real JetStream smoke run, point `MERCURY_INTEGRATION_NATS_URL` at a disposable NATS server
with JetStream enabled and run:

```bash
uv run pytest -m integration
```

The integration tests create Mercury's standard stream, consumer, and KV names. Never point them
at a shared or production broker.

## Safety review rules

- Observation mode must never construct an exchange client or make private venue requests.
- Live mode must continue to fail before any connection or order attempt.
- Broker failures, missing safety state, invalid numeric state, and stale dynamic risk state fail
  closed.
- Explicit JetStream ACK/NAK/TERM handlers must use manual acknowledgement.
- Archive messages are acknowledged only after a Lance write or a durable quarantine write.
- Do not add claims of profitability, legal availability, exchange authorisation, or production
  readiness without dated, reviewable evidence.
- Keep safety-relevant changes small and separate from unrelated cleanup.

## Style and checks

Mercury uses Ruff, mypy, pytest, `pip-audit`, and package builds in CI. The canonical local gate is:

```bash
make check
```

Use typed domain models at service boundaries. Prefer explicit failure behavior over broad
exception suppression. Document migrations when changing the archive schema or the Nephtys event
contract.

## Scope

The supported alpha path is direct Brain processing, Executor observation mode, and Plutarch
archival. Bytewax, sandbox execution, and venue-specific research adapters are experimental.
Changes to those areas must not weaken the supported path.
