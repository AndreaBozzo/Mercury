# Public alpha release checklist

This records the final evidence gate used for Mercury's public observation alpha. Run source checks
from a fresh clone at the exact commit intended for the release. Record the Mercury and Nephtys
revisions and the date in the release notes.

## Repository settings and policy

- [x] `LICENSE` contains Apache License 2.0 and `NOTICE` identifies the copyright owner.
- [x] Contribution, security, conduct, support, and pull-request policies are present.
- [x] Issue and collaboration settings match the support policy: Issues are enabled, a pull-request
  template and structured issue forms are present, and Discussions are intentionally disabled
  while support is issue-based.
- [x] CI actions and container inputs are immutable-SHA/digest pinned; Dependabot version updates
  are configured for uv and GitHub Actions.
- [x] While still private, enable the dependency graph, Dependabot alerts, and Dependabot security
  updates.
- [x] Push the exact release commit and wait for its CI run to pass.
- [x] Change repository visibility to public.
- [x] Immediately enable and verify private vulnerability reporting.
- [x] Protect `master`, require the unique `check` status, require pull requests and conversation
  resolution, and block force pushes/deletions.
- [x] Confirm public secret scanning and push protection are active; enable code scanning default
  setup and resolve any alerts before announcing the release.

The final GitHub API audit on 2026-07-26 found the repository public with Issues enabled and
Discussions intentionally disabled. Dependabot alerts and security updates, private vulnerability
reporting, secret scanning, push protection, and CodeQL default setup for Python and Actions are
enabled. All three alert APIs reported zero open alerts. `master` requires an up-to-date
GitHub-Actions `check`, a pull request, resolved conversations, linear history, and enforcement for
administrators; force pushes and deletion are blocked.

No personal email address is published as a security-reporting workaround.

## Clean source verification

```bash
git clone https://github.com/AndreaBozzo/Mercury.git
cd Mercury
git checkout <release-commit>
uv sync --locked
uv run python -m scripts.check
git status --short
```

- [x] The portable source gate passes from the clean clone.
- [x] The unit/component suite reports 258 passes when no external broker is configured.
- [x] CI reports 260 passes when its real-NATS integration environment is enabled.
- [x] The source tree is clean after checks, except ignored build/test artifacts.
- [x] The built source and wheel metadata both identify Apache-2.0.
- [x] Download CI's `mercury-distributions` artifact and verify `SHA256SUMS`.

## Secret and data hygiene

- [x] Search the current tree and every reachable Git object for high-confidence credential and
  private-key patterns, sensitive filenames, and archived market-data filenames.
- [x] Confirm `.env`, exchange credentials, NATS credentials, local archives, quarantined payloads,
  coverage data, and audit scratch files are not tracked.
- [x] The full-history scan found no committed credential requiring revocation. If one is later
  discovered, revoke or replace it; deleting its file is not sufficient.
- [x] This source release contains and publishes no market dataset. Define retention, provenance,
  and redistribution policy before any separate dataset publication.

## Clean Docker verification

Use a clean Docker daemon with enough free disk space:

```bash
docker compose config --quiet
docker compose build --pull --no-cache
docker compose up -d
docker compose ps
curl --fail http://127.0.0.1:9108/health
curl --fail http://127.0.0.1:9109/health
curl --fail http://127.0.0.1:9110/healthz
docker compose exec brain id -u
docker compose exec executor id -u
docker compose exec plutarch id -u
docker compose logs --no-color
docker compose down
```

- [x] Every service becomes healthy without crash/restart loops.
- [x] Each application container reports UID `10001`.
- [x] Host ports bind only to loopback.
- [x] Read-only roots do not prevent normal operation; only declared data/tmp mounts are writable.
- [x] Shutdown is clean and NATS consumers/connections drain.

The included broker is unauthenticated and unencrypted. Never expose this Compose profile beyond
the local host.

## Mercury and Nephtys interoperability

- [x] Use an exact, clean Nephtys revision and the documented Coinbase BTC/EUR public profile.
- [x] Confirm a synthetic, contract-compatible Coinbase ticker reaches Brain and Plutarch.
- [x] Confirm Executor records `observed` and never constructs an exchange client.
- [x] Confirm archive rows contain source, subject, source timestamp, payload, and JetStream stream
  sequence.
- [x] Restart NATS, Brain, Executor, and Plutarch one at a time; record observed loss, redelivery,
  duplication, health transitions, and recovery behavior.
- [x] Restart a real Nephtys process and confirm its configured connector is restored correctly.
- [x] Record the known Nephtys pipeline-persistence limitation in release notes.

## Release record

- [x] Summarize known alpha limitations from `README.md` and `docs/ASSESSMENT.md`.
- [x] State explicitly that live execution is disabled and sandbox execution is experimental.
- [x] Link the dated ESMA MiCA register rather than claiming permanent venue eligibility.
- [x] Tag the verified commit as `v0.2.0` and attach the CI-generated source, wheel, and
  `SHA256SUMS` files to the GitHub pre-release.
