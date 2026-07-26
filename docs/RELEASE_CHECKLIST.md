# Public alpha release checklist

This is the final evidence gate for making Mercury public. Run it from a fresh clone at the exact
commit intended for the release. Record the Mercury and Nephtys commit SHAs and the date in the
release notes.

## Repository settings and policy

- [x] `LICENSE` contains Apache License 2.0 and `NOTICE` identifies the copyright owner.
- [x] Contribution, security, conduct, support, and pull-request policies are present.
- [ ] Enable GitHub private vulnerability reporting.
- [ ] Protect `master` and require the CI workflow before merging.
- [ ] Confirm issue, pull-request, and Discussions settings match the support policy.

Do not publish a personal email address merely to satisfy the security policy. GitHub private
vulnerability reporting is the preferred initial confidential channel.

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
- [x] The unit/component suite reports 257 passes when no external broker is configured.
- [ ] CI reports 259 passes when its real-NATS integration environment is enabled.
- [x] The source tree is clean after checks, except ignored build/test artifacts.
- [x] The built source and wheel metadata both identify Apache-2.0.

## Secret and data hygiene

- [x] Search the current tree and every reachable Git object for high-confidence credential and
  private-key patterns, sensitive filenames, and archived market-data filenames.
- [x] Confirm `.env`, exchange credentials, NATS credentials, local archives, quarantined payloads,
  coverage data, and audit scratch files are not tracked.
- [ ] Revoke and replace any credential that has ever been committed; deleting its file is not
  sufficient.
- [ ] Confirm the retention and redistribution policy for any dataset published separately from
  the source repository.

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
- [ ] Tag the verified commit, attach the source/wheel artifacts produced by CI, and record their
  checksums.
