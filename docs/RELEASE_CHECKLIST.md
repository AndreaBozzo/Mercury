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

- [ ] The portable source gate passes from the clean clone.
- [ ] The unit/component suite reports 247 passes when no external broker is configured.
- [ ] CI reports 249 passes when its real-NATS integration environment is enabled.
- [ ] The source tree is clean after checks, except ignored build/test artifacts.
- [ ] The built source and wheel metadata both identify Apache-2.0.

## Secret and data hygiene

- [ ] Search the current tree and every reachable Git object for credentials, private keys,
  personal data, and archived market data.
- [ ] Confirm `.env`, exchange credentials, NATS credentials, local archives, quarantined payloads,
  coverage data, and audit scratch files are not tracked.
- [ ] Revoke and replace any credential that has ever been committed; deleting its file is not
  sufficient.
- [ ] Confirm the retention and redistribution policy for any dataset published separately from
  the source repository.

## Clean Docker verification

Use a clean Docker daemon with enough free disk space:

```bash
docker compose config --quiet
docker compose build --pull
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

- [ ] Every service becomes healthy without crash/restart loops.
- [ ] Each application container reports UID `10001`.
- [ ] Host ports bind only to loopback.
- [ ] Read-only roots do not prevent normal operation; only declared data/tmp mounts are writable.
- [ ] Shutdown is clean and NATS consumers/connections drain.

The included broker is unauthenticated and unencrypted. Never expose this Compose profile beyond
the local host.

## Mercury and Nephtys interoperability

- [ ] Use an exact, clean Nephtys revision and the documented Coinbase BTC/EUR public profile.
- [ ] Confirm a representative ticker reaches Brain and Plutarch.
- [ ] Confirm Executor records `observed` and never constructs an exchange client.
- [ ] Confirm archive rows contain source, subject, source timestamp, payload, and JetStream stream
  sequence.
- [ ] Restart Nephtys, NATS, Brain, Executor, and Plutarch one at a time; record observed loss,
  redelivery, duplication, and recovery behavior.
- [ ] Record the known Nephtys pipeline-persistence limitation in release notes.

## Release record

- [ ] Summarize known alpha limitations from `README.md` and `docs/ASSESSMENT.md`.
- [ ] State explicitly that live execution is disabled and sandbox execution is experimental.
- [ ] Link the dated ESMA MiCA register rather than claiming permanent venue eligibility.
- [ ] Tag the verified commit, attach the source/wheel artifacts produced by CI, and record their
  checksums.
