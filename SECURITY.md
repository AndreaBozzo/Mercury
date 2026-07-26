# Security policy

## Supported scope

Only the latest commit on `master` and the newest tagged pre-release, when one exists, receive
security fixes. Mercury is a research alpha. Live execution is disabled and is not a supported
deployment mode.

## Reporting a vulnerability

Do not open a public issue for a vulnerability, leaked credential, private endpoint, or other
sensitive report.

Use the repository's **Security** tab and choose **Report a vulnerability**. The repository owner
must enable GitHub private vulnerability reporting before making Mercury public. If that option is
not visible, contact the repository owner through their GitHub profile and ask for a private
reporting channel without including exploit details in the initial message.

Include:

- affected revision and deployment mode;
- a concise impact statement;
- reproduction steps or a minimal proof of concept;
- whether credentials, funds, personal data, or broker access may be exposed;
- suggested mitigations, if known.

Reports are handled on a best-effort basis. There is currently no bug-bounty program or guaranteed
response time. Please allow maintainers a reasonable remediation window before disclosure.

## Credential incidents

If an exchange, NATS, Nephtys, cloud, or LLM credential may have leaked, revoke or rotate it
immediately. Do not wait for a code fix. Exchange credentials should use the least privilege
available, have withdrawals disabled, and use network restrictions where the venue supports
them.

## Deployment boundary

The sample Compose deployment is local-only and binds ports to loopback. It has no NATS
authentication or TLS and must not be exposed to another host or an untrusted Docker network.
Production hardening is outside the supported alpha.
