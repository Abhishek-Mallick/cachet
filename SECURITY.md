# Security

## Reporting a vulnerability

**Report privately**, via [GitHub's private vulnerability reporting][advisory] on this repository.
Do not open a public issue for a vulnerability.

[advisory]: https://github.com/Abhishek-Mallick/cachet/security/advisories/new

Please include what you did, what happened, and what you expected. A proof of concept helps; a
minimal one helps more than a thorough one.

**What to expect:** an acknowledgement within a few days, an assessment with a severity and a plan,
and credit in the advisory unless you would rather not be named. Cachet is pre-1.0 and maintained by
one person — that is the honest bound on response time, and it is better stated than discovered.

## Scope

Cachet sits in the read path of a database. The failures that matter most are the ones that let a
reader see something they should not, or let a write go unnoticed.

**In scope, and treated as security rather than correctness:**

- A consistency level serving data that violates its documented guarantee, where the violation is
  reachable by an untrusted caller.
- Session tokens that let one caller observe another's watermark, or that can be forged to obtain a
  stronger or weaker guarantee than the caller is entitled to.
- Cache entries crossing a tenant boundary — the entry carries `tenant_id` for exactly this reason.
- Invalidations that can be suppressed or replayed by an attacker to extend a staleness window.
- Anything that makes the engine serve a row the database would not have returned.

**Out of scope:**

- Denial of service by overwhelming the origin database. Cachet bounds origin load under a
  stampede; it is not a rate limiter.
- Findings that require control of the binlog, the cache, or the database. Cachet trusts its own
  data plane, and a report assuming otherwise is a report about your network.
- Missing hardening on the demo stack in `test/env/`. It exists to be torn down, has default
  credentials on purpose, and should never be exposed.

## What the threat model assumes

- **The cache is trusted.** Anything that can write to Valkey/Redis can serve whatever it likes.
  Deploy it on a network where that is true.
- **The binlog is trusted.** The tailer applies what it reads.
- **Callers are not trusted.** A caller chooses its consistency level and carries a session token,
  and neither should let it read what it could not read from the database directly.

## Verifying a release

Release artefacts are signed with [Sigstore][sigstore] keyless signing, against the OIDC identity of
the workflow that built them. There is no long-lived key to leak, and the transparency log is what
lets you check a download against the workflow run that produced it.

```bash
cosign verify-blob checksums.txt \
  --certificate checksums.txt.pem \
  --signature checksums.txt.sig \
  --certificate-identity-regexp 'https://github.com/Abhishek-Mallick/cachet/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

sha256sum -c checksums.txt --ignore-missing
```

Container images are signed **by digest**, never by tag — a tag can be moved after signing:

```bash
cosign verify ghcr.io/abhishek-mallick/cachet/cachet@sha256:... \
  --certificate-identity-regexp 'https://github.com/Abhishek-Mallick/cachet/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Pin digests rather than tags in anything you deploy.

[sigstore]: https://www.sigstore.dev/

## Supported versions

Pre-1.0: only the latest tag receives fixes. There are no maintained release branches, and saying so
is more useful than a table implying otherwise.
