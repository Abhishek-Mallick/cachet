# ADR 0002 — Valkey as the default cache, Redis supported

**Status:** Accepted · 2026-09-05

## Context

Cachet needs a cache server that supports Lua scripting with `EVALSHA`, hashes, `SET NX PX`, and
clustering. Redis relicensed to AGPLv3 in 2025; Valkey is the Linux Foundation BSD-licensed fork.
They speak the same protocol and run the same Lua.

Cachet runs the cache as an **external service** and does not link against it, so neither licence
propagates to Cachet's own Apache 2.0 licence. The question is therefore not legal exposure — it is
which one is the default in the compose stack, the docs, and the benchmark methodology.

## Decision

**Valkey 8 is the default. Redis 8 is supported and tested.** The compose stack ships Valkey; a
one-line overlay swaps in Redis. `go-redis/v9` is the client for both.

## Rationale

- Neutral governance under the Linux Foundation removes a licensing conversation from every
  enterprise evaluation. For a project whose primary asset is credibility, spending a reviewer's
  attention on a licence question rather than on the consistency model is a bad trade.
- The two are protocol- and script-compatible, so supporting both costs one CI matrix entry rather
  than a compatibility layer.
- Users overwhelmingly already run one of them. Refusing either would be a pointless adoption tax.

## Consequences

- The benchmark methodology must **pin and publish the image digest** of whichever server produced a
  given number, because the two will diverge over time.
- A CI matrix entry runs the Lua and integration suites against both. If they ever diverge
  behaviourally, that divergence is a documented finding, not a silent difference.
- Cachet must not use any command exclusive to either. Reviewed at each new Lua script.
