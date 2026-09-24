# ADR 0009 — Sextant stands alone

**Status:** Accepted · 2026-09-24

## Context

Sextant measures whether a cache is as consistent as it claims. Its core — the rule that decides
what a violation is, the sampling loop, the SLO, the trace ring — imported nothing but
`pkg/consistency`. It was internal only because everything started there.

A verifier nobody can point at their own deployment verifies nothing anybody else can check. And
the population of people with a cache and a doubt is far larger than the population deploying
Cachet.

## Decision

**Publish the verifier; keep the Cachet adapters private.** `pkg/sextant` holds the rule and the
machinery. `Origin` and `Cache` are interfaces, so what is being read is somebody else's problem;
`internal/sextant` keeps the two adapters that know about Cachet's own cache client and its hash
ring.

**No separate repository.** `Classify` is the reason: the rule lives in one function so the number
on a dashboard and the sentence in `CONSISTENCY.md` cannot drift apart. A second repository would
make that a promise instead of a property.

**Three tiers, and the tier is a label on every metric.**

| Tier | Comparison | Can evaluate |
|---|---|---|
| `value` | The cached value against a projection of the row | `EVENTUAL` only |
| `version` | A version field in the cached value against a database column | `EVENTUAL`, `SESSION`, `BOUNDED(t)` |
| `cachet` | The entry's HLC fill version against the row version | All of the above |

`SESSION` and `BOUNDED(t)` are statements about **which** database state an entry reflects. Two
values differing says an entry is not current; it does not say what it is. So at the `value` tier
those levels are not exported at all — not as zero violations, which reads as a clean bill of
health, but as not measured. A dashboard without a tier label is ambiguous in the one direction
that matters.

`Observation` therefore carries a `Difference` rather than two `uint64`s. Two version numbers is an
assumption that versions exist, and an adapter that has none would have supplied zeros a comparison
would treat as versions.

**The binlog is the primary key source, and the alternatives are labelled.** Replication enumerates
rows that *changed*, which is the population a stale entry lives in, and it comes from the database
— so the cache cannot steer what it is checked on. A file is exact and reproducible. `SCAN` reports
itself **degraded**, and the metric says so: it enumerates *cached* keys, so a dropped invalidation
that left no entry cannot be sampled at all, and the failure hides itself by looking like an absent
key.

**Its own `sextant.yaml`, its own formula, the repository's version.** A verifier that demanded a
`cachet.yaml` would be a verifier for one product. `brew install sextant` is the sentence that has
to work. Independent semver is *not* adopted: Sextant ships from the same commits, and a second tag
scheme would be release machinery with nothing behind it until the two need to diverge. That is a
decision to revisit, not an oversight.

**Read-only twice over.** The cache wrapper exposes `Peek` and nothing else, and the documented
deployment is a Redis ACL user with `+@read`. A verifier that could write to what it checks could
influence what it reports; one half of that promise is in the code and the other is checkable
without reading Go.

**Identifiers come from configuration and from nowhere else.** Every statement is built once at
construction from validated identifiers; every value is a bound argument. Nothing derived from a
key becomes SQL. That is the narrow, checkable promise a tool pointed at somebody's production
database has to make.

## Consequences

- `test/verifier` is the credibility argument, executed: three deliberately broken writers must be
  caught and one correct writer must not be accused. `FALSE-POSITIVES.md` publishes the measured
  rate under a write burst — 0 in 6,455 observations — because that number decides whether anyone
  keeps the tool running.
- A verifier whose tier needs versions and whose adapters cannot supply them fails loudly rather
  than falling back to comparing bytes. A silent fallback would keep exporting `SESSION` and
  `BOUNDED` series that nothing was measuring.
- Cachet keeps its native path: `sextant -cachet-config cachet.yaml` reads its own entries and
  routes by its own ring, which is exact and needs no key template.
