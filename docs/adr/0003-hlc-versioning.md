# ADR 0003 — Hybrid Logical Clocks for entry versioning

**Status:** Accepted · 2026-09-05

## Context

Every cache mutation in Cachet is a compare-and-set against a version (`CONSISTENCY.md` §2). That
makes the version scheme the most consequential design decision in the system: it must be monotonic
per shard, cheap to produce on every write, comparable as a scalar, and roughly aligned with wall
time so that `BOUNDED(t)` can be expressed in seconds rather than in opaque counters.

## Decision

**A Hybrid Logical Clock per shard**, packed into a `uint64`: 48 bits of physical milliseconds, 16
bits of logical counter.

```
now = wall_clock_ms()
if now > hlc.physical: hlc = (now, 0)
else:                  hlc = (hlc.physical, hlc.logical + 1)
```

Every cached table carries `version BIGINT UNSIGNED NOT NULL`, indexed. Cachet maintains it.

## Rationale

| Alternative | Why not |
|---|---|
| Wall clock alone | Clock skew across engine nodes produces version inversions, and therefore silent staleness — the exact failure this project exists to detect |
| MySQL GTID | Requires parsing and ordering GTID sets; not comparable as a scalar, and awkward inside a Lua script |
| Auto-increment sequence table | A write to obtain a version on every write: an extra round trip plus a contention point on the hot path |
| Pure Lamport counter | Monotonic, but carries no wall-clock meaning, so `BOUNDED(t)` becomes inexpressible |

HLC keeps the useful half of each: monotonic under skew like a logical clock, interpretable in
milliseconds like a physical one. 16 bits of logical counter permits 65,536 writes within a single
millisecond on one shard before the physical component is forced forward — far above any plausible
per-shard write rate. 48 bits of milliseconds runs to the year 10889.

It also converts fault #8 (clock skew) from a hope into a measurement: HLC degrades observably rather
than silently.

## Consequences

- **A real integration cost, stated plainly in the product scope:** every cached table needs a
  `version` column, and every write through Cachet maintains it. This is not hidden.
- Versions from **different shards are incomparable**. Comparing them is a bug, which is why the
  session watermark is a per-shard map rather than a scalar (`CONSISTENCY.md` §4).
- The engine must monitor skew against `max_clock_skew` and surface it; `BOUNDED(t)`'s guarantee is
  explicitly conditional on that bound.
- Out-of-band writes made directly to MySQL will not carry a version. Flux must treat a missing or
  zero version as "invalidate unconditionally", and the model records those keys as
  `BOUNDED(cdc_lag_bound)` for everyone.
