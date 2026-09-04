<h1 align="center">Cachet</h1>

<p align="center">
  <strong>An integrated read cache for sharded OLTP databases<br/>
  that continuously proves its own correctness — and reports it as a number.</strong>
</p>

<p align="center">
  <em>🚧 Early development. Architecture and consistency model are settled; implementation is in progress.
  See <a href="#roadmap">Roadmap</a> for honest status.</em>
</p>

---

## The problem

Your services each cache in Redis. Each one invalidates differently. None of you can say how often
you're serving stale data.

That's not a discipline problem — it's a **layering** problem. The cache lives in the application,
and the application only sees SQL. When it issues `UPDATE orders SET status=? WHERE customer_id=?`,
it does not know which rows that touched. So it guesses: blow away the table, or set a short TTL and
hope. A short TTL doesn't make you correct. It just shortens the window in which you're wrong.

So teams end up choosing between two bad options:

- **Cache aggressively** and accept stale reads you can't quantify, until an incident forces the TTL
  down and the database load back up.
- **Don't cache** and pay for read capacity that grows faster than traffic — read replicas multiply
  cost without fixing consistency, because a replica is just a slower way to be stale.

Neither option gives you a number. That's the gap Cachet is built for.

## The approach

Cachet moves the cache **into the data layer**, where writes are actually visible.

```
   PROXY MODEL (ReadySet, PolyScale)        INTEGRATED MODEL (Cachet)

   app ──► proxy ──► database               app ──► query engine ──► storage engine
             │                                          │                  │
             └─ sees: SQL text                          └─ sees: affected row keys
                infers: "maybe this table"                 knows: exactly these 3 rows
                gives:  eventual consistency               gives: read-own-writes
```

A proxy sits outside the database and can only infer what a write touched. A query engine that owns
the read path receives **the exact set of affected row keys plus a commit timestamp** — so
invalidation is precise, and can happen on the write path before the write is acknowledged.

Precise invalidation is the foundation. Everything below is what it makes possible.

## What's different

| | Cachet | ReadySet | PolyScale | DIY Redis |
|---|---|---|---|---|
| Invalidation | CDC + **exact write-path** | Streaming dataflow | Heuristic | Hand-rolled |
| Consistency | **Read-own-writes, tiered** | Eventual | Probabilistic | Undefined |
| **Measured correctness** | ✅ **Live SLO** | ❌ | ❌ | ❌ |
| Stampede protection | ✅ **Leases** | Partial | ❌ | ❌ |
| Self-tuning admission | ✅ **Per-key r:w** | ❌ manual | Heuristic | ❌ |

**Every cache on that list asks you to trust it. Cachet is the only one that proves it.**

### 1. Leases — bounded origin load

On a miss, exactly one caller gets a token to fill that key. Concurrent callers wait briefly, then
read the filled value. Origin load per key is bounded at ~1 per lease interval **regardless of
concurrency** — not best-effort, by construction.

Deduplicating concurrent fills — the common approach — fixes *ordering*: a slow fill can't overwrite
a newer value. It does nothing for *admission*. Ten thousand simultaneous misses on a hot key still
all reach the database, which is exactly when you can least afford them.

### 2. Adaptive admission — no human decides what to cache

Cachet tracks the observed read:write ratio **per key** with a count-min sketch, and caches only
what earns it.

The usual approach is a person picking tables and a rule of thumb about read:write ratios. But
ratios aren't uniform within a table and they drift. A write-churning key in an otherwise read-heavy
table is pure cost: every write pays invalidation, every read misses. Cachet finds those keys and
stops caching them, continuously.

### 3. Sextant — continuous consistency verification 🔭

A verifier that subscribes to the invalidation stream, shadow-reads every cache replica, and detects
divergence — with **consistency tracing** that records each mutation, so "why was this stale?" has
an answer instead of a shrug.

This is the feature the category is missing. Sampling monitors tell you a violation happened, some
minutes later, without telling you why. Sextant runs continuously and keeps enough state to
reconstruct the sequence that caused any divergence it finds.

### 4. Consistency as a per-request parameter

| Level | Guarantee | For |
|---|---|---|
| `STRONG` | Bypasses cache | Money, auth |
| `SESSION` *(default)* | Read-own-writes + monotonic reads | Almost everything |
| `BOUNDED(t)` | Staleness ≤ t | Feeds, counts, listings |
| `EVENTUAL` | Best effort | Recommendations |

And Sextant publishes a **measured SLO per level** — not a promise in a doc, a live number.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  QUERY ENGINE (stateless)                                     │
│   read:  lease-guarded lookup → fill                          │
│   write: commit → exact invalidation → ack                    │
│   + adaptive admission · circuit breaker · consistency levels │
└────────┬──────────────────────────────────┬──────────────────┘
         ▼                                  ▼
   ┌───────────┐                  ┌───────────────────┐
   │  Redis    │◄──invalidate─────│ Sharded MySQL     │
   │  + Lua:   │                  │ + MyRocks         │
   │  leases   │                  │ returns affected  │
   │  dedup    │                  │ keys + commit ts  │
   │  markers  │                  └─────────┬─────────┘
   └─────┬─────┘                            │ binlog
         │                          ┌───────▼───────┐
         │◄────backstop─────────────│  CDC tailer   │
         ▼                          └───────┬───────┘
   ┌──────────────────────────────────────────────────┐
   │  SEXTANT — continuous verifier + tracing         │
   │  exports live consistency SLO per level          │
   └──────────────────────────────────────────────────┘
```

Backed by **MySQL + MyRocks** — an LSM storage engine, where reads are more expensive and more
variable than on a B-tree. That makes the cache work harder for its place in the stack, and makes
the benchmarks more interesting.

## Scope — what Cachet is deliberately not

- **Not a transparent SQL proxy.** That forfeits exact invalidation. It's the trade we refuse.
- **Not a query-result cache.** Point lookups and row ranges. Complex joins are ReadySet's job.
- **Not a write cache.** Writes go to the database. Always.
- **Not a database.** It never becomes the source of truth.

## Roadmap

| Phase | Status |
|---|---|
| 0 · Foundation — sharded MyRocks, gRPC API, Zipfian load harness, uncached baseline | 🔜 |
| 1 · Naive TTL cache | ⬜ |
| 2 · CDC invalidation, Lua dedup, negative caching, circuit breaker | ⬜ |
| 3 · Exact write-path invalidation, consistency levels, read-own-writes | ⬜ |
| 4 · **Leases · adaptive admission · Sextant** | ⬜ |
| 5 · Fault injection report — 9 faults, each caught and explained | ⬜ |
| 6 · Benchmarks + writeup | ⬜ |

## Benchmarks

*Populated as phases land. Every claim here will be reproducible from `bench/`.*

| Configuration | Hit rate | p99 read | Origin QPS under stampede | Staleness | Redis mem |
|---|---|---|---|---|---|
| No cache | — | — | — | — | — |
| TTL only | — | — | — | — | — |
| + CDC invalidation | — | — | — | — | — |
| + exact write-path invalidation | — | — | — | — | — |
| **+ leases** | — | — | — | — | — |
| **+ adaptive admission** | — | — | — | — | — |

## Prior art

Cachet builds on published work. Credit where it's owed:

- [**Integrated caching in a sharded document store**][cf1] ([follow-up][cf2]) — putting the cache
  inside the query engine, CDC-driven invalidation, exact write-path invalidation, negative caching,
  and sharding the cache independently of the database
- [**Scaling Memcache**][memcache] (NSDI '13) — leases, stale sets, and thundering herds
- [**Cache made consistent**][polaris] (2022) — continuous cache-consistency verification and
  tracing, and the demonstration that it's worth several orders of magnitude. *Its verifier is named
  Polaris; Sextant is the instrument that measures against it.*
- **Riak** — per-request tunable consistency, an idea the industry dropped and shouldn't have

## License

TBD

[cf1]: https://www.uber.com/gb/en/blog/how-uber-serves-over-40-million-reads-per-second-using-an-integrated-cache/
[cf2]: https://www.uber.com/us/en/blog/how-uber-serves-over-150-million-reads/
[memcache]: https://courses.cs.duke.edu/fall25/compsci512/internal/readings/facebook-memcached.pdf
[polaris]: https://engineering.fb.com/2022/06/08/core-infra/cache-made-consistent/
