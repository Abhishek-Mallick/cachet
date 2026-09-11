<h1 align="center">Cachet</h1>

<p align="center">
  <strong>An integrated read cache for sharded OLTP databases<br/>
  that continuously proves its own correctness — and reports it as a number.</strong>
</p>

<p align="center">
  <a href="./documentation/WHAT-IS-CACHET.md"><strong>📖 What is Cachet?</strong></a>
  &nbsp;·&nbsp;
  <a href="./documentation/USING-CACHET.md"><strong>🚀 Using Cachet</strong></a>
  &nbsp;·&nbsp;
  <a href="./CONSISTENCY.md">Consistency model</a>
  &nbsp;·&nbsp;
  <a href="#benchmarks">Benchmarks</a>
  &nbsp;·&nbsp;
  <a href="#status">Status</a>
</p>

<p align="center">
  <sub><strong>⚠️ Under active development.</strong> Phases 0–2 complete, Phase 3 next.
  Not production software. <a href="#status">See what works today →</a></sub>
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

## Status

**Currently: Phase 2 complete. Phase 3 next.**

| Phase | | What it delivers |
|---|---|---|
| **0 · Foundation** | ✅ | Sharded MyRocks stack, consistent-hash ring, HLC versioning, gRPC API over TCP **and** Unix sockets, RED metrics + Grafana, open-loop Zipfian harness, uncached baseline |
| **1 · Naive TTL cache** | ✅ | Cache-aside with TTL only. Staleness knowingly bad, and asserted as a test |
| **2 · Exact invalidation** | ✅ | CDC tailer + checkpointing, versioned CAS, negative caching, independent cache ring, proportional circuit breaker, `cachetctl` |
| **3 · Consistency model** | ⬜ | Affected-key extraction, session tokens, 4 levels, conformance suite, Go SDK with OTel watermark propagation |
| **4 · Leases · adaptive admission · Sextant** | ⬜ | The stampede graph, per-key r:w admission, continuous verification |
| **5 · Proof** | ⬜ | 9 injected faults, each caught *and explained* |
| **6 · Publish** | ⬜ | Writeup + the MyRocks-vs-InnoDB cache-value study |

### What runs today

| Component | State |
|---|---|
| `cachet` — query engine, cache-aware reads, versioned CAS fill/tombstone | ✅ Working |
| `flux` — CDC tailer, durable atomic binlog checkpoints | ✅ Working |
| `cachetctl` — operator CLI: status, ring, inspect, invalidate, checkpoint | ✅ Working |
| `benchctl` — open-loop driver, staleness probe, report generator | ✅ Working |
| Synchronous write-path invalidation | ✅ Working *(arrived early from Phase 3)* |
| Negative caching with read-own-inserts | ✅ Working |
| Independent cache ring — routed separately from database shards | ✅ Working |
| Proportional circuit breaker — sheds a fraction, never all | ✅ Working |
| `pkg/cachet` Go SDK · consistency conformance suite | ⬜ Phase 3 |
| Leases · adaptive admission · Sextant | ⬜ Phase 4 |

### Three results worth knowing about

**Read-own-writes held in Phase 1 with no invalidation at all.** Writes never touched the cache, yet
a session carrying its token could not be served a stale entry — because the watermark check rejects
anything filled from a database state older than its own write. That is the payoff of watermarking
on the *fill* version rather than the row version, a decision made on paper before any of the code
existed.

**A TTL-only cache beats a correct one on hit rate, because it has stopped noticing writes.** At the
production 4-hour TTL it reached 93.9% and 46 origin QPS; exact invalidation gives some of that back
(89.7%, 78 QPS). Two of the TTL-only runs hit 100% with zero origin load. It is "perfect" precisely
to the extent that it is wrong. Publishing that trade is the point.

**Shedding a read costs hit rate; shedding an invalidation costs correctness.** The circuit breaker
gates reads and fills but never tombstones. A shed read is served from the database and nobody is
misinformed — but a shed invalidation leaves a stale entry alive, still serving a value the database
has already changed. That asymmetry is enforced by a test, and the test was checked by making the
mistake on purpose: gating tombstones produced *"10 of 10 tombstones to a dead node were silently
swallowed."*

## Benchmarks

*Populated as phases land. Every row is regenerated by `make bench-report` from JSON in
`bench/results/`; no number here is typed by hand.*

**Origin QPS** is steady-state database load under W2 — the cost metric the caching claim actually
rests on. The stampede number (one hot key, ten thousand concurrent readers) is a separate
measurement and arrives with leases in Phase 4a.

| Configuration | Hit rate | p99 read | Origin QPS | Staleness | Cache mem |
|---|---|---|---|---|---|
| No cache | — | 15.20ms <sub>(12.78ms–17.87ms)</sub> | 760 | — | — |
| TTL only | 93.9% | 63.81ms <sub>(10.35ms–77.18ms)</sub> | 46 | 10.07s <sub>p99</sub> | — |
| + CDC invalidation | 89.7% | 16.51ms <sub>(15.23ms–27.41ms)</sub> | 78 | 34.98ms <sub>p99</sub> | — |
| + exact write-path invalidation | — | — | — | — | — |
| **+ leases** | — | — | — | — | — |
| **+ adaptive admission** | — | — | — | — | — |
<sub>Measured on **docker-desktop-macos** (arm64, 10 cores) · 3 runs · 25s measured at 500 rps · θ=0.99 · 2026-09-06. Generated by `make bench-report`; do not edit by hand.</sub>

### How to read that table

**The result so far is load, not latency.** Caching cut steady-state database load by **81%**
(760 → 145 origin QPS in Phase 1), and exact invalidation cut staleness from **10.07 s to 34.98 ms —
288× better** — while giving back some hit rate as the price of correctness.

**The p99 column is not a result yet, and we say so rather than rounding it into one.** Read p99 has
been unmeasurable for three phases running: the run-to-run spreads swamp every difference
(TTL-only ranged 10.35–77.18 ms). When the spread exceeds the effect, nothing has been measured.
Two runs — one of each configuration — would have produced a confident and false claim.

**Why the cache does not help p99 *here*, and why that is expected.** At 500 rps against three idle
MySQL shards, an uncached point lookup is already fast; the cache removes a database round trip that
was not the bottleneck. The cost it removes is *load*, not wall time. A p99 win should appear once
the origin is under enough pressure to queue — which is exactly what Phase 4a's stampede workload is
built to create.

**Standing caveat on every number above:** measured on a Docker Desktop VM, whose virtualised
filesystem gives I/O latency that is neither predictable nor representative — and MyRocks is
I/O-sensitive. Comparisons *between rows* hold, because every row was measured identically. The
absolute values do not transfer. Headline numbers move to a dedicated Linux host before publication.

Both invalidation paths were measured separately, so neither is credited with the other's work. The
difference between them is entirely in the tail — synchronous invalidation is bounded by the write
it rides on, while CDC adds a variable delivery delay:

| Invalidation path | Staleness p50 | p90 | p99 | p99.9 |
|---|---|---|---|---|
| CDC only | 5.60 ms | 14.53 ms | 34.98 ms | 101.50 ms |
| Synchronous write path + CDC backstop | 6.28 ms | 9.80 ms | 17.41 ms | **26.05 ms** |

<sub>250 observations each, none failing to converge. Methodology: [`docs/cachet-benchmarking.md`](./docs/cachet-benchmarking.md).</sub>

## Documentation

| Doc | What it covers |
|---|---|
| 📖 [**What is Cachet?**](./documentation/WHAT-IS-CACHET.md) | The intent, the problem it solves, and why the approach is better. **Start here.** |
| 🚀 [**Using Cachet**](./documentation/USING-CACHET.md) | Quick start, configuration, the gRPC API, consistency levels, benchmarking, troubleshooting |
| [`CONSISTENCY.md`](./CONSISTENCY.md) | The normative consistency model — every level's guarantee, non-guarantee, and the test that catches its violation |
| [`CONTRIBUTING.md`](./CONTRIBUTING.md) | Engineering standards and the CI gate |

| [`docs/cachet-benchmarking.md`](./docs/cachet-benchmarking.md) | How every published number is produced, and the traps that make benchmarks fiction |
| [`docs/adr/`](./docs/adr/) | Architecture decisions: Go · Valkey vs Redis · HLC versioning · sidecar-as-default |

<sub>Detailed product, delivery and execution planning is kept out of this repo. The two documents
above are published because a reader cannot check our claims without them.</sub>

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

[cf1]: https://www.uber.com/gb/en/blog/how-uber-serves-over-40-million-reads-per-second-using-an-integrated-cache/
[cf2]: https://www.uber.com/us/en/blog/how-uber-serves-over-150-million-reads/
[memcache]: https://courses.cs.duke.edu/fall25/compsci512/internal/readings/facebook-memcached.pdf
[polaris]: https://engineering.fb.com/2022/06/08/core-infra/cache-made-consistent/
