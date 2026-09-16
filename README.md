<h1 align="center">Cachet</h1>

<p align="center">
  <strong>An integrated read cache for sharded OLTP databases<br/>
  that continuously proves its own correctness — and reports it as a number.</strong>
</p>

<p align="center">
  <a href="./documentation/WHAT-IS-CACHET.md"><strong>📖 What is Cachet?</strong></a>
  &nbsp;·&nbsp;
  <a href="./documentation/USING-CACHET.md"><strong>🚀 Quickstart</strong></a>
  &nbsp;·&nbsp;
  <a href="./CONSISTENCY.md">Consistency model</a>
  &nbsp;·&nbsp;
  <a href="#capabilities">Capabilities</a>
  &nbsp;·&nbsp;
  <a href="#benchmarks">Benchmarks</a>
</p>

<p align="center">
  <sub>Apache 2.0 · Go 1.27 · MySQL/MyRocks + Valkey or Redis<br/>
  <strong>Pre-1.0.</strong> The <a href="#capabilities">capabilities table</a> marks what ships today.</sub>
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
| **Measured correctness** | **Live SLO** <sub>roadmap</sub> | ❌ | ❌ | ❌ |
| Stampede protection | **Leases** <sub>roadmap</sub> | Partial | ❌ | ❌ |
| Self-tuning admission | **Per-key r:w** <sub>roadmap</sub> | ❌ manual | Heuristic | ❌ |

**Every cache on that list asks you to trust it. Cachet is the only one that proves it.**

### Exact invalidation, on two paths

Cachet invalidates the rows a write actually touched, not the table it might have touched.

- **On the write path**, after commit and before the acknowledgement. By the time your write returns,
  the stale entry is already gone. A conditional write (`UPDATE … WHERE tenant_id = ?`) resolves its
  affected rows inside the transaction with `SELECT … FOR UPDATE` and invalidates exactly those.
- **From the binlog**, as a backstop — and as the only path that catches writes made straight to the
  database by a migration or an admin script.

Both are versioned compare-and-set operations, so replaying the binlog is idempotent and a restarted
tailer cannot undo newer state. When a predicate matches more rows than the resolution budget allows,
Cachet says so in the response rather than silently doing less: the write still commits, the affected
keys fall back to the binlog path, and the caller is handed the staleness bound that now applies.

### Leases — bounded origin load <sub>`roadmap`</sub>

On a miss, exactly one caller gets a token to fill that key. Concurrent callers wait briefly, then
read the filled value. Origin load per key is bounded at ~1 per lease interval **regardless of
concurrency** — not best-effort, by construction.

Deduplicating concurrent fills — the common approach — fixes *ordering*: a slow fill can't overwrite
a newer value. It does nothing for *admission*. Ten thousand simultaneous misses on a hot key still
all reach the database, which is exactly when you can least afford them.

### Adaptive admission — no human decides what to cache <sub>`roadmap`</sub>

Cachet tracks the observed read:write ratio **per key** with a count-min sketch, and caches only
what earns it.

The usual approach is a person picking tables and a rule of thumb about read:write ratios. But
ratios aren't uniform within a table and they drift. A write-churning key in an otherwise read-heavy
table is pure cost: every write pays invalidation, every read misses. Cachet finds those keys and
stops caching them, continuously.

### Sextant — continuous consistency verification 🔭 <sub>`roadmap`</sub>

A verifier that subscribes to the invalidation stream, shadow-reads every cache replica, and detects
divergence — with **consistency tracing** that records each mutation, so "why was this stale?" has
an answer instead of a shrug.

This is the feature the category is missing. Sampling monitors tell you a violation happened, some
minutes later, without telling you why. Sextant runs continuously and keeps enough state to
reconstruct the sequence that caused any divergence it finds.

### Consistency as a per-request parameter

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

## Quickstart

```bash
make env-up        # 3 MyRocks shards + Valkey, healthy in ~15s
make seed          # deterministic fixtures
make build         # cachet, flux, cachetctl, benchctl -> ./bin
./bin/cachet -config cachet.yaml
```

Then talk to it from Go:

```go
c, err := cachet.Dial(ctx, "unix:///var/run/cachet.sock")
defer c.Close()

_, err = c.Put(ctx, "entities:1", cachet.Record{TenantID: 1, Payload: body})

got, err := c.Get(ctx, "entities:1")               // SESSION: reads your own write
got, err = c.Get(ctx, "entities:1", cachet.AtLevel(consistency.Strong))
got, err = c.Get(ctx, "entities:1", cachet.WithinStaleness(2*time.Second))
```

No token appears anywhere — the client carries your session for you, including across a service
boundary. `make demo` brings the same stack up with Prometheus and a provisioned Grafana dashboard.

Full walkthrough: **[Quickstart →](./documentation/USING-CACHET.md)**

## Capabilities

Cachet is pre-1.0 and developed in the open. This table is the contract: everything marked
**Available** is implemented, tested against a real MySQL + Valkey stack, and covered by the
consistency conformance suite.

| | Capability | |
|---|---|---|
| **Available** | Integrated read cache for sharded MySQL, with read-through fill | ✅ |
| | Exact invalidation on the write path — after commit, before the ack | ✅ |
| | Exact invalidation from the binlog, with durable checkpoints and idempotent replay | ✅ |
| | Conditional writes that resolve their affected rows inside the transaction | ✅ |
| | Four consistency levels, selected per request | ✅ |
| | Session tokens: read-own-writes, read-own-inserts, read-own-deletes, monotonic reads | ✅ |
| | Causal propagation across service boundaries via OpenTelemetry baggage | ✅ |
| | Negative caching, with read-own-inserts over a cached absence | ✅ |
| | Cache sharding independent of database sharding | ✅ |
| | Proportional circuit breaker — sheds a fraction of traffic to an unhealthy node | ✅ |
| | Go SDK (`cachet-go`) that carries the session for you | ✅ |
| | Operator CLI (`cachetctl`) — health, routing, key inspection, manual invalidation | ✅ |
| | Prometheus metrics and a provisioned Grafana dashboard | ✅ |
| **Roadmap** | Leases — origin load per key bounded regardless of concurrency | ⬜ |
| | Adaptive per-key admission driven by observed read:write ratio | ⬜ |
| | Sextant — continuous consistency verification and a live SLO per level | ⬜ |
| | Shadow mode — measure your consistency before changing any application code | ⬜ |

**On the roadmap items:** they are described above because they are what Cachet is *for* — the
reasons the architecture is shaped the way it is. They are not implemented yet, and nothing in this
repository pretends otherwise.

## Guarantees, and how they are checked

Every level's promise — and every documented *non*-promise — is executed as a cell of a conformance
matrix across all four levels. The suite also runs the invalidation-dependent cells against a
deliberately naive cache and **requires them to fail**, because a consistency test that has never
failed is proving nothing.

```bash
make test-consistency     # the matrix, plus the test that proves the matrix works
```

Two results worth stating plainly, because they cut against the product's own pitch:

- **A TTL-only cache reaches a better hit rate than a correct one**, because it has stopped noticing
  writes. At a four-hour TTL it reached 93.9% and 46 origin QPS; exact invalidation gives some of
  that back — 89.7% and 78 QPS. It is "perfect" precisely to the extent that it is wrong, and that
  trade is published rather than hidden.
- **Read-own-writes holds even with no invalidation at all**, carried entirely by the session
  watermark. That is the payoff of watermarking on the fill version rather than the row version.

## Benchmarks

Every row below is regenerated by `make bench-report` from JSON in `bench/results/`. No number here
is typed by hand, and the empty rows stay empty until the capability that fills them exists.

**Origin QPS** is steady-state database load — the cost metric the caching claim actually rests on.

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
(760 → 145 origin QPS), and exact invalidation cut staleness from **10.07 s to 34.98 ms —
288× better** — while giving back some hit rate as the price of correctness.

**The p99 column is not a result yet, and we say so rather than rounding it into one.** The
run-to-run spreads swamp every difference (TTL-only ranged 10.35–77.18 ms). When the spread exceeds
the effect, nothing has been measured. Two runs — one of each configuration — would have produced a
confident and false claim.

**Why the cache does not help p99 *here*, and why that is expected.** At 500 rps against three idle
MySQL shards, an uncached point lookup is already fast; the cache removes a database round trip that
was not the bottleneck. The cost it removes is *load*, not wall time. A p99 win should appear once
the origin is under enough pressure to queue, which is what the stampede workload is built to create
— and that measurement arrives with leases.

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

## Compatibility and versioning

| | |
|---|---|
| **Database** | MySQL 8.0 with MyRocks (Percona Server). InnoDB is supported and CI-tested |
| **Cache** | Valkey 8 (default) or Redis — protocol- and Lua-compatible ([ADR 0002](./docs/adr/0002-redis-vs-valkey.md)) |
| **Go** | 1.27+ for the SDK |
| **Transports** | gRPC over TCP and Unix sockets, both first-class |

Semver on every artifact, with the gRPC protocol versioned independently as `cachet.v1`. Engine and
SDK negotiate on connect, so a mismatch is a startup error rather than a puzzling failure later.

**A change to the consistency model is always a major version.** The guarantee is the contract.

## Contributing

Issues and pull requests are welcome. [`CONTRIBUTING.md`](./CONTRIBUTING.md) has the engineering
standards and the CI gate; the short version is that tests come before the code they cover, and any
decision that would otherwise be re-argued gets an ADR.

## Licence

[Apache 2.0](./LICENSE).

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
