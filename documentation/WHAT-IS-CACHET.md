# What is Cachet?

> **Read this first.** It explains the intent, the problem, and why the approach is different.
> To actually run it, see [USING-CACHET.md](./USING-CACHET.md).
> Status of every claim below: [README → Status](../README.md#status).

---

## In one sentence

**Cachet is an integrated read cache for sharded OLTP databases that continuously proves its own
correctness — and reports it as a number.**

---

## The problem

Every team that outgrows a single database ends up caching in front of it. Almost none of them can
tell you how often that cache serves stale data.

That is not sloppiness. It is a **layering** problem:

> The cache lives in the application. The application only sees SQL.

When your service issues `UPDATE orders SET status = ? WHERE customer_id = ?`, it does not know
which rows that statement touched. So it guesses:

- **Blow away the whole table** — correct, and it destroys the hit rate.
- **Set a short TTL and hope** — cheap, and it does not make you correct. A 30-second TTL does not
  fix staleness; it just shortens the window in which you are wrong, from unbounded to 30 seconds.

Which leaves two bad options, and teams oscillate between them:

| Option | What you get | What it costs |
|---|---|---|
| Cache aggressively | High hit rate, low DB load | Stale reads you cannot quantify — until an incident forces the TTL down |
| Don't cache | Correctness | Read capacity that grows faster than traffic; read replicas multiply cost without fixing staleness, because a replica is just a slower way to be stale |

**Neither option gives you a number.** Nobody can answer "what fraction of your reads were stale
last week?" That gap is what Cachet exists to close.

---

## The core idea

Move the cache **into the data layer**, where writes are actually visible.

```
   PROXY MODEL (ReadySet, PolyScale)        INTEGRATED MODEL (Cachet)

   app ──► proxy ──► database               app ──► query engine ──► storage engine
             │                                          │                  │
             └─ sees: SQL text                          └─ sees: affected row keys
                infers: "maybe this table"                 knows: exactly these 3 rows
                gives:  eventual consistency               gives: read-own-writes
```

A proxy sits outside the database and can only *infer* what a write touched. A query engine that
owns the read path *receives* the exact set of affected row keys plus a commit timestamp — so
invalidation is precise, and can happen on the write path before the write is acknowledged.

Precise invalidation is the foundation. Everything else is what it makes possible.

### The one invariant everything rests on

> Every cache entry carries a version. Every mutation of an entry — fill, invalidate, tombstone —
> is a compare-and-set against that version. **A lower version never overwrites a higher one.**

That single rule handles racing fills, out-of-order invalidation, duplicate CDC delivery, and
tailer restarts replaying old events. It is enforced in Lua, server-side, so it holds even when
concurrent clients disagree. See [`CONSISTENCY.md`](../CONSISTENCY.md) for the normative model.

---

## Why it is better

| | Cachet | ReadySet | PolyScale | DIY Redis |
|---|---|---|---|---|
| Invalidation | CDC + **exact write-path** | Streaming dataflow | Heuristic | Hand-rolled |
| Consistency | **Read-own-writes, tiered** | Eventual | Probabilistic | Undefined |
| **Measured correctness** | ✅ **Live SLO** | ❌ | ❌ | ❌ |
| Stampede protection | ✅ **Leases** | Partial | ❌ | ❌ |
| Self-tuning admission | ✅ **Per-key r:w** | ❌ manual | Heuristic | ❌ |

**Every cache on that list asks you to trust it. Cachet is the only one designed to prove it.**

Four things follow from sitting inside the data layer:

### 1 · Exact invalidation, not inference

Two paths, deliberately separable so neither gets credit for the other's work:

- **Synchronous write path** — after commit, before ack. A committed write is invisible to other
  sessions for microseconds.
- **CDC tailer (`flux`)** — reads the MySQL binlog as a backstop, and as the *only* path for writes
  made directly to the database by migrations and admin scripts.

Both are versioned compare-and-set, so replaying the binlog is idempotent and a restarted tailer
cannot undo newer state. That is not an aspiration — it is an executed test (`make test-e2e`,
`make test-chaos`): the tailer is killed mid-stream with writes in flight, restarted from its
checkpoint, and then deliberately rewound so it replays events it has already applied.

### 2.5 · A cache ring that is not the database's ring

Cache entries are routed by their own consistent-hash ring, independent of database sharding. If the
two were correlated, every key held by a failed cache node would miss to the *same* database shard —
one node's failure becoming one shard's overload, the exact hot-spot a cache is supposed to prevent.
Independent, those misses spread across every shard and the database sees a uniform bump.

An unhealthy node is handled by a **proportional** circuit breaker rather than a latch. A node
failing 30% of the time is still answering 70% of its reads; tripping it fully open throws that away
and delivers the load step to the origin as a cliff. Shedding scales with the observed failure rate
and is capped below 100%, so probe traffic always survives to notice recovery.

The asymmetry matters: **reads and fills are shed, invalidations never are.** Shedding a read costs
hit rate. Shedding an invalidation costs correctness.

### 2 · Consistency as a per-request parameter

| Level | Guarantee | Use for |
|---|---|---|
| `STRONG` | Bypasses the cache entirely | Money, auth |
| `SESSION` *(default)* | Read-own-writes + monotonic reads | Almost everything |
| `BOUNDED(t)` | Staleness ≤ t, measured from **commit** | Feeds, counts, listings |
| `EVENTUAL` | Best effort | Recommendations |

The non-obvious design decision: a session watermark is checked against the **fill version** (the
database state the entry was filled from), not the **row version**. Watermarking on the row version
collapses the hit rate to near zero on any shard taking writes. Watermarking on the fill version is
what makes read-own-writes affordable — and it works so well that read-own-writes holds with *no
invalidation at all*, carried entirely by the watermark.

The guarantee is carried by a **token held by the client**, not by the server. That is what lets it
survive a reconnect, an engine failover, and a hop into another service — and it is why the Go SDK
carries the token for you rather than leaving it as something to remember.

Every level, and every *non*-guarantee, is executed as a cell of a conformance matrix. The suite also
runs the invalidation-dependent cells against a deliberately naive cache and **requires them to
fail**, because a consistency test that has never failed is proving nothing.

### 3 · Leases — origin load bounded by construction

On a miss, exactly one caller gets a token to fill that key. Concurrent callers wait briefly, then
read the filled value. Origin load per key is bounded at ~1 per lease interval **regardless of
concurrency**.

The common alternative — deduplicating concurrent fills — fixes *ordering*: a slow fill cannot
overwrite a newer value. It does nothing for *admission*. Ten thousand simultaneous misses on a hot
key still all reach the database, which is precisely when you can least afford them.

Asserted by `make test-e2e`: 500 concurrent readers of one invalidated hot key produce **one**
origin read; with waiting disabled, 198 of 200 reach the database.

### 4 · Sextant — continuous consistency verification 🔭

A verifier that subscribes to the invalidation stream, shadow-reads every cache replica, and detects
divergence — with **consistency tracing** that records each mutation, so "why was this stale?" has
an answer instead of a shrug.

This is the piece the category is missing. Sampling monitors tell you a violation happened, some
minutes later, without telling you why. Sextant runs continuously and keeps enough state to
reconstruct the sequence that caused any divergence it finds, then publishes a **measured SLO per
consistency level**. Not a promise in a document. A live number.

**Shadow mode** is the part worth trying first: point it at a deployment your application is not
reading through, and it reports what your consistency *would have been* — no code change, no risk.
It never mutates the cache it observes, which is asserted by a test and enforced by the type it is
given.

---

## What Cachet is deliberately *not*

- **Not a transparent SQL proxy.** That forfeits exact invalidation. It is the trade we refuse.
- **Not a query-result cache.** Point lookups and row ranges. Complex joins are ReadySet's job.
- **Not a write cache.** Writes go to the database. Always.
- **Not a database.** It never becomes the source of truth.

---

## Why MySQL + MyRocks

MyRocks is an LSM storage engine: a read may touch several SST levels, so read latency is **higher
and more variable** than a B-tree's. That makes the cache work harder for its place in the stack —
the value of a cache hit is greater, and the difference is measurable. The **delta in cache value
between MyRocks and InnoDB** is a number nobody has published, and Cachet is built to measure it.

---

## The honesty rule

This project's entire thesis is measured correctness, so the benchmarks are held to the same
standard as the product:

> **When the run-to-run spread exceeds the effect, nothing has been measured.**

Cachet's own read-p99 improvement is reported as *not measurable*, because the run-to-run spreads
overlap. That row stays honest in the README rather than being quietly rounded
into a win. A benchmark table where the correct configuration wins every column is a table nobody
should believe — and Cachet's does not.

See [`docs/cachet-benchmarking.md`](../docs/cachet-benchmarking.md) for the full methodology.

---

## Where to go next

| You want to | Read |
|---|---|
| Run it, call it, configure it | [USING-CACHET.md](./USING-CACHET.md) |
| The exact guarantees, normatively | [`CONSISTENCY.md`](../CONSISTENCY.md) |
| How every number is produced | [`docs/cachet-benchmarking.md`](../docs/cachet-benchmarking.md) |
| Why each major decision was made | [`docs/adr/`](../docs/adr/) |
| What ships today | [README → Capabilities](../README.md#capabilities) |
