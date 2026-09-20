<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./.github/assets/cachet-logo.png">
    <img src="./.github/assets/cachet-logo-on-light.png" alt="Cachet" width="340">
  </picture>
</p>

<p align="center">
  <strong>An integrated read cache for sharded OLTP databases<br/>
  that proves its own correctness — and reports it as a number.</strong>
</p>

<p align="center">
  <a href="https://github.com/Abhishek-Mallick/cachet#quickstart"><strong>Quickstart</strong></a>
  &nbsp;·&nbsp;
  <a href="./documentation/WHAT-IS-CACHET.md"><strong>What is Cachet?</strong></a>
  &nbsp;·&nbsp;
  <a href="./CONSISTENCY.md">Consistency model</a>
  &nbsp;·&nbsp;
  <a href="#benchmarks">Benchmarks</a>
</p>

<p align="center">
  <sub>Apache 2.0 · Go 1.27 · MySQL/MyRocks + Valkey or Redis · <strong>pre-1.0</strong></sub>
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
| Invalidation | CDC + **exact write-path** | Streaming dataflow · TTL | Heuristic | Hand-rolled |
| Consistency | **Read-own-writes, tiered** | Eventual | Probabilistic | Undefined |
| **Measured correctness** | ✅ **Live SLO** | ❌ | ❌ | ❌ |
| Stampede protection | ✅ **Leases** | Partial | ❌ | ❌ |
| Self-tuning admission | Per-key read:write | Auto, per query | Heuristic | ❌ |
| Query shapes | Point lookups only | **Joins, aggregates** | Any | Any |
| Databases | MySQL/MyRocks | **MySQL + Postgres** | Many | Any |
| Integration | A Go SDK | **Wire-compatible proxy** | Proxy | Your own code |

The bottom three rows are where the alternatives are ahead, and they are in the table for that
reason. ReadySet in particular is better at more things than Cachet is: it speaks your wire
protocol, it does Postgres, and it caches joins.

**What none of them can tell you is how consistent your reads actually were** — the information
needed to answer that does not exist in a proxy. That one gap is the whole reason Cachet exists. If
you do not need that answer, one of the others is probably the right choice.

### Consistency as a per-request parameter

| Level | Guarantee | For |
|---|---|---|
| `STRONG` | Bypasses the cache | Money, auth |
| `SESSION` *(default)* | Read-own-writes + monotonic reads | Almost everything |
| `BOUNDED(t)` | Staleness ≤ t, measured from **commit** | Feeds, counts, listings |
| `EVENTUAL` | Best effort | Recommendations |

Riak had per-request tunable consistency and the industry dropped it. It should not have.

The non-obvious part: a session watermark is checked against the **fill version** — the database
state an entry was filled from — not the row's own version. Watermarking on the row version
collapses the hit rate to near zero on any shard taking writes. The payoff is testable and
surprising: read-own-writes holds with *no invalidation at all*.

### Sextant — the verifier 🔭

```
entities:9980001 on shard1: cache fv=117287293673472000, db=117287293673537536,
behind 1m0s, violates [EVENTUAL]
  trace: 16:21:13.877 fill      v=1 src=read_fill actor=engine-1
  trace: 16:21:13.877 tombstone v=2 src=cdc actor=shard1
```

Sampling monitors tell you a violation happened, minutes later, without telling you why. Sextant
runs continuously, keeps enough state to reconstruct the sequence that caused any divergence it
finds, and publishes a measured SLO per level. That trace is the whole difference between a monitor
and a verifier.

Run it in **shadow mode** first — pointed at a deployment your application is not reading through,
it reports what your consistency *would have been*, with no code change and no risk.

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

## Install

Nothing below requires cloning this repository.

```bash
# The operator CLI — health, routing, key inspection, and `bench quick` against your own database
brew install Abhishek-Mallick/cachet/cachetctl

# Or a signed binary, verified before you run it
cosign verify-blob checksums.txt \
  --certificate checksums.txt.pem --signature checksums.txt.sig \
  --certificate-identity-regexp 'https://github.com/Abhishek-Mallick/cachet/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

```bash
# The engine, as a sidecar. Signed by digest, because a tag can be moved after signing.
docker pull ghcr.io/abhishek-mallick/cachet/cachet:v0.1.0
helm install cachet oci://ghcr.io/abhishek-mallick/charts/cachet --version 0.1.0
```

```go
import "github.com/Abhishek-Mallick/cachet/pkg/cachet"   // go get github.com/Abhishek-Mallick/cachet
```

<sub>Pre-1.0 and not yet tagged: the commands above describe the release pipeline in
`.github/workflows/release.yml`, which publishes on the first tag. Until then, build from source
with `make build`.</sub>

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

### See the problem first

```bash
./bin/cachet -config examples/staleness-demo/cachet.yaml &
go run ./examples/staleness-demo
```

Three caches against the same MySQL and the same Valkey — a TTL cache, an invalidate-on-every-write
cache, and Cachet — where the only thing that changes is how each decides an entry is wrong. The
middle one is the interesting one: it is what most teams run, and it is still stale after a
conditional write. [**examples/staleness-demo →**](./examples/staleness-demo/)

## Features

| | |
|---|---|
| **Exact invalidation** | The write path knows which rows changed and invalidates them before your write is acknowledged. Conditional writes resolve their affected rows inside the transaction. The binlog is the backstop, catching migrations and admin scripts. |
| **Consistency per request** | `STRONG`, `SESSION`, `BOUNDED(t)` or `EVENTUAL`, chosen per call — with read-own-writes on the hot path that does not collapse the hit rate. |
| **Measured, not asserted** | Sextant watches the cache against the database and publishes consistency per level. Every violation carries a trace that says *why*. |
| **Shadow mode** | Point it at a deployment you are not reading through and measure what your consistency *would have been* — no code change, no risk. |
| **Stampede-proof** | One caller fills a hot key; everyone else waits briefly. 500 concurrent readers of one invalidated key produce a single origin read. |
| **Self-tuning** | Per-key read:write ratios decide what gets cached, so a write-churning key in a read-heavy table stops being cached without anyone filing a ticket. |
| **Survives its own failures** | A proportional circuit breaker sheds a *fraction* of traffic to an unhealthy node. Losing the cache entirely is a correctness non-event. |
| **Built to be operated** | `cachetctl` for health, routing, key inspection and manual invalidation. Prometheus metrics and a provisioned Grafana dashboard. |

## What you actually run

Cachet is **infrastructure, not a library**. You run a process — as a sidecar over a Unix socket, or
as a shared service tier over TCP — and your application talks to it through a thin client.

| | |
|---|---|
| `cachet` | The query engine. The process your application reads through |
| `flux` | The CDC tailer, streaming the binlog as an invalidation backstop |
| `sextant` | The consistency verifier. Run it in shadow mode first |
| `cachetctl` | The operator CLI |
| `cachet-go` | A Go module — `go get github.com/Abhishek-Mallick/cachet/pkg/cachet` |
| `cachet-proxy` | Speaks the **MySQL wire protocol**, for applications that will not take an SDK |

The SDK is Go today. The contract is gRPC (`cachet.v1`), so any language that can generate a client
can talk to it — but port the session-token handling first, because that is what carries the
guarantee. [More on that →](./documentation/WHAT-IS-CACHET.md)

### No SDK? Point your MySQL client at the proxy

```bash
cachet-proxy -config cachet.yaml -listen :3307
mysql -h 127.0.0.1 -P 3307 -u cachet -p       # an ordinary client, no code change
```

It carries **writes as well as reads**, which is what separates it from a read-through cache: inside
the write it resolves and invalidates exactly the row that changed, and maintains the `version`
column your application has never heard of. A write it cannot resolve to specific rows is **refused
with an error** rather than forwarded, because forwarding it would leave stale entries that no
invalidation can ever reach.

The trade is the session. A bare SQL connection has nowhere to hold a token, so a *connection* is
the session — read-own-writes within a connection, and nothing stronger across a pool. Applications
that need the guarantee to follow a request between services want the SDK.

## Guarantees, and how they are checked

Every level's promise — and every documented *non*-promise — is executed as a cell of a conformance
matrix across all four levels. The suite also runs the invalidation-dependent cells against a
deliberately naive cache and **requires them to fail**, because a consistency test that has never
failed is proving nothing.

```bash
make test-consistency     # the matrix, plus the test that proves the matrix works
make test-chaos           # injected faults, and what explains each one
```

Failure behaviour gets the same treatment. [`FAULTS.md`](./FAULTS.md) is generated from injected
faults, and the bar for an entry is not that Cachet survived — it is that the entry names the
command or metric telling an operator *why*. Each one also carries the evidence its injection
actually fired, since a toxic that silently failed to apply produces a green test asserting nothing.

Two results worth stating plainly, because they cut against the product's own pitch:

- **A TTL-only cache reaches a better hit rate than a correct one**, because it has stopped noticing
  writes. At a four-hour TTL it reached 93.9% and 46 origin QPS; exact invalidation gives some of
  that back — 89.7% and 78 QPS. It is "perfect" precisely to the extent that it is wrong, and that
  trade is published rather than hidden.
- **Read-own-writes holds even with no invalidation at all**, carried entirely by the session
  watermark. That is the payoff of watermarking on the fill version rather than the row version.

## Benchmarks

Every row below is regenerated by `make bench-report` from JSON in `bench/results/`. No number here
is typed by hand.

**The empty rows have working implementations and no published figure.** They stay blank until they
can be measured on a host that is not a laptop VM — filling them from a noisy machine would produce
numbers that look like measurements and are artefacts. The feature claims above rest on executed
tests — `make test-consistency`, `make test-e2e`, `make test-chaos` — not on this table.

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

**The result so far is load, not latency.** With exact invalidation, steady-state database load fell
from **760 to 78 origin QPS — a 90% reduction** — and staleness fell from **10.07 s to 34.98 ms,
288× better**, while giving back some hit rate as the price of correctness.

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

**[📚 cachet.buildlab.in](https://cachet.buildlab.in)** — the documentation site. Source in
[`web/`](./web); run it locally with `cd web && npm install && npm run dev`.

| Doc | What it covers |
|---|---|
| 📖 [**What is Cachet?**](./documentation/WHAT-IS-CACHET.md) | The intent, the problem it solves, and why the approach is better. **Start here.** |
| 🚀 [**Using Cachet**](./documentation/USING-CACHET.md) | Quick start, configuration, the gRPC API, consistency levels, benchmarking, troubleshooting |
| [`CONSISTENCY.md`](./CONSISTENCY.md) | The normative consistency model — every level's guarantee, non-guarantee, and the test that catches its violation |
| [`FAULTS.md`](./FAULTS.md) | Injected faults, and the command or metric that explains each one |
| [`CONTRIBUTING.md`](./CONTRIBUTING.md) | Engineering standards and the CI gate |
| [`CHANGELOG.md`](./CHANGELOG.md) | What changed, and the rule that a consistency change is a major version |
| [`SECURITY.md`](./SECURITY.md) | Reporting, the threat model, and how to verify a release |
| [`deploy/helm/cachet/`](./deploy/helm/cachet/) | The Helm chart, and why it will not deploy your sidecar for you |

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
