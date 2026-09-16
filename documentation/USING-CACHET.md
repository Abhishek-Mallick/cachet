# Using Cachet

> **What Cachet is and why:** [WHAT-IS-CACHET.md](./WHAT-IS-CACHET.md).
> **Exact guarantees:** [`CONSISTENCY.md`](../CONSISTENCY.md).
>
> ⚠️ **Cachet is pre-1.0.** This page documents what is available today; capabilities still on the
> roadmap are marked as such. See [README → Capabilities](../README.md#capabilities).

---

## What runs today

| Component | Binary | State |
|---|---|---|
| Query engine — gRPC data plane, cache-aware reads, versioned CAS fill/tombstone | `cachet` | ✅ Working |
| CDC tailer — MySQL binlog → invalidation, durable checkpoints | `flux` | ✅ Working |
| Benchmark driver — open-loop, Zipfian, staleness probe, report generator | `benchctl` | ✅ Working |
| Operator CLI — status, ring, inspect, invalidate, checkpoint | `cachetctl` | ✅ Working |
| Consistency verifier | `sextant` | ⬜ Roadmap |
| Independent cache ring + proportional circuit breaker | (in `cachet`) | ✅ Working |
| Go SDK — carries the session, propagates it via OTel baggage | `pkg/cachet` | ✅ Working |

---

## Prerequisites

- **Go 1.27.1**
- **Docker** with Compose (the test stack is 3× Percona/MyRocks shards + Valkey + Toxiproxy)
- `make`. On macOS you need **GNU Make ≥ 3.82** — the system `make` is 3.81 and silently ignores
  `.SHELLFLAGS`, which makes a failing `env-up` report success. `brew install make` and use `gmake`.

---

## Quick start

```bash
# 1 · Bring the stack up (healthy in ~14s against a 90s budget)
make env-up

# 2 · Seed deterministic fixtures (SEED_PROFILE=small|medium|large)
make seed

# 3 · Build every binary into ./bin
make build

# 4 · Run the engine against the stack
./bin/cachet -config path/to/config.yaml
```

Tear down with `make env-down` (removes volumes), or `make env-reset` to wipe, restart and reseed.

### The 90-second demo

```bash
make demo          # stack + Prometheus + provisioned Grafana dashboards
# Grafana: http://localhost:3000  (admin/admin)
```

Ten panels: guarantee settings read from the running process, RED rates, errors by gRPC code,
read/write percentiles, in-flight, success rate.

---

## Configuration

The engine takes an optional YAML file and layers environment overrides on top. Everything is
validated **at boot** — an invalid config fails to start with an error naming the offending field,
rather than surfacing on the first user request.

```bash
./bin/cachet -config cachet.yaml
./bin/cachet -version
```

```yaml
# Listen on TCP and a Unix socket simultaneously. The sidecar topology is the default,
# so both transports are first-class from the first release (ADR 0004).
listen:
  - "tcp://:9090"
  - "unix:///var/run/cachet.sock"

shards:
  - { id: shard0, dsn: "user:pass@tcp(127.0.0.1:3306)/cachet" }
  - { id: shard1, dsn: "user:pass@tcp(127.0.0.1:3307)/cachet" }
  - { id: shard2, dsn: "user:pass@tcp(127.0.0.1:3308)/cachet" }

cache:
  # Cache NODES, routed by their own ring — independent of the shards above. Losing one
  # costs only its share of the key space, spread across every shard rather than
  # concentrated on one. An empty list disables caching (the uncached baseline).
  addresses: ["10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379"]

  # Per-node proportional circuit breaker. Tuning knobs, not guarantee settings.
  breaker:
    window: 10s          # how far back health is judged
    buckets: 10          # window subdivisions; too few and shedding oscillates
    min_requests: 20     # evidence floor — 2 failures out of 2 means nothing
    failure_floor: 0.05  # error rate tolerated without shedding
    max_shed: 0.95       # MUST stay below 1; the remainder is recovery probe traffic

default_level: SESSION

observability:
  metrics_listen: ":9100"     # its own port, so metrics stay scrapable while the data plane saturates
  log_level: info
  log_format: text
  service_name: cachet

shutdown:
  drain_timeout: 15s
```

### Environment overrides

Prefix `CACHET_`, and use **two** underscores for nesting (single underscores appear inside the key
names themselves, so one separator would be ambiguous):

```bash
CACHET_CONSISTENCY__MAX_CLOCK_SKEW=500ms
CACHET_CONSISTENCY__ENTRY_TTL=30s
CACHET_OBSERVABILITY__LOG_LEVEL=debug
```

### Guarantee settings — not tuning knobs

These change what Cachet **promises**. They are logged at boot and exported as
`cachet_guarantee_setting` gauges precisely so a changed guarantee cannot go unnoticed
(`CONSISTENCY.md` §9).

| Setting | Default | What changing it changes |
|---|---|---|
| `consistency.max_clock_skew` | `250ms` | Bounds engine↔shard clock disagreement. Shortens the `BOUNDED(t)` window, so the engine stays conservative about its own clock |
| `consistency.cdc_lag_bound` | `5s` | The staleness bound applied to degraded writes and to writes made **directly to MySQL**, bypassing Cachet |
| `consistency.write_path_invalidation_budget` | `50ms` | How long synchronous invalidation may take before the write path becomes the suspect in a violation investigation |
| `consistency.max_affected_keys` | `1000` | Where a conditional write stops resolving affected keys exactly and falls back to CDC, reporting `degraded=true` |
| `consistency.max_session_shards` | — | Caps session-token size before it starts evicting watermarks |
| `consistency.entry_ttl` | `4h` | The last-resort safety net and `EVENTUAL` convergence bound. **Correctness comes from invalidation; the TTL is the backstop, not the strategy** |
| `consistency.synchronous_invalidation` | `true` | ON: a committed write is invisible to other sessions for microseconds. OFF: invalidation falls entirely to CDC and other sessions are bounded by `cdc_lag_bound` instead |

---

## Using the Go SDK

`pkg/cachet` is the supported way to talk to Cachet, and the reason is not convenience.

**The session guarantee is carried by a token, and a token nobody propagates is a guarantee nobody
has.** On raw gRPC you would thread a watermark through every call and every service hop by hand.
The ones you forgot would not fail — they would quietly return staler data than the level you asked
for, on exactly the requests where it mattered. The client carries it so "I forgot" is not a
reachable state.

```go
import (
    "github.com/Abhishek-Mallick/cachet/pkg/cachet"
    "github.com/Abhishek-Mallick/cachet/pkg/consistency"
)

c, err := cachet.Dial(ctx, "unix:///var/run/cachet.sock")
defer c.Close()

// Write, then read. No token appears anywhere — the client kept it.
if _, err := c.Put(ctx, "entities:1", cachet.Record{TenantID: 1, Payload: body}); err != nil { ... }

got, err := c.Get(ctx, "entities:1")           // SESSION by default
got, err = c.Get(ctx, "entities:1", cachet.AtLevel(consistency.Strong))
got, err = c.Get(ctx, "entities:1", cachet.WithinStaleness(2*time.Second))  // BOUNDED(2s)

if got.Found {
    use(got.Record.Payload)
}
if got.Meta.Degraded {
    // You may ignore this. It must be a decision, not an accident.
    log.Warn("served below the requested level", "reason", got.Meta.DegradedReason)
}
```

`Dial` performs the protocol handshake, so an incompatible server is a **startup** error rather than
a confusing failure on whichever request first touches the field that changed.

### Crossing a service boundary

The watermark travels in **OpenTelemetry baggage**, which every instrumented transport already
propagates. A bespoke header would mean teaching every hop about Cachet first — and the hops nobody
remembered would silently downgrade the caller.

```go
// Upstream, before calling another service:
ctx = cachet.ContextWithSession(ctx, client.Session())
ctx, err = cachet.InjectSession(ctx)

// Downstream, on an inbound request:
if w, ok := cachet.ExtractSession(ctx); ok {
    client.AdoptSession(w)   // merges, never replaces — your own writes still count
}
```

Without this, the downstream starts with an empty session and reads as if it had written nothing.
That is documented behaviour, not a bug (`CONSISTENCY.md` §4) — but it is a weaker guarantee than
the one you asked for, so it is worth doing.

### Conditional writes

```go
res, err := c.UpdateWhere(ctx, cachet.Predicate{TenantID: 1, MatchStatus: 1, SetStatus: 2})

if res.Degraded {
    // Too many rows to resolve exactly. The WRITE committed; what was given up is the exact key
    // list. Your own reads are still correct; other sessions see these keys as
    // BOUNDED(res.EffectiveStalenessBound) until the CDC tailer catches up.
}
// res.AffectedKeys is exact when Degraded is false, and EMPTY when it is true — never partial.
```

---

## The gRPC API

`cachet.v1.CacheService`. Use this directly only if you are not writing Go; otherwise prefer the SDK
above, which handles session propagation for you. Generate a client from
[`api/cachet/v1/cachet.proto`](../api/cachet/v1/cachet.proto).

| RPC | Purpose |
|---|---|
| `Handshake` | Called once on connect. Negotiates protocol version and returns the levels this server will actually serve, so a client fails fast at startup rather than on its first unsupported request |
| `Get` | Single-key read at a chosen consistency level |
| `BatchGet` | N independent reads. **Deliberately no cross-key snapshot at any level** — Cachet caches rows, not transactions |
| `Put` | Write-through. Commits to the shard, then invalidates |
| `Delete` | Removes the row and its cache entry |
| `UpdateWhere` | Conditional write. Resolves affected keys exactly inside the transaction and invalidates them before the ack; reports `degraded` and leaves them to CDC when the predicate is too large |

### Consistency levels

Passed per request. `SESSION` is the default.

| Level | Guarantee | Notes |
|---|---|---|
| `CONSISTENCY_LEVEL_STRONG` | Bypasses the cache | |
| `CONSISTENCY_LEVEL_SESSION` | Read-own-writes + monotonic reads | Requires carrying the session token |
| `CONSISTENCY_LEVEL_BOUNDED` | Staleness ≤ t | `staleness_bound` is **required**; measured from **commit**, not from read |
| `CONSISTENCY_LEVEL_EVENTUAL` | Best effort | |

### The session token — carry it

`SessionToken` is a **sparse map from shard id to HLC version**, not a scalar, because each shard
runs its own clock and versions from different shards are incomparable (ADR 0003). It is held by
the **client**, not the server — which is what lets the guarantee survive a reconnect or an engine
failover, since engines are stateless with respect to sessions.

**Every response returns an advanced token. Carry it into the next request.** A client that drops
the token silently gets a weaker guarantee than the one it asked for.

### Read the metadata

Responses carry `ReadMeta` / `WriteMeta` reporting **what actually happened**, as distinct from what
was asked for:

- `level_served` — may be weaker than the level requested
- `degraded` + `degraded_reason` — the engine could not honour the requested level. Ignoring this is
  your decision, but it must be a *visible* decision: a silent downgrade of a stated guarantee is
  exactly the failure Cachet exists to eliminate
- `cache_hit`
- `row_version` and `fill_version` — both exposed because they answer different questions.
  Conflating them is a correctness bug, not an inefficiency (`CONSISTENCY.md` §1)

Absence is a first-class answer: `GetResponse.found = false` is a **cacheable fact**, and a later
insert invalidates that negative entry.

---

## Operating it — `cachetctl`

The control plane. Five commands, each answering a question you ask during an incident. Every one
takes `-config <path>` and `-json`.

```bash
cachetctl status                          # is every cache node answering, and how fast?
cachetctl ring                            # both routing rings, and each node's share of the keys
cachetctl inspect entities:1              # where does this key live, and what do we hold for it?
cachetctl invalidate entities:1 -dry-run  # preview the blast radius
cachetctl invalidate entities:1           # the escape hatch
cachetctl checkpoint -state-dir ./.flux   # how far has each shard's tailer got?
```

`inspect` is the one you will reach for most:

```
key         entities:1
cache node  127.0.0.1:6379
shard       shard1
cached      yes — value
row ver     117253494587916288   (orders fills against each other)
fill ver    117253501905666048   (answers freshness)
payload     256 bytes
expires in  3h59m56s
```

**Both versions, always.** They answer different questions, and the gap between them above is the
design working: this row has not been written in a while (old `row ver`) but was read from the
database moments ago (recent `fill ver`). Shown only one of them, you cannot tell a stale entry from
an old row that is perfectly fresh.

`inspect` also distinguishes a **negative entry** ("we know this row does not exist") from an absent
one ("we have not looked"). They are identical to a reader and completely different to you.

`ring` deliberately shows **both** rings side by side, because the cache ring and the shard ring are
independent and the most expensive assumption you can make is that they are the same thing under two
names.

> **On `invalidate`:** it stamps the tombstone from the current clock, so it beats anything already
> in flight. Use `-dry-run` first. It is one key at a time on purpose — the blast radius of a manual
> invalidation should be something you can state out loud before you run it.

Commands that would need Sextant (`key trace`) or adaptive admission (`admission explain`) are
**absent rather than stubbed**. A control plane that answers "why was this stale?" with a placeholder
is worse than one that admits it cannot answer yet.

---

## Running the CDC tailer

`flux` reads the MySQL binlog and invalidates cache entries, as a backstop to synchronous
invalidation and as the **only** path that catches writes made directly to the database by
migrations and admin scripts.

```bash
./bin/flux -config cachet.yaml -state-dir ./.flux
```

Checkpoints are durable and written atomically, so a restart resumes exactly where it stopped.
Replay is idempotent: every mutation is a versioned compare-and-set, so a tailer replaying old
events cannot undo newer state.

**The stack requires `binlog_format=ROW` and `binlog_row_image=FULL`** — both are set in
`test/env/compose.yml`. A minimal row image omits unchanged columns, and an invalidation without the
row's version cannot take part in the compare-and-set: it could only delete unconditionally, which
reopens the delete-versus-fill race. Flux refuses to tail a table that has no `version` column for
the same reason.

Check its progress with `cachetctl checkpoint`. A shard with no checkpoint is reported as such —
"the tailer for shard2 was never started" is exactly the incident worth seeing.

---

## Testing

```bash
make test-unit          # fast, no containers, -race
make test-integration   # testcontainers: one MySQL + one Redis
make test-lua           # Lua scripts against a real Redis, under concurrency
make test-consistency   # the conformance suite — the gate that matters
make test-e2e           # end to end, over both TCP and Unix sockets
make test-chaos         # the 9 injected faults (roadmap)
make test-all           # everything, in dependency order
```

Integration tests **fail** rather than skip when the environment is down. A CI gate that goes green
while testing nothing is worse than no gate.

Also: `make lint` (vet + golangci-lint, including tagged test files), `make fmt`, `make tidy`
(verified to be a no-op), `make vuln`.

---

## Benchmarking

Every number in the README is generated from JSON. Nothing is typed by hand.

```bash
make bench          # run the suite, writing JSON to bench/results/
make bench-report   # regenerate the README table from bench/results/
make bench-guard    # fail on a p99 regression >10% vs the recorded baseline
```

Or drive `benchctl` directly:

```bash
./bin/benchctl run \
  --workload bench/workloads/w2.yaml \
  --target tcp://127.0.0.1:9090 \
  --phase 2-cdc --topology service \
  --runs 3 --host my-linux-box

./bin/benchctl probe --phase 2-cdc --samples 250 --ttl 4h --key-base 5000
```

`--host` is **required** — measuring on Docker Desktop is acceptable; letting a reader assume
otherwise is not.

`--phase` is an identifier the report generator matches to a row in the README table (`0-baseline`,
`1-ttl`, `2-cdc`, `3-exact`, `4a-leases`, `4b-admission`), not a free-form label.

### Workloads

| File | Shape |
|---|---|
| `bench/workloads/w1.yaml` | Uniform control |
| `bench/workloads/w2.yaml` | **The primary workload.** Zipfian θ=0.99, 95% reads, 10k rows, 2000 rps. Every README row comes from this file |
| `bench/workloads/w3.yaml` | Hot-key / stampede shape |

The parameters in W2 must stay **constant across configurations**. A row you cannot compare to the
row above it is decoration.

### Two things the harness gets right, deliberately

- **Open loop.** Requests are issued on schedule and measured from their *intended* start, so a
  stalled server shows up as tail latency instead of disappearing. Verified by test: a 300 ms stall
  at 1000 rps produced 202 samples and a p99 of 295 ms. A closed-loop harness reports 1 sample and a
  healthy p99 — which is exactly how a stampede would be hidden, and then its fix hidden too.
- **Scrambled Zipfian ranks.** The hot set is scattered across the id space through a bijection.
  Unscrambled, the working set is rows 1..N — physically adjacent in an LSM engine, sharing SST
  blocks — which hands MyRocks a locality advantage no real workload has.

If a run reports `Behind`, the generator could not keep up: that run measured the harness, not the
system. **Discard it.** Do not adjust it.

See [`docs/cachet-benchmarking.md`](../docs/cachet-benchmarking.md) for the full methodology.

---

## Deployment topologies

| Topology | Shape | When |
|---|---|---|
| **Sidecar** *(default)* | One engine per app pod, over a Unix socket | Lowest latency; measured ~15% better at p99 than the service tier even before caching landed |
| **Service tier** | A shared pool of engines behind TCP | Easier to operate; fewer processes |
| **Embedded** | Library in-process | Not shipped in v1 |

See [ADR 0004 — sidecar as the default topology](../docs/adr/0004-sidecar-default-topology.md).

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| `make env-up` reports success but nothing works | macOS system `make` is 3.81 and ignores `.SHELLFLAGS`, so `set -e` is inert. Use GNU Make ≥ 3.82 (`brew install make` → `gmake`) |
| Unix socket boot failure naming a path length | The path exceeds the kernel's `sun_path` limit. This is a boot error by design, not a runtime surprise |
| Engine refuses to start, naming a config field | Working as intended — validation is strict and runs before anything else starts |
| Grafana panels show "datasource not found" | The datasource needs a pinned uid; a fresh stack otherwise generates a random one |
| Staleness probe refuses to start on a key | The key does not exist in the seeded fixture. A probe that measures a cache fill and calls it staleness produces an excellent number and no information |

---

## Contributing

See [`CONTRIBUTING.md`](../CONTRIBUTING.md). The short version: `make lint` and `make test-unit`
must be green, tests come before the code they cover, and any decision that would otherwise be
re-argued gets an ADR in [`docs/adr/`](../docs/adr/).
