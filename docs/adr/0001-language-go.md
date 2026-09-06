# ADR 0001 — Go as the implementation language

**Status:** Accepted · 2026-09-05

## Context

Cachet is an I/O-bound system: thousands of concurrent requests, each fanning out to Redis and one to
three MySQL shards, each under a deadline. It ships as a sidecar next to every application pod, so
footprint and start time are product constraints, not preferences. Its core dependency is MySQL
binlog replication. Its central claim is correctness under concurrency.

The realistic candidates were Go, Rust, and Java.

## Decision

**Go.**

## Rationale

- **Concurrency ergonomics.** Goroutines start at ~2 KB and the runtime parks them on network waits,
  so the natural blocking, linear code *is* the async design. A thread-per-request language forces a
  callback or future model — a different, harder program.
- **`context.Context` is standardised.** Deadline and cancellation propagation across a whole request
  tree, accepted by `database/sql`, `go-redis` and gRPC alike. Tail latency is mostly about giving up
  correctly, and this is the mechanism for it.
- **The ecosystem for this exact system exists.** `go-mysql-org/go-mysql` is a mature binlog
  replication client; `go-redis/v9` covers Lua, `EVALSHA` and cluster; gRPC, Prometheus, OpenTelemetry
  and testcontainers are all first-class *simultaneously*. Cachet must hand-write a verifier, a lease
  protocol, an admission sketch and a benchmark harness. Everything else should be a `go get`.
- **The toolchain targets this project's largest risk.** `go test -race` in CI turns "we believe the
  CAS path is correct" into "CI would fail if it weren't". Native fuzzing suits the version/CAS logic;
  `pprof` and `testing.B` mean every phase can end in a measurement.
- **Single static binary, trivial cross-compile.** This is what makes multi-arch images, a
  Homebrew-able `cachetctl` and a ~50–100 MB sidecar realistic.

## Alternatives

**Rust** — better absolute tail latency and no GC, but roughly 2–3× the implementation time and,
decisively, a materially weaker MySQL binlog ecosystem. That turns a component timeboxed at five days
into the project's main risk. Cachet's differentiator is provable consistency, not lowest possible
latency; spending the budget on a borrow checker instead of on Sextant optimises the wrong variable.

**Java** — Debezium is the best CDC tooling in existence, and it is JVM. But the delivery model puts a
Cachet process beside every application pod, and a JVM per pod (RSS plus warmup curve) is incompatible
with "negligible added latency". The topology decision excludes the JVM more firmly than any benchmark
would.

**C++, Node, Python** — no memory safety, no real parallelism, and wrong tool respectively.

## Consequences

- **Accepted cost: a garbage collector.** Go's GC is concurrent with sub-millisecond typical pauses,
  but it is not zero, and it will be visible at p99.9 on an allocation-heavy path. Mitigation: keep
  the hot path allocation-light (`sync.Pool`, preallocation, no `[]byte`↔`string` copies), set
  `GOMEMLIMIT`, and **record the GC pause distribution alongside latency in every benchmark**. If it
  is bad, that is a published finding, not an embarrassment.
- **Accepted cost: verbose error handling and weak abstraction facilities.** Both are tolerable
  because `CONTRIBUTING.md` bans the abstractions we would otherwise reach for.
- No compile-time memory-safety proof; we rely on `-race` in CI and on Go having no manual free.
- Revisit only if benchmarks show GC pauses dominating p99.9 *and* the binlog ecosystem in another
  language has matured. Neither is true today.
