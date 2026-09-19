# Why cache invalidation belongs in the data layer

A three-act demonstration you can run in about two minutes. Each act caches the same rows in the
same Valkey against the same MySQL. **Only the invalidation strategy changes.**

| | Strategy | Result |
|---|---|---|
| Act 1 | A TTL cache | Serves data the database has already changed |
| Act 2 | Invalidate on every write | Correct for point writes. Stale for conditional ones |
| Act 3 | Cachet | Correct, and the untouched entries stay cached |

Act 2 is the one worth watching. It is what most teams actually run, it is written by people who
understand caching, and it is still wrong — because an application issues a statement and never
learns its consequences.

## Run it

```bash
make env-up                                              # MySQL + Valkey, ~15s
make build

./bin/cachet -config examples/staleness-demo/cachet.yaml &   # the engine, for Act 3

go run ./examples/staleness-demo                         # all three acts
```

Useful flags:

```bash
go run ./examples/staleness-demo -act=ttl      # one act at a time
go run ./examples/staleness-demo -act=naive
go run ./examples/staleness-demo -act=cachet
go run ./examples/staleness-demo -pause=0      # no pauses, for a quick check
go run ./examples/staleness-demo -pause=2s     # slower, for a screen recording
```

Each act uses its own customer and its own id range, so any act can be run alone and repeatedly.

## What is real here, and what is staged

**Real.** The database, the cache, the engine, the conditional write, and every read. Act 3 goes
through the same gRPC API and the same SDK an application would use.

**Staged.** The naive caches in Acts 1 and 2 are implemented in this file, in about thirty lines, so
that nothing is hidden behind a framework. They are not strawmen — read them and check.

**The exit code means something.** Acts 1 and 2 are *supposed* to serve stale data; that is the
demonstration, and it does not fail the program. Only Act 3 failing exits non-zero, so this can run
as a test of itself.

## Why Act 3's cache probes ask at EVENTUAL

Act 3 checks "is this entry still cached" at `EVENTUAL` rather than the default `SESSION`. That
question is about the cache, not about what a session may be served — and a long-lived `SESSION`
client reading several keys on one shard currently reports a miss on all of them, because each read
advances the session watermark to that read's fill version. Probing at `SESSION` would make Act 3
look like an invalidation failure when the invalidation is exactly right.

The correctness assertions in Act 3 use the default level.
