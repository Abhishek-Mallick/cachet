# Changelog

Notable changes to Cachet. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [semantic versioning](https://semver.org/) — with one addition specific to this
project:

> **A change to the consistency model is a major version, always.** Not a minor one, and never a
> patch. The guarantees in [`CONSISTENCY.md`](./CONSISTENCY.md) are the contract; the API is merely
> how you reach them.

## [Unreleased]

Pre-1.0. Nothing has been tagged yet, so everything below is what exists on `main`.

### The read path

- **Integrated read cache for sharded MySQL/MyRocks**, addressed by row key, with hybrid logical
  clock versioning per shard.
- **Exact invalidation on two separable paths.** The write path invalidates after commit and before
  the acknowledgement; the binlog tailer (`flux`) is the backstop for writes the engine never saw.
  Both are versioned compare-and-set, so replay is idempotent.
- **Conditional writes resolve their affected rows inside the transaction** with `SELECT … FOR
  UPDATE`, degrading past a per-shard budget rather than guessing.
- **Four consistency levels chosen per request** — `STRONG`, `SESSION`, `BOUNDED(t)`, `EVENTUAL` —
  with session tokens that propagate across service hops.
- **Negative caching**, including read-own-inserts over a cached absence.
- **`cachet-proxy`**: the MySQL wire protocol, so an application can use Cachet with no SDK and no
  code change. It carries writes as well as reads, so it invalidates exactly the row a write changed
  and maintains the `version` column on the application's behalf. A write it cannot resolve to
  specific rows is refused rather than forwarded.
- **Leases**: origin load for a key is bounded regardless of how many callers miss at once.
- **Adaptive admission**: per-key read:write ratios decide what is cached, with a hysteresis band
  and a minimum dwell. Off by default.
- **Proportional circuit breaker** per cache node — a node failing 30% still serves 70%, because
  tripping fully open converts a partial degradation into a cliff.
- Cache ring independent of database sharding, so one dead shard does not darken a third of the
  keyspace.

### Rows of any shape

- **A table's shape is a declaration, not a compiled-in assumption.** Cache entries carry an opaque
  encoded row plus a fingerprint of that shape, so an entry survives a column being added, renamed,
  retyped or reordered without a flush, and without a rolling deploy in which two engines disagree
  about the shape corrupting anything
  ([ADR 0005](./docs/adr/0005-declared-table-descriptors.md),
  [ADR 0006](./docs/adr/0006-row-encoding-and-entry-fingerprint.md)).
- **Storage, keys and the binlog tailer all read the declaration**: statements are built at boot,
  indexes validated against the live table, and invalidation keys built from the descriptor's own
  key columns.
- **Composite and non-integer primary keys.** The key grammar escapes its values, so splitting is
  unambiguous; a single-integer key still renders exactly as it always did, which leaves existing
  entries where the hash ring already put them. String key columns under a case- or
  accent-insensitive collation are refused rather than cached, because MySQL would treat two keys as
  one and Cachet would not.
- **Generic rows on the wire, over a protocol the server describes.** The SDK gains `GetRow`,
  `BatchGetRows` and `PutRow` against table descriptors it learns at the handshake, rather than
  against a schema you configure it with a second copy of.
- **Any column type Cachet carries, proven end to end.** DECIMAL, DATETIME, TIMESTAMP and JSON are
  carried as the text MySQL produced — a round trip through `time.Time` loses the distinction
  between what was stored and what a driver chose to format, and DECIMAL through `float64` loses
  money. `parseTime` is forced off on every connection whatever your DSN says, because a cached
  DATETIME rendered by the driver is a string the database would never produce.
- **Composite primary keys** round-trip on reads, writes and deletes. Batching and conditional
  writes on them are refused rather than half-supported: `WHERE (a,b) IN ((?,?),…)` is a row
  constructor MyRocks plans differently.
- **One engine serves several declared tables.** Each is routed over the shards its topology names,
  because routing comes from the key and the key is namespaced by table — a single ring shared by
  every table would be correct only by coincidence. Reads, writes, batches and conditional writes
  all run against the declaration: there is no typed path left for a fixture table.
- **Conditional writes on any declared shape.** A predicate matches and sets the columns the
  deployment declared, resolves its affected keys exactly inside the transaction, and stamps the
  table's own version column — whatever it is called.
- **`tables:` and `topologies:` in config — and no built-in table.** What Cachet caches is
  declared: the table, its columns and their order, the primary key, the version column, and the
  conditional-write shapes the deployment permits. A topology is a named set of shards, and tables
  name one rather than listing shards, because routing comes from the key and the key is namespaced
  by table. Every binary — engine, proxy, `flux`, `sextant` — reads the declaration instead of a
  name compiled into it.
- **`cachetctl config migrate`** writes the declaration Cachet used to compile in into an existing
  config, preserving your comments and settings. The shape is unchanged, so the row fingerprint is
  unchanged and every cache entry you already hold still reads as a hit.
- **The wire proxy reads the same declaration.** Its patterns, its column lists, the key it builds
  and the version column it maintains all come from the table descriptor rather than from names
  compiled into it — including composite primary keys, read in either order. It verifies the
  declaration against `INFORMATION_SCHEMA` at startup, and where the declared columns prove to be
  the whole row it can answer `SELECT *` from the cache instead of refusing it.
- **`cachet.v1` and `cachet.v2` are served at once**, from one engine, until 1.0 — a v1 write is
  visible to a v2 reader and the reverse, and the SDK negotiates the newer protocol and falls back
  to the older one against a server that does not serve it. Client and server upgrade on their own
  schedules ([ADR 0007](./docs/adr/0007-two-wire-protocols-at-once.md)).

### Proof

- **Sextant**: consistency tracing, violation detection, and a per-level SLO published as a number.
  Every violation carries the trace that explains it.
- **Shadow mode**: measure what your consistency *would have been*, against traffic Cachet does not
  serve, without changing a line of application code.
- **Conformance suite**: every level's guarantee and documented non-guarantee as a cell, including
  cells run against a deliberately broken cache that are *required to fail*. Every cell runs
  against three fixtures — the shipped table over `cachet.v1`, the same table over `cachet.v2`, and
  a table with a string primary key, a nullable column, a DECIMAL and a version column called
  something else. A guarantee that held only for the fixture would be a guarantee about the
  fixture.
- **Fault injection**: nine faults in [`FAULTS.md`](./FAULTS.md), each caught *and explained* by a
  named command or metric, each carrying evidence its injection fired.

### Tooling

- `cachetctl` — health, routing, key inspection, manual invalidation, tailer checkpoints, admission
  explanation, and `bench quick` against your own database.
- Open-loop benchmark harness with a staleness probe; every published figure regenerated by script.
- Per-commit microbenchmark history with sparklines, written by CI as a regression signal.
- Observability: RED metrics, guarantee-setting gauges, a provisioned Grafana dashboard.

### Fixed

- **A JSON column could not be written at all.** Every value was bound as bytes, which MySQL sends
  with character set `binary` and refuses to build a JSON value from. Arguments are now bound per
  column, so only a genuinely binary column is handed bytes.
- **A primary key's declared collation was never checked against the database.** Declaring
  `utf8mb4_bin` over a case-insensitive column passed every config check and produced exactly the
  silent invalidation miss the declaration rules exist to prevent — MySQL treating two keys as one
  row while Cachet treats them as two entries.
- **The proxy refused every string-valued bound parameter.** A prepared statement's string, blob
  and decimal arguments arrive as the MySQL type plus raw bytes rather than as a Go string, and the
  classifier did not recognise that shape — so a table keyed by a `VARCHAR` could never be cached
  through the proxy. Invisible until a table with a string primary key existed, because integer
  keys arrive as integers.
- **An empty string was written as SQL NULL.** A non-NULL value whose bytes were nil — which is how
  an empty `bytes` field decodes — rendered as an untyped nil, and `database/sql` turns that into
  NULL. The two are different facts, which is the reason the row encoding carries a null bitmap.
- **A long-lived `SESSION` client never hit the cache.** Reading more than one key on a shard
  produced a miss on every read, for ever, with no writes involved — 0/5 where `EVENTUAL` got 5/5.
  Each read advanced that shard's watermark to its own fill version, so reading one key made every
  other key's entry look too old to serve. Reads now advance by the row version observed. `SESSION`
  is the default level, so this made the cache useless for the most ordinary usage there is.

- **The CDC backstop dropped invalidations the cache could not accept.** On a tombstone error the
  tailer logged a warning, dropped the event, and advanced its checkpoint past it — so during a cache
  partition both invalidation paths lost the same write and the stale entry survived until its TTL,
  with nothing reporting it. It now retries, and freezes its checkpoint if the budget runs out so a
  restart replays from before the loss. Found by fault injection, not by a test that was looking
  for it.
- **Adaptive admission raised p99 to 629 ms against a 34 ms control.** The count-min sketch
  recomputed cell indices per bucket and hashed while holding a global mutex. A lock convoy surfaces
  as a tail rather than an average, which is why every correctness test passed. Found by
  benchmarking.
- **`cache.New` took 41.75 s to report one unreachable node** with a 200 ms timeout, because the
  dial timeout was hardcoded to 2 s and retries were unbounded. Now 2.87 s.
- **A cache entry omitted `tenant_id` and `status`**, so the same key returned a different record
  depending on whether the cache happened to be warm. Found by the conformance suite.

### Known limitations

- **The SDK is Go only.** The wire contract is gRPC (`cachet.v1`) and generating a client for
  another language is supported. `cachet-proxy` covers applications that will not take an SDK at
  all, at the cost of a per-connection session rather than one that follows a request.
- **The proxy forwards prepared statements.** They are correct, and they are not cached: a prepared
  statement's meaning depends on arguments the classifier has not seen.
- **Four benchmark rows have working implementations and no published figure.** They stay blank
  until they can be measured on a host that is not a laptop VM. Read p99 is reported as *not
  measurable* in the current environment rather than rounded into a win.
- **Admission is per-instance.** Each engine measures ratios from its own traffic; cross-instance
  gossip is deliberately deferred.
- **Not production-ready.** Pre-1.0, and nobody runs it in production.

[Unreleased]: https://github.com/Abhishek-Mallick/cachet/commits/main
