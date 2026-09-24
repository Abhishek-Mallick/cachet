# ADR 0008 — One row path, and what a declaration may say

**Status:** Accepted · 2026-09-24

## Context

[ADR 0005](./0005-declared-table-descriptors.md) made a table's shape a declaration and
[ADR 0006](./0006-row-encoding-and-entry-fingerprint.md) made a cache entry carry an opaque row plus
a fingerprint. [ADR 0007](./0007-two-wire-protocols-at-once.md) put a generic row on the wire. Under
all of that, the engine still read and wrote through a typed `Record` whose five fields were the
fixture table's columns, with `internal/engine/row.go` translating between the two. That file's doc
comment said it existed to be deleted.

Two protocols and one fixture-shaped engine is a system that can *describe* anybody's table and
serve nobody's.

## Decision

**The engine's own operations are in rows, and both protocols are adapters over them.**

`GetRow`, `BatchGetRows`, `PutRow`, `DeleteRow` and `UpdateRowsWhere` are the engine. Routing,
leases, admission, invalidation and every consistency rule live there, once. `cachet.v2` maps onto
them; `cachet.v1` is a shim that serves a table of the fixture's shape under any name and refuses
anything else, including an engine serving several tables — `Record` has no field to say which one
it meant.

**Each table is routed on its own ring.** Routing is derived from the key and the key is namespaced
by table, so `orders:5` and `users:5` hash to different positions. A single ring shared by every
table would be correct only when every table happened to share a topology. Tables name a topology;
topologies name shards.

**A declaration may say anything the storage path can carry, and boot refuses the rest.** Checked
against `INFORMATION_SCHEMA` before a request is accepted: columns and nullability, a primary key
column's collation, and every declared predicate's index coverage. Three things are refused outright
rather than half-supported — batching and conditional writes on a composite primary key, because
`WHERE (a,b) IN ((?,?),…)` is a row constructor MyRocks plans differently; and a generated column,
which is simply not declared, leaving the cache holding less than the whole row and the proxy
refusing `SELECT *` for that table.

**`parseTime` is forced off on every connection, whatever the operator's DSN says.** With it on the
driver parses DATETIME and TIMESTAMP into `time.Time` and returns its own rendering —
`2026-02-03T04:05:06.789Z` where MySQL sent `2026-02-03 04:05:06.789`. A cache entry would hold a
string the database would never produce, the proxy would answer a client with it, and two reads of
one row would differ depending on which was cached. Carrying the bytes MySQL sent is the property
the row encoding rests on; it cannot be left to a query parameter.

**Arguments are bound per column, not per value.** A `[]byte` is sent with character set `binary`,
and MySQL refuses to build a JSON value from one. A DECIMAL or DATETIME written that way is coerced
instead of refused, which is worse. Only a genuinely binary column is handed bytes.

## Consequences

- `internal/engine/row.go`, `internal/engine/key.go`, `internal/engine/affected.go`,
  `internal/storage/affected.go`, `storage.Record` and the typed `Get`/`BatchGet`/`Put`/`Delete` are
  gone. The only remaining translation is the v1 shim, which goes with v1 at 1.0.
- The conformance matrix runs per fixture — `entities` over v1, `entities` over v2, and a table with
  a string primary key, a nullable column, a DECIMAL and a version column called `row_version`. A
  guarantee that held only for the fixture would have been a guarantee about the fixture.
- Four defects surfaced only once a table that is not the fixture existed: the proxy refused every
  string-valued bound parameter; an empty string was written as SQL NULL; a primary key's declared
  collation was never checked against the database; and JSON could not be written at all. None was
  reachable from a five-column integer-keyed table, which is what a single fixture buys you.
