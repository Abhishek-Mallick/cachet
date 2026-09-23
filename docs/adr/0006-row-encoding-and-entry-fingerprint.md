# ADR 0006 — Row encoding, and the entry fingerprint

**Status:** Accepted · 2026-09-23

## Context

[ADR 0005](./0005-declared-table-descriptors.md) makes a table's shape a declaration. A cache entry
must then carry a row of that shape, and two problems follow immediately:

1. The entry has to survive the declaration changing — a column added, renamed, retyped, reordered —
   without a flush, and without a rolling deploy in which two engines disagree about the shape
   corrupting anything.
2. The compare-and-set machinery in `fill_cas.lua` is what every consistency level rests on. It must
   not have to change.

## Decision

**The entry carries an opaque encoded row plus a fingerprint of its shape.**

The Redis hash is `v` (row version), `f` (fill version), `n` (negative), `r` (the encoded row) and
`h` (the schema fingerprint). The Lua never looks inside `r`.

**The row is positional with an explicit null bitmap**, behind a format byte. Values are MySQL's own
bytes.

**The fingerprint is checked inside the Lua**, and a mismatch reads as a miss — in `read_lease.lua`
it falls through to the *lease* path.

**The tombstone carries no fingerprint.**

## Rationale

**Why opaque, and why the Lua is unchanged.** Every comparison in `fill_cas.lua` — the zero-padded
decimal strings, the rule never to call `tonumber()` on a version, the resurrection guard, the
same-row-version freshness rule — is the reason a stale fill cannot win a compare-and-set. Making
the row opaque means an arbitrary user table requires no change to any of it. Re-deriving that
machinery to save a field rename would have been the worst trade available.

**Why a null bitmap rather than protobuf.** The argument is correctness, not speed. SQL `NULL` and a
zero value are different facts, and *"this column is NULL"* is a different fact again from *"this
row does not exist"*, which the cache records separately as a negative entry. Three absences, all
distinct; proto3 scalars cannot express the first distinction without wrappers.

**Why the fingerprint check is in the Lua.** This is the whole design. A schema change invalidates
every entry at once. If each reader only learned that *after* being told "hit", every one of them
would go to the origin with **no lease protecting it** — a fleet-wide stampede at the exact moment a
deployment is already in motion. Falling through to the lease path makes a deploy cost one origin
read per key instead of all of them.

**Why the fingerprint is not in the cache key.** It would remap the hash ring on every deploy, which
is the same stampede by a different route, plus a full-cache miss.

**Why the tombstone carries none.** An invalidation is a statement about a *row*, not about a
schema. Mid-deploy, an old engine's tombstone must still block a new engine's fill — otherwise the
two invalidate past each other and a write made during the deploy is lost by whichever engine did
not see it.

## Consequences

- **A schema change needs no flush and no downtime.** Old entries mismatch, read as a miss, and
  refill. Two engines running different shapes against one cache miss each other rather than
  misread each other.
- **Entries written before this change have no `h` field**, so they read as a miss and refill. The
  v0.1.0 → v0.2.0 upgrade needs no cache migration.
- **An online DDL under a running engine** is not detected by the fingerprint alone, because the
  engine's descriptor has not changed. Periodic re-verification against `INFORMATION_SCHEMA` is what
  closes that, and it sheds to the origin rather than serving a wrong shape.
- **Validation must be symmetric.** A fuzzer found `DecodeRow` reading a NULL into a NOT NULL column
  that `EncodeRow` refused to write — a corrupt or hostile entry could produce a row whose NOT NULL
  primary key was NULL. The cache is shared state, so a decoder may not assume its input came from
  this code. The failing input is committed as a seed.
