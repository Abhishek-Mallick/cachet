# ADR 0007 — Two wire protocols at once

**Status:** Accepted · 2026-09-23

## Context

`cachet.v1` encodes one table's schema into the protocol. `Record` has `tenant_id`, `status` and
`payload` as fields; `UpdateWhereRequest` has `tenant_id`, `match_status` and `set_status`. That was
honest while Cachet served one fixture table, and it is the single largest obstacle to serving
anybody else's: a team with an `orders` table cannot describe a row to this protocol at all.

[ADR 0005](./0005-declared-table-descriptors.md) made a table's shape a declaration and
[ADR 0006](./0006-row-encoding-and-entry-fingerprint.md) made the cache entry carry an opaque row
plus a fingerprint. The wire is the last place the fixture's columns are still spelled out.

Replacing it is a breaking change to the only contract applications hold. v0.1.0 is published: a
binary built against it exists, and an upgrade that turns it into a connection error is an upgrade
nobody can stage.

## Decision

**Serve both protocols from one engine, and negotiate in the SDK.**

`cachet.v2` carries `Row` — a list of `Value`s, each a `is_null` flag and canonical bytes — and
publishes `TableDescriptor`s at the handshake. The engine registers both services on every
listener, from the same `*Engine`: `internal/engine/v2.go` is an adapter, not a second engine.

Three consequences follow, and they are the reason for the shape:

**One state, two spellings.** Routing, leases, admission, invalidation and every consistency rule
belong to the engine. Two of any of them would be two things that could come to mean different
things by SESSION. A v1 write is visible to a v2 reader and the reverse, which is what makes a
half-upgraded fleet — and a rollback from one — safe rather than lossy.

**The v1 shim narrows loudly.** v1 can only describe the fixture's shape, so a v2 row that is not
that shape has no v1 representation. `internal/engine/v2_convert.go` refuses it, naming what did
not fit, rather than inventing columns. A conditional write that matches on a column v1's predicate
cannot express is refused for the same reason: matching on the nearest available column instead
would silently change which rows the write touched.

**The SDK prefers v2 and falls back.** `cachet.Dial` handshakes on v2 first and drops to v1 only on
`codes.Unimplemented` — never on any other error, which would turn an authorization or connection
problem into a silent downgrade to the protocol with no descriptors. Against an older server the
typed API keeps working and the generic one returns `ErrNoDescriptors`, which names the cause.
Client and server therefore upgrade on their own schedules.

Descriptors come from the handshake rather than from client configuration. A client configured with
its own copy of the schema is a second copy to keep in step with the first, and the failure mode of
two copies drifting is reading one column's bytes under another column's name — silent, and wrong
in a way no type checks.

## Consequences

- The dual-serve window lasts until 1.0. `cachet.v1` is then removed; the SDK's typed `Record` API
  goes with it, since it describes a table only Cachet's own fixtures have.
- `internal/engine/row.go` — the typed-record bridge — survives until storage produces rows against
  a configured descriptor. Its doc comment already says it exists to be deleted.
- v2's `UpdateWhereRequest` names a table. v1's could not, so the name is checked in the shim: a
  field accepted and ignored is a write the caller believes landed on one table and that landed on
  another.
- Two protocols mean two paths to keep tested. `test/e2e/protocol_v2_test.go` asserts the
  cross-visibility in both directions for every verb, and `pkg/cachet/negotiate_test.go` holds an
  old server still — no live engine this build starts serves v1 alone.
