# ADR 0005 — Tables are declared, not discovered

**Status:** Accepted · 2026-09-23

## Context

Until v0.1.0 Cachet cached exactly one table: `entities`, with a fixed five-column schema, hardcoded
into every SQL statement, into the cache entry, and into four Lua scripts. Everything above it —
the consistency model, the verifier, the fault suite, the proxy — was real and operated on a
fixture. **No team could use Cachet on their own schema**, which made it the blocking item for the
product regardless of what else shipped.

Making it generic means deciding where the description of a table comes from, because routing, cache
identity, invalidation and the proxy's classifier are all derived from it.

## Decision

**A declared descriptor, verified against the live schema at boot.**

Configuration names the table, its primary key, its cached columns and their types, and the version
column. Every SQL statement is built once at startup from identifiers that have been validated.
`INFORMATION_SCHEMA` is then read to *assert* the declaration matches, and the engine **refuses to
start** on any mismatch.

The type set is closed. A column whose type Cachet cannot round-trip byte-identically is not
cacheable, which the proxy already knows how to express by refusing to serve a statement naming it.

## Rationale

| Alternative | Why not |
|---|---|
| **Discover the schema, cache everything** | The system would be choosing what is cacheable. That contradicts the stance the whole codebase takes about SQL it does not understand: the cost of refusing is a cache miss, the cost of misunderstanding is a correctness bug, and those are not comparable |
| **Generate code per table** | Fastest hot path and keeps the record typed, but the engine is a deployed server binary. Recompiling per deployment destroys the `cachet-proxy` no-SDK story, which is the only path for applications that will not take a library |
| **Declare, and trust the declaration** | A declaration that has drifted from the database surfaces as a column-not-found on whichever user request touches it first, or — for nullability — as a decode failure on whichever row happens to hold a NULL. Both are 3am problems that boot-time verification turns into a startup error with the column named |

## Consequences

**Three refusals are load-bearing**, and each is refused at boot because none can be detected later:

1. **A case- or accent-insensitive string primary key.** Under `utf8mb4_0900_ai_ci` — the collation
   Cachet's own fixture used — MySQL treats `'Ann'` and `'ann'` as the *same row*, while a cache key
   derived in Go treats them as two. A write under one key and an entry under the other is an
   invalidation that silently misses, with no runtime signal, ever.
2. **A non-integer or nullable version column.** Every compare-and-set orders on it, and a NULL
   cannot be compared.
3. **An identifier that is not a plain SQL identifier.** Identifiers reach SQL, and "they only come
   from configuration" is a claim about deployment rather than a control.

**Extra columns in the table are fine and expected** — `updated_at` is one. They are simply not
cached. This is also what makes `SELECT *` unservable from cache unless the declared set provably
covers the whole table, which the proxy checks.

**DECIMAL, DATETIME, TIMESTAMP and JSON are carried as the text MySQL produced**, not parsed and
re-rendered. A round trip through `time.Time` loses the difference between what was stored and what
a driver chose to format, and DECIMAL through `float64` loses money.
