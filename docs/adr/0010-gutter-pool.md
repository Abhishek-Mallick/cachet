# ADR 0010 — A gutter pool for a cache node that stops answering

**Status:** Accepted · 2026-09-26

## Context

Cachet had two answers for a failing cache node, and both protect the same thing.

The per-node circuit breaker sheds load proportionally, so a node failing 30% still serves 70% and
a dead one stops holding requests open. The read path treats a cache error as a miss and falls
through to the database. Both protect **latency**.

Neither protects the **origin**. The moment a node stops answering, its entire share of the keyspace
arrives at the database — with three cache nodes, a third of all reads, simultaneously, on a
database that has no breaker of its own. `PRIOR-ART.md` calls this the most valuable unimplemented
idea in the three Meta papers, and it is: it is the difference between a degraded cache and a
database incident.

Memcache's answer (NSDI '13 §3.4) is a small standby pool — the *gutter* — that catches those keys.
The question for Cachet is not whether to have one. It is what a pool that nothing invalidates does
to a system whose entire product claim is that its staleness is stated.

## Decision

**A standby pool, consulted only when the home node fails to answer, whose entries are bounded by
their TTL and reported as degraded.**

**Only on failure, never on a miss.** A miss means the answer is not cached and the origin is where
it comes from. The home node is the pool invalidations reach, so an entry living only in the gutter
is one nothing has invalidated — serving it on a miss would return a value the home node had
already superseded. The engine therefore distinguishes "the node did not answer" from "the node said
no", which it previously did not.

**The freshness rules are the same rules.** A gutter entry carries a fill version like any other, so
the watermark check and the `BOUNDED(t)` window apply to it unchanged. That is not a convenience —
it is why the pool is safe at `SESSION` at all: read-own-writes is carried by the **watermark**, not
by invalidation, so a session that wrote at version `v` rejects a gutter entry filled before `v`
exactly as it would reject a primary one. `STRONG` never reads a cache. `EVENTUAL` is the level the
gutter actually weakens, because it is the one that asks for no freshness.

**The TTL is the bound, and it is on the response.** Nothing invalidates a gutter entry, so its TTL
is the entire statement about its staleness. A read the gutter served carries `degraded = true`, the
reason, and `effective_staleness_bound = gutter.ttl` — the same shape §5 already uses for a degraded
write. A degraded response without a number is one the caller cannot act on, which is why
`effective_staleness_bound` was added to `cachet.v2`'s `ReadMeta`.

**Tombstoned only when the home node refuses.** Invalidating the gutter on every write would double
invalidation traffic for a pool that is almost never read. Doing it when the home node *rejected*
the tombstone targets exactly the condition under which the gutter can hold a superseded value, at
no cost in the common path. This is a small improvement on memcache's design, which relies on the
TTL alone.

**A gutter address may not be a cache address.** A standby on the pool it stands by for does not
survive the failure it exists for: whatever emptied one emptied the other. Refused at config load,
because that configuration looks like protection while providing none.

**Off by default.** It needs nodes nobody has by default, and the failure mode of a
wrongly-configured gutter is a false sense of safety.

## Consequences

- One more pool to run, and one more thing to size. The pool is small by design: it holds only the
  keys of nodes that are currently down, for the length of its TTL.
- `EVENTUAL` reads during an outage are bounded by `gutter.ttl` rather than unbounded. That is a
  *stronger* statement than before — previously those reads went to the database, which was fresher
  but is precisely the load this exists to avoid — so the trade is stated per response rather than
  buried in a config file.
- `test/e2e/gutter_test.go` asserts the point that matters on the **origin counter**: twenty reads of
  ten keys with the home node cut reach the database ten times rather than twenty. "The cache still
  answered" is not the property.
- The gutter never takes a lease. Leases are per-key state on the home node, and the home node is
  the one that is not answering.
