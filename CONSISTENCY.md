# Cachet — The Consistency Model

> **Status:** v1, 2026-09-05. This document is **normative**. It is the contract.
>
> Every guarantee below has a named test in `test/conformance/`. If a guarantee is not testable as
> written, the guarantee is wrong, not the test. A change to this document is a **major version
> bump** — see `CONTRIBUTING.md`.

---

## 0. How to read this

Each level states three things, deliberately:

- **Guarantees** — what you may rely on.
- **Non-guarantees** — what you may *not* rely on, stated as plainly as the guarantees. This half is
  the reason to trust the other half.
- **Mechanism** — how the engine enforces it, precisely enough to implement and to test.

The single most important sentence in this document:

> **Cachet never returns a value that is older than something the same session has already
> observed or written.** Everything else is a tunable relaxation of *how much staleness other
> sessions' writes may exhibit*.

---

## 1. Vocabulary

| Term | Definition |
|---|---|
| **Shard** | One MySQL instance. Keys map to shards by consistent hash. Each shard has its **own** HLC |
| **HLC version** | `uint64` = `(physical_ms << 16) | logical`. Monotonically increasing **per shard**. Never compare versions across shards |
| **Row version** `rv` | The `version` column of the row. The version of the write that last modified it |
| **Fill version** `fv` | The shard HLC value read at the moment the DB query that produced a cache entry was issued. "This entry reflects shard state as of `fv`" |
| **Tombstone version** `tv` | The version at which an entry was invalidated. An entry with `tv > rv` is not servable |
| **Session watermark** `W` | A sparse map `shard_id → version`: the highest version this session has written or observed, per shard |
| **Commit** | The MySQL `COMMIT` of a Cachet write returns; the write is durable and visible to a direct DB read |
| **Ack** | Cachet's `Put`/`Delete` response reaches the client. Always **after** commit and after invalidation |

### The cache entry

```
entry := { rv uint64, fv uint64, tv uint64, payload []byte, negative bool }
```

Four fields, and the reason for each is load-bearing:

- `rv` orders **fills against each other** — a slow read's fill cannot clobber a newer write's value.
- `fv` answers **"is this entry fresh enough for this session / this staleness bound?"** — separately
  from how old the row itself is. A row untouched for a year has an ancient `rv` and may still have
  been read from the database one millisecond ago.
- `tv` makes invalidation a **compare-and-set** rather than a delete, closing the delete-vs-fill race.
- `negative` marks "this row does not exist", which is a cacheable fact and must be invalidated on
  insert.

**Separating `rv` from `fv` is the design decision that makes `SESSION` cheap.** A watermark check
against `rv` would reject every entry whose row happens to be old, collapsing the hit rate to near
zero on any shard that takes writes. A watermark check against `fv` rejects only entries that were
filled from a database state older than what the session has already seen — which is exactly the
condition that matters, and no more.

---

## 2. The core invariant

> **Every mutation of a cache entry is a compare-and-set. A lower version never overwrites a higher
> one.**

Stated as three rules, which are what `internal/cache/lua/*.lua` implement and what
`test/conformance/invariant_test.go` verifies:

| Mutation | Applies iff |
|---|---|
| `fill(k, rv, fv, payload)` | entry absent · **or** `rv > e.rv` · **or** (`rv == e.rv` **and** `fv > e.fv`) — **and** in all cases `rv >= e.tv` |
| `tombstone(k, v)` | entry absent · **or** `v > e.rv` — sets `e.tv = max(e.tv, v)` |
| `evict(k)` | always (TTL or memory pressure); losing an entry is never a correctness event |

Consequences, each of which is a test:

- **Duplicate CDC delivery is idempotent.** Replaying the binlog re-applies tombstones that no longer
  apply, and they are rejected.
- **Out-of-order invalidation is safe.** Write-path and CDC invalidations may arrive in any order.
- **A restarted tailer cannot undo newer state.** Catch-up replays old versions, which lose the CAS.
- **`rv >= e.tv` prevents resurrection.** A fill carrying a value older than a known invalidation is
  rejected rather than served.

---

## 3. The four levels

### 3.1 `STRONG`

**Use for:** money, authorisation, anything you would have written `SELECT ... FOR UPDATE` for.

**Guarantees**
- The read **bypasses the cache entirely**. It is a direct query against the shard.
- You get exactly the isolation MySQL gives you at the configured transaction isolation level.
  Cachet neither adds to nor subtracts from it.
- The cache is not consulted, not filled, and not mutated by a `STRONG` read. It is, however,
  **recorded in the admission sketch** — a key read strongly is still a hot key.

**Non-guarantees**
- **Not distributed linearizability.** A `STRONG` read spanning two shards is two independent reads
  at two independent points in time. Cachet has no cross-shard transactions and will not pretend to.
- No performance guarantee. `STRONG` is the level that costs what the database costs.

**Mechanism**
```
Get(k, STRONG):
    return db.Read(shardOf(k), k)         # cache untouched
```

**Tests:** `conformance/strong_test.go` — `TestStrongNeverReadsCache`,
`TestStrongObservesCommitImmediately`, `TestStrongDoesNotFillCache`.

---

### 3.2 `SESSION` — the default

**Use for:** almost everything.

**Guarantees**

For a session `S` holding watermark `W`:

1. **Read-own-writes.** If `S` wrote key `k` on shard `s` and received an ack, every subsequent
   `SESSION` read of `k` by `S` returns that write or a later one.
2. **Read-own-inserts.** The same holds when the write was an insert over a previously cached
   negative entry.
3. **Read-own-deletes.** The same holds for deletes; the read returns "not found", not the old value.
4. **Monotonic reads.** Within `S`, successive reads of `k` never move backwards in version.
5. **Causal propagation across services.** If `S`'s watermark is carried across an RPC boundary (the
   SDK does this via OpenTelemetry context propagation), the downstream service inherits guarantees
   1–4 with respect to the upstream's writes.

**Non-guarantees**

- **Not read-*others*-writes.** Another session's write, committed 5 ms ago, may not be visible to
  you. Its visibility is bounded by the write-path invalidation, and in the degraded case (§5) by
  the CDC lag bound.
- **Not writes-follow-reads.** v1 does not order your writes against values you have read.
- **Not a snapshot across keys.** Two keys read in one `BatchGet` may reflect different points in
  time. There is no cross-key atomicity at any level.
- **The guarantee is scoped to the lifetime of the session token.** If the token is lost, so is the
  guarantee — see §4. This is the sharpest edge in the model and it is stated, not hidden.

**Mechanism**
```
Get(k, SESSION, W):
    s := shardOf(k)
    e := cache.Read(k)
    if e exists and not tombstoned and e.fv >= W[s]:
        return e.payload                        # hit
    payload, rv, fv := db.Read(s, k)            # miss: read through
    cache.Fill(k, rv, fv, payload)              # CAS, §2
    W[s] = max(W[s], fv)                        # observing advances the watermark
    return payload

Put(k, v, W):
    BEGIN
      ver := shard.hlc.Next()
      UPDATE ... SET payload=v, version=ver WHERE pk=k
    COMMIT
    cache.Tombstone(k, ver)                     # after commit, BEFORE ack
    W[shardOf(k)] = max(W[s], ver)
    return ack, W
```

Two orderings carry the whole guarantee, and both are testable:

- **Invalidate after commit, before ack.** By the time the caller holds the ack, the stale entry is
  already tombstoned. This is what makes read-own-writes hold even for a *different* process that
  receives the watermark.
- **`W[s] = max(W[s], fv)` on read.** Observing advances the watermark, which is what gives monotonic
  reads without any extra state.

**Tests:** `conformance/session_test.go` — `TestReadOwnWrite`, `TestReadOwnInsertOverNegative`,
`TestReadOwnDelete`, `TestMonotonicReadsUnderConcurrentWriters`,
`TestWatermarkPropagatesAcrossServiceHop`, `TestSessionSurvivesEngineFailover`.

---

### 3.3 `BOUNDED(t)`

**Use for:** feeds, counts, listings, anything where "a few seconds behind" is a product decision
rather than a bug.

**Guarantees**

> **`t` is measured from commit time, not from read time.**

Precisely: a `BOUNDED(t)` read at wall-clock instant `T` returns a value that reflects **every write
committed at or before `T − t`**, on that key's shard. Writes committed inside the window `(T − t, T]`
may or may not be reflected.

- All `SESSION` guarantees also hold. `BOUNDED(t)` is `SESSION` *plus* an additional freshness floor;
  it is never weaker than `SESSION` for your own writes.
- The bound is enforced on the **fill version** `fv`, so it is a bound on how stale the *database
  snapshot behind the entry* is — not on how long the entry has sat in Redis.

**Non-guarantees**

- The bound holds only while measured clock skew between the reading engine and the writing shard is
  within `max_clock_skew` (default 250 ms, configurable). Sextant measures actual skew continuously
  and raises a violation if it exceeds the configured bound — the guarantee does not silently lapse.
- `t` is a *bound*, not a target. Entries are usually far fresher; do not build on the bound being
  tight.
- `BOUNDED(0)` is not `STRONG`. Use `STRONG`.

**Mechanism**
```
Get(k, BOUNDED(t), W):
    if e.fv.physical_ms < now_ms() - t - max_clock_skew:  treat as miss
    else: apply the SESSION check
```

The `max_clock_skew` term is subtracted deliberately: the engine is conservative about its own clock,
so an under-estimate of the entry's age can never widen the promised window.

**Tests:** `conformance/bounded_test.go` — `TestBoundedRejectsEntryOlderThanWindow`,
`TestBoundedMeasuresFromCommitNotRead`, `TestBoundedIsNeverWeakerThanSession`,
`TestBoundedShrinksWindowByClockSkew`.

---

### 3.4 `EVENTUAL`

**Use for:** recommendations, related-items, anything where a stale answer is merely a worse answer.

**Guarantees**
- Any non-tombstoned entry may be served, subject to TTL.
- Invalidation is still exact and still applied — `EVENTUAL` does not disable correctness machinery,
  it only declines to *wait* for it. Entries still converge.
- Maximum staleness is bounded in practice by the entry TTL (hours, as a safety net, not a strategy).

**Non-guarantees**
- **No read-own-writes.** You may read your own stale value immediately after your own ack.
- No monotonic reads. Successive reads may go backwards.

`EVENTUAL` exists to make the *cost* of the other levels visible in the benchmark table. It is the
control group.

**Tests:** `conformance/eventual_test.go` — `TestEventualMayServeStale`,
`TestEventualStillConverges`, `TestEventualRespectsTombstone`.

---

## 4. Session tokens — lifetime, reconnect, and loss

The question the build plan flagged as needing an answer before any code: **what does `SESSION`
guarantee across a client reconnect?**

### The token

```
SessionToken := { watermarks map[ShardID]uint64 }   # sparse: only shards this session touched
```

Opaque to the caller, carried in gRPC metadata as `cachet-session-bin`, propagated by the SDK
alongside OpenTelemetry trace context.

### Lifetime rules

| Event | What happens to the guarantee |
|---|---|
| gRPC connection drops and reconnects | **Guarantee holds.** The token lives in the SDK client object, not in the connection. Reconnect is invisible to it |
| Engine instance dies; SDK fails over to another | **Guarantee holds.** The token is client-held; engines are stateless with respect to sessions. This is why the watermark is not server-side |
| Request crosses a service boundary with propagation enabled | **Guarantee holds** for the downstream service |
| Request crosses a service boundary *without* propagation | **Guarantee is lost at that hop.** The downstream session starts empty and reads as if it had written nothing |
| SDK client object is recreated (process restart) | **Guarantee is lost.** A new session has an empty watermark |
| Token exceeds `max_session_shards` (default 64) | The oldest watermarks are evicted; reads against those shards silently fall back to `BOUNDED(ttl)`. The response sets `degraded=true` and names the level actually served |

**The rule underneath all of it:** the session guarantee is a property of *the token*, not of the
client, the connection, or the engine. Hold the token, keep the guarantee. Drop it, and you have a
new session — which is correct behaviour, not a bug, because a process that has forgotten it wrote
something has no writes to read.

### Token integrity

v1 does not sign the token. Cachet v1 is single-tenant with no auth (spec §9), and the failure modes
of a forged token are self-inflicted: a too-high watermark causes extra misses; a too-low one causes
staleness only for the forger. **When multi-tenancy lands, the token must be signed** — recorded here
so it is not rediscovered later.

---

## 5. Degraded responses

A conditional or range write (`UPDATE ... WHERE status = ?`) does not name its affected keys. The
engine resolves them exactly with `SELECT pk ... FOR UPDATE` inside the transaction. When the
predicate matches more than `max_affected_keys` rows (default 1000), that resolution is abandoned as
too expensive and the write relies on CDC invalidation instead.

When that happens the response carries:

```
degraded        = true
effective_level = BOUNDED(cdc_lag_bound)
reason          = "predicate exceeded max_affected_keys"
```

**What is and is not lost — precisely:**

- **Your own session guarantee survives.** The write still committed at version `ver`, and
  `W[shard] = max(W[shard], ver)` still advances. Every subsequent `SESSION` read on that shard
  rejects entries filled before your write and reads through. This falls out of watermarking on `fv`
  rather than on `rv`, and it is the second payoff of that choice.
- **Other sessions' reads become `BOUNDED(cdc_lag_bound)`** for the affected keys until Flux catches
  up, instead of being invalidated before your ack.

The SDK surfaces `degraded` as a field on the result. **Ignoring it is a caller decision, but it must
be a visible one** — a silent downgrade of a stated guarantee is exactly the failure this project
exists to eliminate.

**Tests:** `conformance/degraded_test.go` — `TestLargePredicateDegradesOthersNotSelf`,
`TestDegradedFlagIsSurfacedToCaller`, `TestDegradedConvergesWithinCDCLagBound`.

---

## 6. What Cachet never guarantees, at any level

Stated once, plainly, so no level's fine print has to repeat it:

| Not guaranteed | Why |
|---|---|
| Cross-key atomicity or a consistent snapshot across keys | Cachet caches rows, not transactions. `BatchGet` is N independent reads |
| Cross-shard transactions or cross-shard version ordering | Each shard has its own HLC. Versions from different shards are incomparable, and comparing them is a bug |
| Serializability | Use `STRONG`, and the database's own isolation |
| Writes-follow-reads | Not in v1 |
| Bounded staleness under unbounded clock skew | The `BOUNDED` bound is conditional on `max_clock_skew`; Sextant measures and alarms rather than letting it lapse silently |
| Any guarantee for writes made **directly to MySQL**, bypassing Cachet | Those are invalidated by CDC only, so they are `BOUNDED(cdc_lag_bound)` for everyone, including the writer |

---

## 7. How a violation is defined and detected

This is the definition Sextant implements. It has to be tight, because the product claim is a number
derived from it.

> A **violation** is a cache entry `e` for key `k` on shard `s` such that `e.fv < db_version(k)` and
> the entry has existed in that state for longer than the **propagation bound** `P`.

Where `P = write_path_invalidation_budget + cdc_lag_bound + max_clock_skew`, measured, not assumed.

The propagation bound is what separates a real violation from a benign in-flight race: an entry that
is momentarily behind while an invalidation is in flight is *not* a violation, because the model
never promised instantaneous propagation to other sessions. An entry still behind after `P` is a
violation, because something was dropped, reordered, or lost.

**Per-level accounting.** A single stale entry is a violation of some levels and not others:

| Level | Counts as a violation when |
|---|---|
| `STRONG` | Never — `STRONG` does not read the cache. A `STRONG` read returning a stale value is a *database* bug |
| `SESSION` | A read served an entry with `fv < W[s]` — i.e. the watermark check was bypassed or wrong |
| `BOUNDED(t)` | A read served an entry with `fv` older than `t + max_clock_skew` |
| `EVENTUAL` | An entry remained behind `db_version` for longer than the entry TTL — i.e. it never converged |

Sextant exports one gauge per level. **The nines on the dashboard are per level**, because a single
blended number would hide exactly the trade the levels exist to expose.

**Tests:** `conformance/violation_test.go` — `TestInFlightRaceIsNotAViolation`,
`TestStaleBeyondPropagationBoundIsAViolation`, `TestViolationIsAttributedToTheRightLevels`.

---

## 8. The conformance matrix

`test/conformance/matrix_test.go` generates this cross product. Every cell is executed; every cell
either asserts a guarantee or asserts its documented absence.

| Operation | `STRONG` | `SESSION` | `BOUNDED(t)` | `EVENTUAL` |
|---|---|---|---|---|
| Read own point write | ✅ sees it | ✅ sees it | ✅ sees it | ⬜ may not |
| Read own insert (over negative entry) | ✅ | ✅ | ✅ | ⬜ |
| Read own delete | ✅ | ✅ | ✅ | ⬜ |
| Read own conditional write (degraded) | ✅ | ✅ | ✅ | ⬜ |
| Read another session's write, immediately | ✅ | ⬜ may not | ⬜ may not | ⬜ |
| Read another session's write, after `P` | ✅ | ✅ | ✅ | ✅ |
| Monotonic reads under concurrent writers | ✅ | ✅ | ✅ | ⬜ |
| Staleness bounded by `t` | n/a | ⬜ unbounded by others' writes | ✅ | ⬜ |
| Survives engine failover | ✅ | ✅ | ✅ | ✅ |
| Survives Redis flush | ✅ | ✅ | ✅ | ✅ |
| Survives CDC restart mid-stream | ✅ | ✅ | ✅ | ✅ |
| Cross-key snapshot | ❌ | ❌ | ❌ | ❌ |

✅ guaranteed and tested · ⬜ explicitly not guaranteed, and tested to confirm the model is honest
about it · ❌ never, at any level

**The `⬜` column entries are as important as the `✅` ones.** A model that only tests what it promises
has not been tested; it has been advertised.

---

## 9. Configuration that changes the guarantee

These are not tuning knobs. Changing one changes what Cachet promises, so each is logged at boot and
exported as a Prometheus gauge.

| Setting | Default | Affects |
|---|---|---|
| `max_clock_skew` | 250 ms | The `BOUNDED(t)` window, and the propagation bound `P` |
| `cdc_lag_bound` | 5 s | The staleness bound for degraded writes and for out-of-band DB writes |
| `write_path_invalidation_budget` | 50 ms | Part of `P`; exceeded means the write path is the suspect |
| `max_affected_keys` | 1000 | When a conditional write degrades (§5) |
| `max_session_shards` | 64 | When a session token starts evicting watermarks (§4) |
| `entry_ttl` | 4 h | The `EVENTUAL` convergence bound, and the last-resort safety net |

---

## 10. Open questions

Tracked here rather than in code comments, because each is a model question, not an implementation
one.

- **Should `BatchGet` offer an optional cross-key snapshot at one `fv`?** It is implementable (read
  all keys at one shard HLC read) but only within a single shard, and offering it per-shard invites
  the assumption that it holds across shards. *Leaning no. Revisit if a real caller asks.*
- **Should the watermark be per-key for small sessions and per-shard beyond a threshold?** Per-key
  would eliminate the residual false-miss rate on write-heavy shards. *Measure the false-miss rate in
  Phase 3 first; do not pre-optimise a number nobody has looked at.*
- **Writes-follow-reads:** worth adding in v2 if session propagation proves reliable in practice.
