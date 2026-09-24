# Sextant — is your cache as consistent as you think it is?

Sextant answers that with a number, per consistency level, measured against your own deployment.

It works on **any** cache. Point it at a Redis and the database behind it and it samples, compares,
and publishes what it found. Nothing in it requires Cachet — Cachet is one of the three things it
knows how to read.

```bash
brew tap abhishek-mallick/cachet https://github.com/Abhishek-Mallick/cachet
brew install sextant

sextant -config sextant.yaml
```

---

## Why a number and not a dashboard

Most caches are correct almost always. That is the problem: "almost always" is indistinguishable
from "always" by inspection, and the difference only shows up as a support ticket somebody closes as
unreproducible.

A verifier is worth running only if it can be believed, and there are exactly two ways to lose that:

- **Report violations everywhere.** The number becomes noise, and it gets turned off within a week.
- **Report none.** Worse — it arrives with a reassuring figure attached, and the first person to
  trust it is the last one who will.

Everything below is about not doing either.

---

## What it measures, and what it admits it cannot

Sextant compares a cached entry against the system of record. **How** it compares them decides what
it can say, so the comparison is a label on every metric rather than a footnote:

| Tier | Comparison | Can evaluate |
|---|---|---|
| `value` | The cached value against a projection of the row | `EVENTUAL` only |
| `version` | A version field in the cached value against a database column | `EVENTUAL`, `SESSION`, `BOUNDED(t)` |
| `cachet` | A Cachet entry's fill version against the row version | All of the above |

`SESSION` and `BOUNDED(t)` are statements about **which** database state an entry reflects. Two
values differing tells you an entry is not current; it does not tell you what it is. So at the
`value` tier those levels are **not exported at all** — not as zero violations, which would read as a
clean bill of health, but as not measured.

**`version` is the tier to aim for.** It costs your application one field in whatever it already
caches, and it buys every guarantee except the ones that need Cachet's own clock.

## What counts as a violation

> A violation is a cache entry that is behind the database **and has been for longer than the
> propagation bound**.

The bound is summed from what your system promises — invalidation budget, replication lag, clock
skew — not tuned by hand. An entry behind for less than that is an invalidation in flight, which no
cache design promises will not happen. An entry behind for longer is something that was dropped.

That distinction is the entire difference between a verifier and a monitor.

## Where the keys come from

What you sample decides what you can find.

| Source | What it enumerates | |
|---|---|---|
| `binlog` | Rows that **changed**, from replication | **Primary.** The population a stale entry lives in, read from the database — so the cache cannot steer what it is checked on |
| `file` | A fixed list | Exact and reproducible. What you want during an incident, and what conformance runs use |
| `scan` | The cache's **own** keyspace | **Degraded**, and the metric says so |

`scan` is degraded for a specific reason, not a stylistic one: it asks the cache what to check the
cache on. An invalidation that was dropped also left no entry to enumerate, so the failure hides
itself by looking like an absent key. It exists because a deployment with no replication access has
nothing else, and a degraded measurement that says so beats no measurement.

## Safety, since you are pointing this at production

- **Read-only twice over.** The cache wrapper exposes one method, `Peek`. The documented deployment
  is a Redis ACL user with `+@read`, so the connection cannot write even if the code were wrong —
  one half of the promise is in the code, the other is checkable without reading Go.
- **No identifier ever comes from a key.** Every statement is built once, at startup, from
  identifiers in your config; every value is a bound argument. You can print the statement Sextant
  will issue before you let it connect.
- **A deliberate load.** It samples at the interval and batch size you set, with a small connection
  pool. It does not serve traffic and does not behave like something that does.

## Does it cry wolf?

Measured, not asserted. [`FALSE-POSITIVES.md`](../FALSE-POSITIVES.md) is written by
`make test-verifier`: four **correct** writers hammer the same keys the verifier is sampling, so
every violation reported under that load is a false positive by construction.

The same suite runs three deliberately broken caches — TTL-only, delete-before-commit, and a racy
write-through — and requires each one to be caught. A verifier that cannot fail is not evidence.

---

## Configuration

See [`examples/sextant/sextant.yaml`](../examples/sextant/sextant.yaml), which is commented line by
line and is checked by the test suite for being loadable.

The shape of it:

```yaml
tier: version

origin:
  dsns: ["verifier:password@tcp(mysql-0:3306)/app"]
  table: orders
  key_column: id
  version_column: row_version
  shard: single            # or hash, across the DSNs above

cache:
  addrs: ["redis-0:6379"]
  username: verifier       # grant +@read
  key_template: "order:{key}"
  codec: json              # raw · json · hash
  version_field: row_version

keys:
  source: binlog           # binlog · file · scan

bound:
  write_path_invalidation_budget: 50ms
  cdc_lag_bound: 5s
  max_clock_skew: 250ms
```

Everything checkable is checked at load, and every refusal names the line to change. A `version`
tier with no version column is refused; so is one whose codec cannot extract a version, because the
run would otherwise export `SESSION` and `BOUNDED` series that nothing measured.

## Metrics

```
cachet_sextant_consistency_nines{level="SESSION",tier="version",keys="binlog"}
cachet_sextant_observations{level="SESSION",tier="version",keys="binlog"}
cachet_sextant_violations{level="SESSION",tier="version",keys="binlog"}
cachet_sextant_key_source_degraded{tier="version",keys="scan"}
cachet_sextant_shadow_mode{tier="version",keys="binlog"}
```

`observations` is the one to alert on first. Zero observations is **not** perfect consistency, it is
no evidence — and a verifier reporting a perfect score before it has looked at anything is the most
misleading thing this tool could do.

## Verifying Cachet itself

```bash
sextant -cachet-config cachet.yaml
```

Reads Cachet's own entries through its cache client and routes the origin by its hash ring. Exact,
and no key template to get wrong. Add `-shadow` to measure what consistency *would have been* for a
deployment no application reads through.
