# Cachet — test environment

Three layers, deliberately kept separate. Mixing them is the usual reason a suite gets slow, then
gets skipped, then stops protecting anything.

| Layer | Lives in | Build tag | Runs against | Typical time |
|---|---|---|---|---|
| **Unit** | `*_test.go` beside the code | none | nothing external | < 2 s |
| **Integration** | `*_integration_test.go` beside the code | `integration` | testcontainers: one MySQL + one Valkey | ~15 s |
| **Conformance / E2E** | `test/conformance/`, `test/e2e/` | `e2e` | the full stack in `test/env/` | minutes |
| **Chaos** | `test/chaos/` | `chaos` | the full stack + Toxiproxy | minutes |

`bench/` is a fourth thing and is not a test: it measures, it does not assert. See
`docs/cachet-benchmarking.md`.

---

## Quick start

```bash
make env-up            # 3 MyRocks shards + Valkey, healthy in well under the 90s budget
make seed              # deterministic fixture data (SEED_PROFILE=small|medium|large)
make test-unit
make test-integration
make env-down
```

`make help` lists every target.

---

## What is in `env/`

| File | Purpose |
|---|---|
| `compose.yml` | The base stack: `shard0/1/2` (Percona Server + MyRocks) and `cache` (Valkey) |
| `compose.observability.yml` | Prometheus + Grafana, dashboards provisioned from `deploy/grafana/` |
| `compose.chaos.yml` | Toxiproxy in front of every dependency |
| `compose.innodb.yml` | Swaps MyRocks for InnoDB — the storage-engine study in build plan §13 |
| `mysql/my.myrocks.cnf` | Shard configuration. Two settings are marked REQUIRED and are correctness-relevant, not performance knobs |
| `mysql/initdb/` | Schema applied at bootstrap; the canonical copy is `test/fixtures/schema/` |
| `redis/valkey.conf` | Bounded memory with LRU, no persistence — losing the whole cache must be a correctness non-event |

Ports are offset from the defaults (`3316–3318`, `6379`) so a local MySQL does not collide. Copy
`.env.example` to `.env` to change them.

### Two things in the MySQL config that are not tuning

- **`binlog_format=ROW` with `binlog_row_image=FULL`.** Flux extracts the primary key *and* the
  `version` column from every binlog event. A minimal row image omits unchanged columns, which
  leaves invalidation guessing — and guessing is the thing this project exists to remove.
- **`transaction_isolation=READ-COMMITTED`.** The conditional-write path resolves affected keys
  exactly with `SELECT pk ... FOR UPDATE` (`CONSISTENCY.md` §5); READ-COMMITTED locks matched rows
  without relying on gap-lock semantics MyRocks does not implement.

There is also a non-obvious one worth knowing before editing that file: **`default_storage_engine`
is deliberately not set to `ROCKSDB`**, and every `rocksdb_*` variable carries the `loose_` prefix.
The data directory is bootstrapped by `mysqld --initialize` before any plugin is loaded, so a
ROCKSDB default aborts the bootstrap and leaves an unusable data directory. Tables name their engine
explicitly in the DDL instead.

---

## Fixtures

`make seed` loads deterministic data. **Determinism is a requirement, not a nicety**: a consistency
violation has to be reproducible from a test name alone, which is only true if the data underneath
it is byte-identical on every run.

| Profile | Rows | For |
|---|---|---|
| `small` | 10,000 | e2e and conformance — fast enough that people keep running them |
| `medium` | 1,000,000 | working set stops fitting in the cache |
| `large` | 10,000,000 | the profile published benchmark numbers come from |

The generator is a pure function of `(profile, seed)`: no wall time, no hostnames, no global random
source. Each shard's SHA-256 digest is written to `seed_meta`, so "was this loaded completely, and
is it the same data as last time?" is one query rather than a full-table comparison.

---

## Rules

These are what keep the suites usable. They are not aspirational — each has bitten a project before.

| Rule | Why |
|---|---|
| **`make env-up` under 90 s**, timed in CI | It is the top of the adoption funnel. If it drifts to three minutes, adoption drops and nobody notices |
| **Every suite brings up and tears down its own environment** | A test depending on a stack someone else started is a flaky test |
| **Deterministic seeds** | A violation must be reproducible from the test name |
| **No `sleep`** — wait on conditions via `test/tools/waitfor` | Sleeps make a suite slow and flaky at the same time |
| **Every failure dumps state** — the row, the cache entry, and the Sextant trace for the offending key | A consistency failure you cannot explain is one you will eventually delete |
| **`goleak` in every `TestMain`** | Enforces "no goroutine without an owner" mechanically rather than by review |
| **Chaos asserts detection, not survival** | Surviving a fault is table stakes. Catching *and explaining* it is the product |

---

## Not built yet

Tracked in `docs/cachet-build-plan.md` §12. These directories are created when the code they test
exists, rather than being stubbed now:

| Path | Arrives with |
|---|---|
| `env/compose.sidecar.yml`, `env/compose.service.yml` | T8 — the engine's UDS and TCP listeners |
| `harness/` | T8 — needs something to point at |
| `conformance/`, `e2e/` | T8 onward; the conformance suite lands in Phase 3 with `CONSISTENCY.md` |
| `chaos/` | Phase 5 |
| `tools/waitfor/` | T8 |
