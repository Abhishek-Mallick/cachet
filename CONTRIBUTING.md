# Contributing to Cachet

Cachet's claim is that it is *correct*, and that the correctness is *measured*. Everything below
exists to keep that claim true. These are CI gates, not aspirations — code that violates them does
not merge.

The full reasoning lives in `docs/cachet-build-plan.md` §7. This file is that section in the
imperative, so review can be about design rather than style.

---

## Getting started

```bash
make env-up          # 3 MySQL shards + Redis + Toxiproxy, healthy in <90s
make seed            # deterministic fixture data
make test-unit       # fast, no containers
make lint            # what CI will run
make env-down
```

`make help` lists everything.

---

## The rules

### Concurrency and lifecycle

1. **`context.Context` is the first parameter** of every function that does I/O, waits, or spawns.
   Never store one in a struct.
2. **No goroutine without an owner.** Every goroutine runs under an `errgroup` or has an explicit
   stop channel, and is awaited during shutdown. Every package's `TestMain` calls `goleak`.
3. **No unbounded queues or channels.** Every buffer has a size and a documented overflow behaviour.
4. **Graceful shutdown is a feature.** Stop accepting, drain in-flight work within a deadline, then
   hard-stop.

### Errors

5. Wrap with `%w` and add context: `fmt.Errorf("fill %s: %w", key, err)`.
6. Use sentinel errors for control flow (`ErrLeaseHeld`, `ErrVersionConflict`) and `errors.Is` /
   `errors.As` at the boundaries. Never compare error strings.
7. **No `panic` in library code.** Panic only on programmer error during initialisation, never on
   request data.

### Types and structure

8. **No `any` / `interface{}` in domain types.** No reflection on the hot path.
9. **No global mutable state.** Dependencies are injected through constructors that return concrete
   structs.
10. **Interfaces are declared by the consumer** and have one or two methods. If an interface has six
    methods, it is a struct wearing a disguise.
11. `internal/` by default. Something moves to `pkg/` only when a user must import it — and moving it
    means committing to semver on it.
12. **`pkg/cachet` (the SDK) may not import `internal/`.** Shared types live in `pkg/consistency` or
    are generated from the proto.

### Observability and config

13. **Structured logging via `log/slog` only.** No `fmt.Println`, no `log.Printf`.
14. Every request path emits RED metrics (rate, errors, duration) and an OpenTelemetry span.
15. Config is one validated struct, loaded once at boot. **Fail fast** on anything invalid, with a
    message that says which field and why.

### Documentation

16. Every exported symbol has a doc comment starting with its name.
17. **A public behaviour change requires a doc change in the same commit.**
18. **No `TODO` / `FIXME` / commented-out code on `main`.** Incomplete work lives behind a config
    flag or on a branch.
19. Decisions that would otherwise be re-argued get an ADR in `docs/adr/NNNN-title.md`: context,
    decision, consequences. One page.

---

## Tests

**Test-first. Watch it fail. Then implement.** A test that has never failed is proving nothing — this
matters more here than in most projects, because the tests *are* the product claim.

| Layer | Lives in | Build tag | Runs against |
|---|---|---|---|
| Unit | `*_test.go` beside the code | none | nothing external |
| Integration | `*_integration_test.go` beside the code | `integration` | testcontainers |
| Conformance | `test/conformance/` | `e2e` | the full stack |
| E2E | `test/e2e/` | `e2e` | the full stack |
| Chaos | `test/chaos/` | `chaos` | the full stack + Toxiproxy |

Rules that keep the suites usable:

- **Deterministic seeds.** Fixed RNG, fixed key space. A consistency violation must be reproducible
  from the test name alone.
- **No `sleep`.** Wait on conditions via `test/tools/waitfor`.
- **Every failure dumps state** — the MySQL row, the Redis entry, and the Sextant trace for the
  offending key. A consistency failure you cannot explain is a failure you will eventually delete.
- **The chaos suite asserts detection, not just survival.** Surviving a fault is table stakes;
  catching and explaining it is the product.

---

## The CI gate

A pull request merges only when all of this is green. No `--no-verify`.

| Stage | Command |
|---|---|
| Format | `golangci-lint fmt --diff` |
| Vet | `go vet ./...` |
| Lint | `golangci-lint run` |
| Tidy | `make tidy` |
| Unit + race | `go test -race -count=2 ./...` |
| Integration | `make test-integration` |
| Consistency | `make test-consistency` |
| Env budget | `make env-up` in under 90 s |
| Build matrix | linux/amd64, linux/arm64, darwin/arm64 |
| Vulnerabilities | `make vuln` |

`-count=2` is deliberate: it catches tests that pass only because of state left by their first run.

---

## Commits and versioning

- **Conventional commits** — `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`. `CHANGELOG.md`
  is generated from them.
- **Semver on every artifact.** The wire protocol versions independently as `cachet.v1`.
- **A change to the consistency model is a major version bump, always.** The guarantee is the
  contract.
- Update the `docs/cachet-build-plan.md` §12 TODO in the same commit as the work it tracks.
