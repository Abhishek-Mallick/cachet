# ADR 0004 — Sidecar is the default topology; UDS is a Phase 0 requirement

**Status:** Accepted · 2026-09-05

## Context

Cachet is a data access layer, not a transparent proxy. Applications call it instead of calling MySQL
directly. That raises a problem the architecture diagram hides:

> If an organisation's apps talk to MySQL directly today, deploying Cachet as a standalone service
> tier **adds a network hop to the exact path we are trying to make faster.**

Rough in-datacenter figures, to be replaced by Phase 0 measurements:

| Path | Hops | Approx. cache-hit latency |
|---|---|---|
| App → Redis (hand-rolled cache today) | 1 | ~0.3 ms |
| App → Cachet **service** → Redis | 2 | ~0.6–0.9 ms |
| App → Cachet **sidecar** (UDS) → Redis | 1 + IPC | ~0.35 ms |
| App with **embedded** Cachet → Redis | 1 | ~0.3 ms |

A standalone service can roughly double cache-hit p99 for the majority of the target market — and it
is invisible until someone deploys it. The system this architecture descends from did not have this
problem, because its query-engine tier already existed and apps were already paying that hop. Cachet
usually enters an environment where they are not.

## Decision

**One binary, three placements. The sidecar over a Unix domain socket is the default**, and the
engine must support `--listen unix:///...` **from Phase 0**, not as a later addition.

| Situation | Topology |
|---|---|
| Apps talk to MySQL directly, polyglot | **Sidecar** (default) |
| Existing BFF / data-access / GraphQL tier | Service tier — replaces a hop rather than adding one |
| All-Go, few services, chasing the last 200 µs | Embedded library |

## Rationale

- UDS IPC costs tens of microseconds, not a network round trip, so the sidecar keeps the latency
  claim intact for the common case.
- It is language-agnostic — the app speaks gRPC over a socket, so polyglot shops are served without
  an SDK in every language on day one.
- Cachet upgrades independently of the application, and the blast radius of a bug is one pod.
- Topology becomes a deployment choice rather than a product variant, which keeps the codebase single.

## Consequences

- **Transport is a Phase 0 concern.** Retrofitting UDS later would mean redoing the transport layer
  and re-running every benchmark. Tracked as amendment D1 (build plan §11).
- **The benchmark harness must measure all three topologies**, not one, so the table above becomes
  data rather than estimates. Amendment D2.
- **Per-pod resource cost** (~50–100 MB RSS budgeted) and per-pod fragmentation of the admission
  sketch. The latter is resolved by mergeable sketch gossip — amendment D4 — without which adaptive
  admission would only work in a deployment shape nobody runs.
- The embedded library splits the API surface and doubles the testing matrix, so it is documented as a
  supported pattern in v1 and shipped when a user asks.
