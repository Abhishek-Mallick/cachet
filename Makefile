# Cachet — single entry point for every workflow.
# See docs/cachet-build-plan.md §9.3 for what each target is for.

SHELL          := /usr/bin/env bash
# .SHELLFLAGS needs GNU Make >= 3.82. macOS still ships 3.81, which ignores it silently — so every
# multi-line recipe below starts with an explicit `set -euo pipefail`. Relying on .SHELLFLAGS alone
# meant a failing command in the middle of a recipe was swallowed and the target reported success,
# which is exactly how a broken `make env-up` reports "healthy in 75s".
.SHELLFLAGS    := -eu -o pipefail -c
.DEFAULT_GOAL  := help

MODULE         := github.com/Abhishek-Mallick/cachet
BIN            := bin
COMPOSE_DIR    := test/env
# --project-directory makes the compose files' relative volume paths resolve correctly no matter
# where make is invoked from; -f paths are resolved against the CWD, so they stay fully qualified.
COMPOSE        := docker compose --project-directory $(COMPOSE_DIR) --project-name cachet
F_BASE         := -f $(COMPOSE_DIR)/compose.yml
F_OBS          := -f $(COMPOSE_DIR)/compose.observability.yml
F_CHAOS        := -f $(COMPOSE_DIR)/compose.chaos.yml
GO             := go
GOTEST         := $(GO) test -race -count=2
SEED_PROFILE   ?= small

# ─── help ────────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ─── build ───────────────────────────────────────────────────────────────────────

.PHONY: build
build: ## Build every binary into ./bin
	@set -euo pipefail; \
	mkdir -p $(BIN); \
	found=0; \
	for d in cmd/*/; do \
	  [ -n "$$(ls $$d*.go 2>/dev/null)" ] || continue; \
	  c=$$(basename $$d); \
	  echo "  building $$c"; \
	  $(GO) build -trimpath -o $(BIN)/$$c ./$$d; \
	  found=$$((found+1)); \
	done; \
	echo "built $$found binaries"

.PHONY: generate
generate: ## Regenerate protobuf stubs (reproducible; plugins pinned in go.mod)
	$(GO) tool buf lint
	$(GO) tool buf generate
	$(GO) mod tidy

.PHONY: clean
clean: ## Remove build output
	rm -rf $(BIN)

# ─── hygiene ─────────────────────────────────────────────────────────────────────

.PHONY: fmt
fmt: ## Format the tree
	golangci-lint fmt ./...

.PHONY: lint
lint: ## Vet + lint, including tagged test files (build plan §7.2)
	$(GO) vet ./...
	golangci-lint run --build-tags=integration,e2e,chaos

.PHONY: tidy
tidy: ## go mod tidy, verified to be a no-op
	@set -euo pipefail; \
	before=$$(mktemp -d); \
	cp go.mod go.sum "$$before/"; \
	$(GO) mod tidy; \
	if ! diff -q go.mod "$$before/go.mod" >/dev/null || ! diff -q go.sum "$$before/go.sum" >/dev/null; then \
	  echo "go.mod/go.sum were not tidy; the changes have been applied — review and commit them:"; \
	  diff -u "$$before/go.mod" go.mod || true; \
	  rm -rf "$$before"; exit 1; \
	fi; \
	rm -rf "$$before"; \
	echo "go.mod and go.sum are tidy"

.PHONY: vuln
vuln: ## Known-vulnerability scan
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# ─── test layers (build plan §9) ─────────────────────────────────────────────────

.PHONY: test-unit
test-unit: ## Fast unit tests, no containers, -race
	$(GOTEST) ./...

.PHONY: test-integration
test-integration: ## testcontainers: one MySQL + one Redis
	$(GOTEST) -tags=integration -timeout=15m ./internal/... ./pkg/...

.PHONY: test-lua
test-lua: ## Lua scripts against a real Redis, under concurrency
	$(GOTEST) -tags=integration -timeout=10m ./internal/cache/...

.PHONY: test-consistency
test-consistency: ## The conformance suite. THE gate that matters.
	$(GOTEST) -tags=e2e -timeout=30m ./test/conformance/...

.PHONY: test-e2e
test-e2e: ## Full-stack scenarios against test/env
	$(GOTEST) -tags=e2e -timeout=30m ./test/e2e/...

.PHONY: test-chaos
test-chaos: ## The 9 injected faults
	$(GOTEST) -tags=chaos -timeout=45m ./test/chaos/...

.PHONY: test-all
test-all: test-unit test-integration test-consistency test-e2e ## Everything, in dependency order

# ─── environment (build plan §9.1) ───────────────────────────────────────────────

.PHONY: env-up
env-up: ## Bring the stack up and wait for health (<90s budget)
	@set -euo pipefail; \
	start=$$(date +%s); \
	$(COMPOSE) $(F_BASE) up -d --wait; \
	echo "env-up healthy in $$(( $$(date +%s) - start ))s"

.PHONY: env-down
env-down: ## Stop the stack and remove volumes
	$(COMPOSE) $(F_BASE) $(F_OBS) $(F_CHAOS) down -v --remove-orphans

.PHONY: env-reset
env-reset: env-down env-up seed ## Wipe, restart, reseed

.PHONY: env-logs
env-logs: ## Tail stack logs
	$(COMPOSE) $(F_BASE) logs -f --tail=100

.PHONY: env-status
env-status: ## Show container health
	$(COMPOSE) $(F_BASE) ps

.PHONY: seed
seed: ## Seed the shards (SEED_PROFILE=small|medium|large)
	$(GO) run ./test/fixtures/seed -profile=$(SEED_PROFILE)

.PHONY: demo
demo: ## Stack + observability + dashboards (the 90-second demo)
	@set -euo pipefail; \
	start=$$(date +%s); \
	$(COMPOSE) $(F_BASE) $(F_OBS) up -d --wait; \
	echo "demo healthy in $$(( $$(date +%s) - start ))s"; \
	echo "Grafana: http://localhost:3000  (admin/admin)"

# ─── benchmarks (see docs/cachet-benchmarking.md) ────────────────────────────────

.PHONY: bench
bench: ## Run the benchmark suite, writing JSON to bench/results/
	@mkdir -p bench/results
	$(GO) run ./cmd/benchctl run --out bench/results

.PHONY: bench-report
bench-report: ## Regenerate the README benchmark table from bench/results/
	$(GO) run ./cmd/benchctl report --in bench/results --readme README.md

.PHONY: bench-guard
bench-guard: ## Fail on a p99 regression >10% vs the recorded baseline
	$(GO) run ./cmd/benchctl guard --in bench/results --threshold 0.10
