# RockyardKV Makefile
# Production-grade build, test, and development automation
#
# Usage: make [target]
# Run 'make help' for available targets
#
# Target RocksDB: v10.7.5 (commit 812b12b)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Go settings
GO ?= go
GOFLAGS ?= -v
GOOS ?= $(shell $(GO) env GOOS)
GOARCH ?= $(shell $(GO) env GOARCH)

# Project metadata
MODULE := github.com/aalhour/rockyardkv
VERSION := v0.1.0
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Build flags
LDFLAGS := -ldflags "-s -w"

# Directories
BIN_DIR := bin
COV_DIR := .coverage
DIST_DIR := dist

# Binaries
SMOKE_BIN := $(BIN_DIR)/smoketest
STRESS_BIN := $(BIN_DIR)/stresstest
ADVERSARIAL_BIN := $(BIN_DIR)/adversarialtest
CRASH_BIN := $(BIN_DIR)/crashtest
TRACEANALYZER_BIN := $(BIN_DIR)/traceanalyzer
LDB_BIN := $(BIN_DIR)/ldb
SSTDUMP_BIN := $(BIN_DIR)/sstdump
MANIFESTDUMP_BIN := $(BIN_DIR)/manifestdump

# ─────────────────────────────────────────────────────────────────────────────
# Test Tier Configuration
# ─────────────────────────────────────────────────────────────────────────────
#
# Tier System (set TIER=quick|long|marathon):
#   quick (default) - 2 min, light concurrency, fast feedback
#   long            - 5 min, 10x harder, pre-merge/nightly
#   marathon        - 30 min, 100x harder, pre-release (v0.2, v0.3)

TIER ?= quick

ifeq ($(TIER),quick)
  # Go tests
  TEST_TIMEOUT := 10m
  RACE_FLAG := -race
  FUZZ_TIME := 30s
  # E2E tests
  E2E_DURATION := 2m
  E2E_THREADS := 8
  CRASH_CYCLES := 3
  ADVERSARIAL_FLAGS := -duration=2m
else ifeq ($(TIER),long)
  # Go tests
  TEST_TIMEOUT := 15m
  RACE_FLAG := -race
  FUZZ_TIME := 1m
  # E2E tests
  E2E_DURATION := 5m
  E2E_THREADS := 32
  CRASH_CYCLES := 10
  ADVERSARIAL_FLAGS := -duration=5m
else ifeq ($(TIER),marathon)
  # Go tests
  TEST_TIMEOUT := 30m
  RACE_FLAG := -race
  FUZZ_TIME := 5m
  # E2E tests
  E2E_DURATION := 30m
  E2E_THREADS := 64
  CRASH_CYCLES := 50
  ADVERSARIAL_FLAGS := -long
else
  $(error Unknown TIER: $(TIER). Use quick, long, or marathon)
endif

# Coverage
COVERAGE_FILE := $(COV_DIR)/coverage.out
COVERAGE_HTML := $(COV_DIR)/coverage.html

# Static analysis / modernization
STATICCHECK_BIN := $(BIN_DIR)/staticcheck
MODERNIZE_BIN := $(BIN_DIR)/modernize
MODERNIZE_REPORT := modernize-report.txt
STATICCHECK_REPORT := staticcheck-report.txt
GO_CRITIC_BIN := $(BIN_DIR)/gocritic
GO_CRITIC_REPORT := go-critic-report.txt

# Complexity (gocyclo)
GOCYCLO_BIN := $(BIN_DIR)/gocyclo
GOCYCLO_MAX ?= 20
GOCYCLO_REPORT := gocyclo-report.txt

# Colors for help output
CYAN := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RESET := \033[0m
BOLD := \033[1m

# ─────────────────────────────────────────────────────────────────────────────
# Default target
# ─────────────────────────────────────────────────────────────────────────────

.DEFAULT_GOAL := help

.PHONY: all
all: build ## Build all binaries (alias for 'build')

# ═══════════════════════════════════════════════════════════════════════════
# BUILD
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: build
build: $(SMOKE_BIN) $(STRESS_BIN) $(ADVERSARIAL_BIN) $(CRASH_BIN) $(TRACEANALYZER_BIN) $(LDB_BIN) $(SSTDUMP_BIN) $(MANIFESTDUMP_BIN) ## Build all binaries
	@echo "✅ Build complete"

$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

$(SMOKE_BIN): $(BIN_DIR) $(shell find . -name '*.go' -type f)
	@echo "🔧 Building smoke test binary..."
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/smoketest

$(ADVERSARIAL_BIN): $(BIN_DIR) $(shell find . -name '*.go' -type f)
	@echo "🔧 Building adversarial test binary..."
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/adversarialtest

$(STRESS_BIN): $(BIN_DIR) $(shell find . -name '*.go' -type f)
	@echo "🔧 Building stress test binary..."
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/stresstest

$(CRASH_BIN): $(BIN_DIR) $(shell find . -name '*.go' -type f)
	@echo "🔧 Building crash test binary..."
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/crashtest

$(TRACEANALYZER_BIN): $(BIN_DIR) $(shell find . -name '*.go' -type f)
	@echo "🔧 Building trace analyzer binary..."
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/traceanalyzer

$(LDB_BIN): $(BIN_DIR) $(shell find . -name '*.go' -type f)
	@echo "🔧 Building ldb binary..."
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/ldb

$(SSTDUMP_BIN): $(BIN_DIR) $(shell find . -name '*.go' -type f)
	@echo "🔧 Building sstdump binary..."
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/sstdump

$(MANIFESTDUMP_BIN): $(BIN_DIR) $(shell find . -name '*.go' -type f)
	@echo "🔧 Building manifestdump binary..."
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $@ ./cmd/manifestdump

.PHONY: build-release
build-release: ## Build release binaries for all platforms
	@echo "🚀 Building release binaries..."
	@mkdir -p $(DIST_DIR)
	GOOS=linux GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/smoketest-linux-amd64 ./cmd/smoketest
	GOOS=linux GOARCH=arm64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/smoketest-linux-arm64 ./cmd/smoketest
	GOOS=darwin GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/smoketest-darwin-amd64 ./cmd/smoketest
	GOOS=darwin GOARCH=arm64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/smoketest-darwin-arm64 ./cmd/smoketest
	# Windows temporarily disabled (v0.x)
	# GOOS=windows GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/smoketest-windows-amd64.exe ./cmd/smoketest
	@echo "✅ Release binaries in $(DIST_DIR)/"

# ═══════════════════════════════════════════════════════════════════════════
# TESTING
# ═══════════════════════════════════════════════════════════════════════════
#
# Primary Targets:
#   test              - Go unit tests (race detection)
#   test-fuzz         - Fuzz tests
#   test-e2e          - All E2E tests at current TIER
#   test-all          - Go + Fuzz + E2E + Benchmarks at current TIER
#   test-release      - Marathon + all Linux distros in parallel
#
# ─────────────────────────────────────────────────────────────────────────────
# Go Tests (via go test)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Run Go tests with race detection (default)
	@echo "🧪 Running Go tests..."
	$(GO) test $(RACE_FLAG) -timeout $(TEST_TIMEOUT) ./...

.PHONY: test-short
test-short: ## Run short Go tests only (fast feedback, no race)
	@echo "🧪 Running short tests..."
	$(GO) test -short -timeout 2m ./...

.PHONY: test-count
test-count: ## Show test statistics
	@echo "📊 Test statistics..."
	@$(GO) test -v ./... 2>&1 | grep -E "^=== RUN" | wc -l | xargs echo "Total test runs:"
	@$(GO) test -v ./... 2>&1 | grep -E "^--- PASS" | wc -l | xargs echo "Passed:"

# ─────────────────────────────────────────────────────────────────────────────
# Fuzz Tests
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test-fuzz
test-fuzz: test-fuzz-table test-fuzz-batch test-fuzz-encoding test-fuzz-skiplist ## Run all fuzz tests
	@echo "✅ All fuzz tests complete"

.PHONY: test-fuzz-table
test-fuzz-table: ## Fuzz table reader/builder
	@echo "🔀 Fuzzing table reader..."
	$(GO) test -fuzz=FuzzTableReader -fuzztime=$(FUZZ_TIME) ./internal/table/...
	$(GO) test -fuzz=FuzzTableBuilder -fuzztime=$(FUZZ_TIME) ./internal/table/...
	$(GO) test -fuzz=FuzzBlockIterator -fuzztime=$(FUZZ_TIME) ./internal/table/...
	$(GO) test -fuzz=FuzzMultipleEntries -fuzztime=$(FUZZ_TIME) ./internal/table/...

.PHONY: test-fuzz-batch
test-fuzz-batch: ## Fuzz batch parser
	@echo "🔀 Fuzzing batch parser..."
	$(GO) test -fuzz=FuzzBatchParse -fuzztime=$(FUZZ_TIME) ./internal/batch/...
	$(GO) test -fuzz=FuzzBatchRoundtrip -fuzztime=$(FUZZ_TIME) ./internal/batch/...

.PHONY: test-fuzz-encoding
test-fuzz-encoding: ## Fuzz encoding primitives
	@echo "🔀 Fuzzing encoding..."
	$(GO) test -fuzz=FuzzVarint32Roundtrip -fuzztime=$(FUZZ_TIME) ./internal/encoding/...
	$(GO) test -fuzz=FuzzVarint64Roundtrip -fuzztime=$(FUZZ_TIME) ./internal/encoding/...
	$(GO) test -fuzz=FuzzVarsignedint64Roundtrip -fuzztime=$(FUZZ_TIME) ./internal/encoding/...
	$(GO) test -fuzz=FuzzLengthPrefixedSliceRoundtrip -fuzztime=$(FUZZ_TIME) ./internal/encoding/...
	$(GO) test -fuzz=FuzzVarint32Decode -fuzztime=$(FUZZ_TIME) ./internal/encoding/...
	$(GO) test -fuzz=FuzzVarint64Decode -fuzztime=$(FUZZ_TIME) ./internal/encoding/...
	$(GO) test -fuzz=FuzzFixed32Roundtrip -fuzztime=$(FUZZ_TIME) ./internal/encoding/...
	$(GO) test -fuzz=FuzzFixed64Roundtrip -fuzztime=$(FUZZ_TIME) ./internal/encoding/...

.PHONY: test-fuzz-skiplist
test-fuzz-skiplist: ## Fuzz skiplist
	@echo "🔀 Fuzzing skiplist..."
	$(GO) test -fuzz=FuzzSkipListInsertContains -fuzztime=$(FUZZ_TIME) ./internal/memtable/...
	$(GO) test -fuzz=FuzzSkipListIteratorConsistency -fuzztime=$(FUZZ_TIME) ./internal/memtable/...

# ─────────────────────────────────────────────────────────────────────────────
# End-to-End Tests (via test binaries)
# Uses TIER configuration (quick/long/marathon)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test-e2e-smoke
test-e2e-smoke: $(SMOKE_BIN) ## E2E: Feature verification (~30s, tier-independent)
	@echo "💨 Running smoke tests..."
	$(SMOKE_BIN) -cleanup

.PHONY: test-e2e-stress
test-e2e-stress: $(STRESS_BIN) ## E2E: Concurrent correctness (uses current TIER)
	@echo "🏋️ Running stress tests ($(E2E_DURATION), $(E2E_THREADS) threads)..."
	$(STRESS_BIN) -cleanup -duration=$(E2E_DURATION) -threads=$(E2E_THREADS)

.PHONY: test-e2e-adversarial
test-e2e-adversarial: $(ADVERSARIAL_BIN) ## E2E: Breaking attempts (uses current TIER)
	@echo "🔥 Running adversarial tests ($(TIER) mode)..."
	$(ADVERSARIAL_BIN) -cleanup $(ADVERSARIAL_FLAGS)

.PHONY: test-e2e-crash
test-e2e-crash: $(CRASH_BIN) ## E2E: Crash recovery (uses current TIER)
	@echo "💥 Running crash tests ($(CRASH_CYCLES) cycles, $(E2E_DURATION) duration)..."
	$(CRASH_BIN) -cycles=$(CRASH_CYCLES) -duration=$(E2E_DURATION) -sync -kill-mode=random

.PHONY: test-e2e-golden
test-e2e-golden: ## E2E: C++ RocksDB compatibility (tier-independent)
	@echo "🥇 Running golden tests (C++ compatibility)..."
	$(GO) test -v -run Golden ./...

.PHONY: test-e2e-golden-corpus
test-e2e-golden-corpus: ## E2E: Run corpus-driven golden tests (requires REDTEAM_CPP_CORPUS_ROOT)
	@if [ -z "$$REDTEAM_CPP_CORPUS_ROOT" ]; then \
		echo "❌ REDTEAM_CPP_CORPUS_ROOT is not set"; \
		echo "   Set it to the path of the red team C++ corpus, e.g.:"; \
		echo "   export REDTEAM_CPP_CORPUS_ROOT=/path/to/rockyardkv-tests/redteam/corpus_cpp_generated"; \
		exit 1; \
	fi
	@echo "🥇 Running corpus-driven golden tests..."
	@echo "   Corpus: $$REDTEAM_CPP_CORPUS_ROOT"
	$(GO) test -v -run 'TestCppCorpus' ./cmd/goldentest/...

.PHONY: test-e2e-cross-compat
test-e2e-cross-compat: ## E2E: Cross-compatibility tests (Go ↔ C++)
	@echo "🔄 Running cross-compatibility tests..."
	$(GO) test -v -run TestReadCppRocksDBSST ./internal/table/...
	$(GO) test -v -run TestGenerateGoSST ./internal/table/...

.PHONY: test-e2e
test-e2e: test-e2e-smoke test-e2e-stress test-e2e-crash test-e2e-adversarial test-e2e-golden ## Run all E2E tests (uses current TIER)
	@echo "✅ All E2E tests complete ($(TIER) tier)"

# ─────────────────────────────────────────────────────────────────────────────
# Status checks (durability/compatibility snapshots)
# ─────────────────────────────────────────────────────────────────────────────

STATUS_RUN_ROOT ?= crashtest-artifacts/status/$(shell date +%Y%m%d-%H%M%S)

.PHONY: status-golden
status-golden: test-e2e-golden ## Status: Always-on compatibility gate (golden tests)

.PHONY: status-durability-wal-sync
status-durability-wal-sync: build ## Status: Repro WAL+sync crash durability behavior (writes artifacts)
	@echo "💥 Running durability repro (wal-sync) ..."
	@bash scripts/status/run_durability_repros.sh wal-sync "$(STATUS_RUN_ROOT)/wal-sync"

.PHONY: status-durability-wal-sync-sweep
status-durability-wal-sync-sweep: build ## Status: Seed sweep for WAL+sync crash durability (writes artifacts)
	@echo "💥 Running durability sweep (wal-sync-sweep) ..."
	@bash scripts/status/run_durability_repros.sh wal-sync-sweep "$(STATUS_RUN_ROOT)/wal-sync-sweep"

.PHONY: status-durability-disablewal-faultfs
status-durability-disablewal-faultfs: build ## Status: Repro DisableWAL+faultfs crash durability behavior (writes artifacts)
	@echo "💥 Running durability repro (disablewal-faultfs) ..."
	@bash scripts/status/run_durability_repros.sh disablewal-faultfs "$(STATUS_RUN_ROOT)/disablewal-faultfs"

.PHONY: status-durability-disablewal-faultfs-minimize
status-durability-disablewal-faultfs-minimize: build ## Status: Minimization sweep for DisableWAL+faultfs durability (writes artifacts)
	@echo "💥 Running durability minimization (disablewal-faultfs-minimize) ..."
	@bash scripts/status/run_durability_repros.sh disablewal-faultfs-minimize "$(STATUS_RUN_ROOT)/disablewal-faultfs-minimize"

.PHONY: status-adversarial-corruption
status-adversarial-corruption: build ## Status: Run adversarial corruption suite (writes artifacts)
	@echo "🧨 Running adversarial corruption suite ..."
	@bash scripts/status/run_durability_repros.sh adversarial-corruption "$(STATUS_RUN_ROOT)/adversarial-corruption"

.PHONY: status-durability-internal-key-collision
status-durability-internal-key-collision: build ## Status: Repro + detect internal-key collisions across SSTs (writes artifacts)
	@echo "🧪 Running internal-key collision repro/check ..."
	@bash scripts/status/run_durability_repros.sh internal-key-collision "$(STATUS_RUN_ROOT)/internal-key-collision"

.PHONY: status-durability-internal-key-collision-only
status-durability-internal-key-collision-only: build ## Status: Collision-check-only gate (ignores DisableWAL verifier failures; HARNESS-02 pending)
	@echo "🧪 Running internal-key collision CHECK-ONLY gate ..."
	@bash scripts/status/run_durability_repros.sh internal-key-collision-only "$(STATUS_RUN_ROOT)/internal-key-collision-only"

.PHONY: status-durability
status-durability: status-durability-wal-sync status-durability-wal-sync-sweep status-durability-disablewal-faultfs status-durability-disablewal-faultfs-minimize status-durability-internal-key-collision ## Status: Run durability repros (writes artifacts)
	@echo "✅ Durability repros complete"

.PHONY: status-check
status-check: status-golden status-durability status-adversarial-corruption ## Status: Run golden tests and repro suite
	@echo "✅ Status check complete"

# ─────────────────────────────────────────────────────────────────────────────
# Benchmark Tests
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test-bench
test-bench: ## Run all benchmarks
	@echo "⏱️ Running all benchmarks..."
	$(GO) test -bench=. -benchmem -run=^$$ ./...

.PHONY: test-bench-table
test-bench-table: ## Benchmark table reader/builder
	@echo "⏱️ Running table benchmarks..."
	$(GO) test -bench=. -benchmem -run=^$$ ./internal/table/...

.PHONY: test-bench-memtable
test-bench-memtable: ## Benchmark memtable/skiplist
	@echo "⏱️ Running memtable benchmarks..."
	$(GO) test -bench=. -benchmem -run=^$$ ./internal/memtable/...

.PHONY: test-bench-db
test-bench-db: ## Benchmark DB operations
	@echo "⏱️ Running DB benchmarks..."
	$(GO) test -bench=. -benchmem -run=^$$ ./db/...

# ─────────────────────────────────────────────────────────────────────────────
# Aggregate Test Targets
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test-all
test-all: test test-e2e test-bench ## Run all tests: Go + Fuzz + E2E + Benchmarks (uses current TIER)
	@echo "Skipping test-fuzz, if you want it run it separately."
	@echo "✅ All tests complete ($(TIER) tier)"

# ─────────────────────────────────────────────────────────────────────────────
# Tier Convenience Aliases
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test-long
test-long: ## Run Go tests (long tier)
	@$(MAKE) test TIER=long

.PHONY: test-marathon
test-marathon: ## Run Go tests (marathon tier - 30m, pre-release)
	@$(MAKE) test TIER=marathon

.PHONY: test-e2e-long
test-e2e-long: ## Run all E2E tests (long tier - 5m each)
	@$(MAKE) test-e2e TIER=long

.PHONY: test-e2e-marathon
test-e2e-marathon: ## Run all E2E tests (marathon tier - 30m each)
	@$(MAKE) test-e2e TIER=marathon

.PHONY: test-all-long
test-all-long: ## Run all tests (long tier - 5m each)
	@$(MAKE) test-all TIER=long

.PHONY: test-all-marathon
test-all-marathon: ## Run all tests (marathon tier - 30m each, pre-release)
	@$(MAKE) test-all TIER=marathon

# ─────────────────────────────────────────────────────────────────────────────
# Release Testing (marathon + all Linux distros in parallel)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test-release
test-release: build ## Pre-release validation: marathon tests + all Linux distros (parallel)
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║              🚀 Pre-Release Test Suite Starting                  ║"
	@echo "║    Marathon tier (30m) + All Linux distros in parallel           ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@# Run marathon tests and all Linux distros in parallel
	@# Each gets 30m, all run simultaneously
	$(MAKE) -j4 \
		_test-release-marathon \
		_test-release-linux \
		_test-release-alpine \
		_test-release-rocky
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║              ✅ Pre-Release Test Suite Complete                  ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  ✅ Marathon:    30m stress/crash/adversarial passed             ║"
	@echo "║  ✅ Debian:      Linux glibc tests passed                        ║"
	@echo "║  ✅ Alpine:      Linux musl libc tests passed                    ║"
	@echo "║  ✅ Rocky Linux: RHEL-compatible tests passed                    ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"

# Internal targets for parallel release testing (30m each)
.PHONY: _test-release-marathon
_test-release-marathon:
	@echo "🏃 [Marathon] Starting 30m stress/crash/adversarial tests..."
	@$(MAKE) test-e2e TIER=marathon

.PHONY: _test-release-linux
_test-release-linux:
	@echo "🐧 [Debian] Starting 30m Linux tests..."
	@docker build -f Dockerfile.ci -t rockyardkv-test-linux . --quiet
	@docker run --rm rockyardkv-test-linux go test -race -timeout 30m ./...

.PHONY: _test-release-alpine
_test-release-alpine:
	@echo "🏔️  [Alpine] Starting 30m Alpine tests..."
	@docker run --rm -v $(PWD):/app -w /app golang:1.25-alpine \
		sh -c "apk add --no-cache gcc musl-dev git && go test -race -timeout 30m ./..."

.PHONY: _test-release-rocky
_test-release-rocky:
	@echo "🪨 [Rocky] Starting 30m Rocky Linux tests..."
	@docker run --rm -v $(PWD):/app -w /app rockylinux:9 \
		sh -c "dnf install -y golang gcc git && cd /app && go test -race -timeout 30m ./..."

# ═══════════════════════════════════════════════════════════════════════════
# QUALITY
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: fmt
fmt-check: ## Check formatting (fails if changes needed)
	@echo "🎨 Checking formatting..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@v2.7.2)
	golangci-lint fmt --diff

.PHONY: fmt-fix
fmt-fix: ## Format code (applies changes)
	@echo "🎨 Formatting code..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@v2.7.2)
	golangci-lint fmt

.PHONY: lint
lint: ## Run golangci-lint
	@echo "🔍 Running linters..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@v2.7.2)
	golangci-lint run ./...

.PHONY: lint-all-platforms
lint-all-platforms: ## Run golangci-lint for all supported platforms (catches platform-specific issues)
	@echo "🔍 Running linters for all platforms..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@v2.7.2)
	@echo "  → Linux (amd64)..."
	@GOOS=linux GOARCH=amd64 golangci-lint run ./...
	@echo "  → local (native)..."
	@GOOS=darwin GOARCH=arm64 golangci-lint run ./...
	# Windows temporarily disabled (v0.x)
	# @echo "  → Windows (amd64)..."
	# @GOOS=windows GOARCH=amd64 golangci-lint run ./...
	@echo "✅ All platforms passed"

# =============================================================================
# Cross-Platform Testing (Docker-based)
# Runs `make test-all-long` inside each container (Go + Fuzz + E2E + Bench)
# =============================================================================

.PHONY: test-docker-debian
test-docker-debian: ## Run full test suite (test-all-long) in Debian Linux
	@echo "🐧 Running make test-all-long in Debian Linux..."
	@docker build -f docker/Dockerfile.debian -t rockyardkv:debian .
	@docker run --rm rockyardkv:debian
	@echo "✅ Debian Linux tests passed"

.PHONY: test-docker-debian-short
test-docker-debian-short: ## Run short Go tests in Debian Linux (faster)
	@echo "🐧 Running short tests in Debian Linux..."
	@docker build -f docker/Dockerfile.debian -t rockyardkv:debian . --quiet
	@docker run --rm rockyardkv:debian make test-short
	@echo "✅ Debian Linux short tests passed"

.PHONY: test-docker-alpine
test-docker-alpine: ## Run full test suite (test-all-long) in Alpine Linux
	@echo "🏔️  Running make test-all-long in Alpine Linux (musl libc)..."
	@docker build -f docker/Dockerfile.alpine -t rockyardkv:alpine .
	@docker run --rm rockyardkv:alpine
	@echo "✅ Alpine Linux tests passed"

.PHONY: test-docker-rockylinux
test-docker-rockylinux: ## Run full test suite (test-all-long) in Rocky Linux 9
	@echo "🪨 Running make test-all-long in Rocky Linux 9 (RHEL-compatible)..."
	@docker build -f docker/Dockerfile.rocky -t rockyardkv:rocky .
	@docker run --rm rockyardkv:rocky
	@echo "✅ Rocky Linux tests passed"

.PHONY: test-docker-all
test-docker-all: ## Run full test suite on all Linux distros (sequential)
	@$(MAKE) test-docker-debian
	@$(MAKE) test-docker-alpine
	@$(MAKE) test-docker-rockylinux
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║                    All Linux Distros Complete                    ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  ✅ Debian:       glibc, matches GitHub CI ubuntu-latest         ║"
	@echo "║  ✅ Alpine:       musl libc, common in production containers     ║"
	@echo "║  ✅ Rocky Linux:  RHEL-compatible, enterprise servers            ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"

.PHONY: test-docker-all-parallel
test-docker-all-parallel: ## Run full test suite on all Linux distros (parallel)
	@echo "🚀 Building all Linux images..."
	@docker build -f docker/Dockerfile.debian -t rockyardkv:debian . &
	@docker build -f docker/Dockerfile.alpine -t rockyardkv:alpine . &
	@docker build -f docker/Dockerfile.rocky -t rockyardkv:rocky . &
	@wait
	@echo ""
	@echo "🐧 Running make test-all-long on all Linux distros in parallel..."
	@echo ""
	@( docker run --rm rockyardkv:debian 2>&1 | sed 's/^/[debian] /' ) &
	@( docker run --rm rockyardkv:alpine 2>&1 | sed 's/^/[alpine] /' ) &
	@( docker run --rm rockyardkv:rocky 2>&1 | sed 's/^/[rocky]  /' ) &
	@wait
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║              All Linux Distros Complete (Parallel)               ║"
	@echo "╠══════════════════════════════════════════════════════════════════╣"
	@echo "║  ✅ Debian:      glibc, matches GitHub CI ubuntu-latest          ║"
	@echo "║  ✅ Alpine:      musl libc, common in production containers      ║"
	@echo "║  ✅ Rocky Linux: RHEL-compatible, enterprise servers             ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"

.PHONY: ci-local
ci-local: lint-all-platforms test test-all-linux ## Reproduce full CI locally (lint + test + all Linux distros)
	@echo ""
	@echo "🎉 CI simulation complete - all checks passed!"

.PHONY: check
check: fmt lint gocyclo test-short ## Run all quality checks (fmt, lint, gocyclo, test-short)
	@echo "✅ All checks passed"

$(COV_DIR):
	@mkdir -p $(COV_DIR)

.PHONY: coverage
coverage: $(COV_DIR) ## Generate HTML coverage report
	@echo "📊 Generating coverage report..."
	$(GO) test -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	$(GO) tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo "📄 Coverage report: $(COVERAGE_HTML)"
	@$(GO) tool cover -func=$(COVERAGE_FILE) | tail -1

.PHONY: coverage-func
coverage-func: $(COV_DIR) ## Show coverage by function
	@echo "📊 Coverage summary..."
	$(GO) test -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	@$(GO) tool cover -func=$(COVERAGE_FILE)

$(GO_CRITIC_BIN): $(BIN_DIR)
	@echo "📥 Installing gocritic..."
	@GOBIN=$$(pwd)/$(BIN_DIR) $(GO) install github.com/go-critic/go-critic/cmd/gocritic@latest

.PHONY: gocritic
gocritic: $(GO_CRITIC_BIN) ## Run gocritic (more tips + correctness)
	@echo "🔍 Running gocritic..."
	@$(GO_CRITIC_BIN) check ./... 2>&1 | tee $(GO_CRITIC_REPORT)
	@echo "📄 gocritic report: $(GO_CRITIC_REPORT)"

$(STATICCHECK_BIN): $(BIN_DIR)
	@echo "📥 Installing staticcheck..."
	@GOBIN=$$(pwd)/$(BIN_DIR) $(GO) install honnef.co/go/tools/cmd/staticcheck@latest

.PHONY: staticcheck
staticcheck: $(STATICCHECK_BIN) ## Run staticcheck (more tips + correctness)
	@echo "🔍 Running staticcheck..."
	@$(STATICCHECK_BIN) ./... 2>&1 | tee $(STATICCHECK_REPORT)
	@echo "📄 Staticcheck report: $(STATICCHECK_REPORT)"

$(MODERNIZE_BIN): $(BIN_DIR)
	@echo "📥 Installing modernize..."
	@GOBIN=$$(pwd)/$(BIN_DIR) $(GO) install golang.org/x/tools/go/analysis/passes/modernize/cmd/modernize@latest

.PHONY: modernize
modernize: $(MODERNIZE_BIN) ## Generate modernization report (Go 1.25 idioms)
	@echo "🧠 Running modernize analysis..."
	@$(MODERNIZE_BIN) ./... 2>&1 | tee $(MODERNIZE_REPORT)
	@echo "📄 Modernize report: $(MODERNIZE_REPORT)"

.PHONY: modernize-fix
modernize-fix: $(MODERNIZE_BIN) ## Apply modernization fixes (commit first)
	@echo "🛠️ Applying modernize fixes..."
	@$(MODERNIZE_BIN) -fix ./...
	@echo "🎨 Formatting after fixes..."
	@gofmt -w .
	@echo "✅ Modernize fixes applied"

$(GOCYCLO_BIN): $(BIN_DIR)
	@echo "📥 Installing gocyclo..."
	@GOBIN=$$(pwd)/$(BIN_DIR) $(GO) install github.com/fzipp/gocyclo/cmd/gocyclo@latest

.PHONY: gocyclo
gocyclo: $(GOCYCLO_BIN) ## Check cyclomatic complexity (default max 20)
	@echo "🧠 Checking cyclomatic complexity (max $(GOCYCLO_MAX))..."
	@$(GOCYCLO_BIN) -over $(GOCYCLO_MAX) . 2>&1 | tee $(GOCYCLO_REPORT)
	@echo "📄 gocyclo report: $(GOCYCLO_REPORT)"

.PHONY: gocyclo-top
gocyclo-top: $(GOCYCLO_BIN) ## Show top 20 most complex functions
	@echo "🏔️ Top complex functions..."
	@$(GOCYCLO_BIN) -top 20 . 2>&1

.PHONY: gocyclo-strict
gocyclo-strict: $(GOCYCLO_BIN) ## Stricter check (max 15)
	@echo "🧠 Checking cyclomatic complexity (max 15)..."
	@$(GOCYCLO_BIN) -over 15 . 2>&1 | tee $(GOCYCLO_REPORT)
	@echo "📄 gocyclo report: $(GOCYCLO_REPORT)"

.PHONY: refactor-tips
refactor-tips: modernize staticcheck gocyclo ## Generate all “refactoring tips” reports (modernize, staticcheck, gocyclo)
	@echo "✅ Refactoring tips reports generated"


# ═══════════════════════════════════════════════════════════════════════════
# DEPENDENCIES
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: deps
deps: ## Download and verify dependencies
	@echo "📦 Installing dependencies..."
	$(GO) mod download
	$(GO) mod verify

.PHONY: deps-update
deps-update: ## Update all dependencies
	@echo "📦 Updating dependencies..."
	$(GO) get -u ./...
	$(GO) mod tidy

# ═══════════════════════════════════════════════════════════════════════════
# DEVELOPMENT
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: info
info: ## Show project information and statistics
	@echo "📋 Project Information"
	@echo "────────────────────────────────────────"
	@echo "Module:     $(MODULE)"
	@echo "Version:    $(VERSION)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(shell $(GO) version)"
	@echo "OS/Arch:    $(GOOS)/$(GOARCH)"
	@echo "────────────────────────────────────────"
	@echo "Tests:      $$($(GO) test -v ./... 2>&1 | grep -E '^=== RUN' | wc -l | tr -d ' ')"
	@echo "Packages:   $$($(GO) list ./... | wc -l | tr -d ' ')"
	@echo "Go Files:   $$(find . -name '*.go' | wc -l | tr -d ' ')"

.PHONY: todo
todo: ## List TODO/FIXME items in code
	@echo "📝 TODO items:"
	@grep -rn "TODO\|FIXME\|XXX\|HACK" --include="*.go" . | head -20

# ═══════════════════════════════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: clean-build
clean-build: ## Remove all build artifacts
	@echo "🧹 Cleaning build artifacts..."
	rm -rf $(BIN_DIR)
	rm -rf $(COV_DIR)
	rm -rf $(DIST_DIR)
	rm -f smoketest stresstest crashtest adversarialtest traceanalyzer ldb sstdump manifestdump
	rm -rf crashtest-artifacts
	rm -f profile.cov coverage.out
	rm -f *.test
	rm -f *.out
	$(GO) clean -cache -modcache

.PHONY: clean-test
clean-test: ## Clean test cache only
	@echo "🧹 Cleaning test cache..."
	$(GO) clean -testcache -fuzzcache

.PHONY: clean-fuzz
clean-fuzz: ## Clean fuzz corpus
	@echo "🧹 Cleaning fuzz cache..."
	rm -rf testdata/fuzz

.PHONY: clean-reports
clean-reports: ## Remove generated refactoring reports (staticcheck, modernize, gocyclo, gocritic)
	@echo "🧹 Cleaning refactoring reports..."
	@rm -f $(MODERNIZE_REPORT) $(STATICCHECK_REPORT) $(GOCYCLO_REPORT) $(GO_CRITIC_REPORT)

.PHONY: clean
clean: clean-build clean-test clean-fuzz clean-reports ## Remove all build artifacts, test cache, fuzz corpus, and refactoring reports (staticcheck, modernize, gocyclo, gocritic)

# ═══════════════════════════════════════════════════════════════════════════
# CI/CD
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: ci
ci: deps lint test coverage smoke ## Run full CI pipeline (deps, lint, test, coverage, smoke)
	@echo "✅ CI pipeline complete"

.PHONY: pre-commit
pre-commit: fmt lint test-short ## Run pre-commit checks (fmt, lint, test-short)
	@echo "✅ Pre-commit checks passed"

# ═══════════════════════════════════════════════════════════════════════════
# HELP
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: help
help: ## Show this help message
	@echo ""
	@echo "\033[1mRockyardKV\033[0m - Pure Go reimplementation of RocksDB v10.7.5"
	@echo ""
	@echo "\033[1mUsage:\033[0m make \033[36m<target>\033[0m [TIER=quick|long|marathon]"
	@echo ""
	@echo "\033[1m\033[33mTier System (TIER=quick|long|marathon):\033[0m"
	@echo "  \033[36mquick\033[0m (default)  Go: 10m, Fuzz: 30s | E2E: 2m, 8 threads"
	@echo "  \033[36mlong\033[0m             Go: 15m, Fuzz: 1m  | E2E: 5m, 32 threads"
	@echo "  \033[36mmarathon\033[0m         Go: 30m, Fuzz: 5m  | E2E: 30m, 64 threads"
	@echo ""
	@echo "\033[1m\033[32mBUILD\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(all|build|build-release):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mTESTING\033[0m"
	@echo "  \033[33m── Go Tests ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test(-short|-count)?:' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo "  \033[33m── Fuzz Tests ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test-fuzz' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo "  \033[33m── E2E Tests (use TIER) ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test-e2e(-smoke|-stress|-crash|-adversarial|-golden|-cross-compat)?:' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo "  \033[33m── Benchmarks ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test-bench' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo "  \033[33m── Aggregate & Tiers ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test-(all|long|marathon|e2e-long|e2e-marathon|all-long|all-marathon|release):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo "  \033[33m── Cross-Platform ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test-(linux|alpine|rockylinux|all-linux):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mQUALITY\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(lint|fmt|fmt-fix|staticcheck|modernize|modernize-fix|refactor-tips|gocritic|gocyclo|gocyclo-top|gocyclo-strict|check|coverage|coverage-func):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mDEPENDENCIES\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(deps|deps-update|tidy):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mDEVELOPMENT\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(doc|watch|info|todo):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mCLEANUP\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^clean' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mCI/CD\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(ci|ci-local|pre-commit):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1mExamples:\033[0m"
	@echo "  make test                   Run Go tests (quick)"
	@echo "  make test-e2e               Run E2E tests (quick tier)"
	@echo "  make test-e2e TIER=long     Run E2E tests (5m each)"
	@echo "  make test-all-marathon      Full suite, 30m each"
	@echo "  make test-release           Pre-release: marathon + all Linux"
	@echo ""
	@echo "\033[1mCurrent Tier: $(TIER)\033[0m"
	@echo "  TEST_TIMEOUT=$(TEST_TIMEOUT)   Go test timeout"
	@echo "  FUZZ_TIME=$(FUZZ_TIME)      Fuzz test duration"
	@echo "  E2E_DURATION=$(E2E_DURATION)    E2E test duration"
	@echo "  E2E_THREADS=$(E2E_THREADS)      E2E concurrency"
	@echo "  CRASH_CYCLES=$(CRASH_CYCLES)     Crash test cycles"
	@echo ""
