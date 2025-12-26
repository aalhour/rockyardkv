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

# Test settings
TEST_TIMEOUT ?= 10m
FUZZ_TIME ?= 30s
RACE_FLAG ?= -race

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
build: $(SMOKE_BIN) $(STRESS_BIN) $(ADVERSARIAL_BIN) $(CRASH_BIN) ## Build all test binaries
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

.PHONY: build-release
build-release: ## Build release binaries for all platforms
	@echo "🚀 Building release binaries..."
	@mkdir -p $(DIST_DIR)
	GOOS=linux GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/smoketest-linux-amd64 ./cmd/smoketest
	GOOS=linux GOARCH=arm64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/smoketest-linux-arm64 ./cmd/smoketest
	GOOS=darwin GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/smoketest-darwin-amd64 ./cmd/smoketest
	GOOS=darwin GOARCH=arm64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/smoketest-darwin-arm64 ./cmd/smoketest
	GOOS=windows GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(DIST_DIR)/smoketest-windows-amd64.exe ./cmd/smoketest
	@echo "✅ Release binaries in $(DIST_DIR)/"

# ═══════════════════════════════════════════════════════════════════════════
# TESTING
# ═══════════════════════════════════════════════════════════════════════════
#
# Test Hierarchy:
#   test              - Quick Go tests (default, fast feedback)
#   test-full         - Thorough Go tests with race detection
#   test-fuzz         - Fuzz testing (configurable duration)
#   test-e2e-*        - End-to-end tests via binaries
#   test-e2e          - All E2E tests (short modes)
#   test-e2e-long     - All E2E tests (long modes, 10 min, 256 threads)
#   test-all          - Everything (Go + E2E short)
#   test-all-long     - Everything (Go + E2E long)
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

.PHONY: test-full
test-full: ## Run Go tests (verbose, race, extended timeout)
	@echo "🧪 Running full test suite..."
	$(GO) test -v $(RACE_FLAG) -timeout 15m ./...

.PHONY: test-verbose
test-verbose: ## Run Go tests with verbose output
	@echo "🧪 Running tests (verbose)..."
	$(GO) test -v $(RACE_FLAG) -timeout $(TEST_TIMEOUT) ./...

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

.PHONY: test-fuzz-batch
test-fuzz-batch: ## Fuzz batch parser
	@echo "🔀 Fuzzing batch parser..."
	$(GO) test -fuzz=FuzzBatchParse -fuzztime=$(FUZZ_TIME) ./internal/batch/...
	$(GO) test -fuzz=FuzzBatchRoundtrip -fuzztime=$(FUZZ_TIME) ./internal/batch/...

.PHONY: test-fuzz-encoding
test-fuzz-encoding: ## Fuzz encoding primitives
	@echo "🔀 Fuzzing encoding..."
	$(GO) test -fuzz=Fuzz -fuzztime=$(FUZZ_TIME) ./internal/encoding/...

.PHONY: test-fuzz-skiplist
test-fuzz-skiplist: ## Fuzz skiplist
	@echo "🔀 Fuzzing skiplist..."
	$(GO) test -fuzz=Fuzz -fuzztime=$(FUZZ_TIME) ./internal/memtable/...

# ─────────────────────────────────────────────────────────────────────────────
# End-to-End Tests (via test binaries)
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: test-e2e-smoke
test-e2e-smoke: $(SMOKE_BIN) ## E2E: Feature verification (~30s)
	@echo "💨 Running smoke tests..."
	$(SMOKE_BIN) -cleanup

.PHONY: test-e2e-stress
test-e2e-stress: $(STRESS_BIN) ## E2E: Concurrent correctness (1 min, 32 threads)
	@echo "🏋️ Running stress tests (1 min, 32 threads)..."
	$(STRESS_BIN) -cleanup -duration=1m -threads=32

.PHONY: test-e2e-stress-long
test-e2e-stress-long: $(STRESS_BIN) ## E2E: Concurrent correctness (10 min, 256 threads)
	@echo "🏋️ Running stress tests (10 min, 256 threads)..."
	$(STRESS_BIN) -cleanup -duration=10m -threads=256

.PHONY: test-e2e-adversarial
test-e2e-adversarial: $(ADVERSARIAL_BIN) ## E2E: Breaking attempts (2 min)
	@echo "🔥 Running adversarial tests (2 min)..."
	$(ADVERSARIAL_BIN) -cleanup

.PHONY: test-e2e-adversarial-long
test-e2e-adversarial-long: $(ADVERSARIAL_BIN) ## E2E: Breaking attempts (10 min, 256 threads)
	@echo "🔥 Running adversarial tests (10 min, 256 threads)..."
	$(ADVERSARIAL_BIN) -cleanup -long

.PHONY: test-e2e-crash
test-e2e-crash: $(CRASH_BIN) ## E2E: Crash recovery (5 cycles, synced writes)
	@echo "💥 Running crash tests (5 cycles, synced writes)..."
	$(CRASH_BIN) -cycles=5 -sync

.PHONY: test-e2e-crash-long
test-e2e-crash-long: $(CRASH_BIN) ## E2E: Crash recovery (20 cycles, synced writes)
	@echo "💥 Running crash tests (20 cycles, synced writes)..."
	$(CRASH_BIN) -cycles=20 -sync

.PHONY: test-e2e-golden
test-e2e-golden: ## E2E: C++ RocksDB compatibility
	@echo "🥇 Running golden tests (C++ compatibility)..."
	$(GO) test -v -run Golden ./...

.PHONY: test-e2e-cross-compat
test-e2e-cross-compat: ## E2E: Cross-compatibility tests (Go ↔ C++)
	@echo "🔄 Running cross-compatibility tests..."
	$(GO) test -v -run TestReadCppRocksDBSST ./internal/table/...
	$(GO) test -v -run TestGenerateGoSST ./internal/table/...

.PHONY: test-e2e
test-e2e: test-e2e-smoke test-e2e-stress test-e2e-crash test-e2e-adversarial test-e2e-golden ## Run all E2E tests (short modes)
	@echo "✅ All E2E tests complete (short modes)"

.PHONY: test-e2e-long
test-e2e-long: test-e2e-smoke test-e2e-stress-long test-e2e-crash-long test-e2e-adversarial-long test-e2e-golden ## Run all E2E tests (long modes)
	@echo "✅ All E2E tests complete (long modes)"

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
test-all: test test-e2e test-bench ## Run all tests: Go + E2E + Benchmarks
	@echo "✅ All tests complete"

.PHONY: test-all-long
test-all-long: test-full test-e2e-long test-bench ## Run all tests: Go + E2E (long) + Benchmarks
	@echo "✅ All tests complete (long modes)"

# ═══════════════════════════════════════════════════════════════════════════
# QUALITY
# ═══════════════════════════════════════════════════════════════════════════

# .PHONY: get-unix
# get-unix: ## Get unix package
# 	@echo "📥 Getting unix package..."
# 	@GOBIN=$$(pwd)/$(BIN_DIR) $(GO) get golang.org/x/sys/unix

# .PHONY: fmt
# fmt: ## Format code with gofmt and goimports
# 	@echo "🎨 Formatting code..."
# 	$(GO) fmt ./...
# 	@which goimports > /dev/null && goimports -w . || true

# .PHONY: vet
# vet: get-unix ## Run go vet
# 	@echo "🔎 Running go vet..."
# 	$(GO) vet ./...

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
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run ./...

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
gocyclo: $(GOCYCLO_BIN) ## Check cyclomatic complexity (default max $(GOCYCLO_MAX))
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

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum
	@echo "🧹 Tidying modules..."
	$(GO) mod tidy

# ═══════════════════════════════════════════════════════════════════════════
# DEVELOPMENT
# ═══════════════════════════════════════════════════════════════════════════

.PHONY: doc
doc: ## Start godoc server on http://localhost:6060
	@echo "📚 Starting godoc server on http://localhost:6060..."
	@which godoc > /dev/null || (echo "Installing godoc..." && go install golang.org/x/tools/cmd/godoc@latest)
	godoc -http=:6060

.PHONY: watch
watch: ## Watch for changes and run tests (requires watchexec)
	@echo "👀 Watching for changes..."
	@which watchexec > /dev/null || (echo "Install watchexec: brew install watchexec" && exit 1)
	watchexec -e go -r -- make test-short

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

.PHONY: clean
clean-build: ## Remove all build artifacts
	@echo "🧹 Cleaning build artifacts..."
	rm -rf $(BIN_DIR)
	rm -rf $(COV_DIR)
	rm -rf $(DIST_DIR)
	rm -f smoketest stresstest crashtest
	rm -f profile.cov coverage.out
	rm -f *.test
	rm -f *.out
	$(GO) clean -cache -testcache

.PHONY: clean-test
clean-test: ## Clean test cache only
	@echo "🧹 Cleaning test cache..."
	$(GO) clean -testcache

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
	@echo "\033[1mUsage:\033[0m make \033[36m<target>\033[0m"
	@echo ""
	@echo "\033[1m\033[32mBUILD\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(all|build|build-release):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mTESTING\033[0m"
	@echo "  \033[33m── Go Tests ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test(-short|-full|-verbose|-count)?:' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'
	@echo "  \033[33m── Fuzz Tests ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test-fuzz' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'
	@echo "  \033[33m── E2E Tests ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test-e2e' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'
	@echo "  \033[33m── Benchmarks ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test-bench' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'
	@echo "  \033[33m── Aggregate ──\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^test-all' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mQUALITY\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(lint|fmt|fmt-fix|staticcheck|modernize|modernize-fix|refactor-tips|gocritic|gocyclo|gocyclo-top|gocyclo-strict|check|coverage|coverage-func):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mDEPENDENCIES\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(deps|deps-update|tidy):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mDEVELOPMENT\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(doc|watch|info|todo):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mCLEANUP\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^clean' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mCI/CD\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^(ci|pre-commit):' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1m\033[32mHELP\033[0m"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | grep -E '^help:' | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "\033[1mExamples:\033[0m"
	@echo "  make build         Build binaries"
	@echo "  make test          Run all tests"
	@echo "  make smoke         Quick validation"
	@echo "  make ci            Full CI pipeline"
	@echo ""
	@echo "\033[1mVariables:\033[0m"
	@echo "  FUZZ_TIME=$(FUZZ_TIME)    Duration for fuzz tests"
	@echo "  TEST_TIMEOUT=$(TEST_TIMEOUT)  Timeout for test runs"
	@echo ""
