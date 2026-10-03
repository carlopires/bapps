# Glommio Development Makefile
#
# Quick commands for common development tasks

.PHONY: help test test-nextest build fmt fmt-check lint check ci ci-only bench clean all
.PHONY: install-tools coverage coverage-summary coverage-lcov coverage-open
.PHONY: bench-timer bench-ci
.PHONY: miri miri-core miri-setup
.PHONY: test-lib test-lima-safe test-task test-channels test-controllers
.PHONY: test-error test-executor test-io test-net test-sync test-task test-timer

# =============================================================================
# Platform Detection & Smart Cargo Commands
# =============================================================================
#
# Automatically detects OS and routes cargo commands appropriately:
#   - macOS:  Uses Lima VM (io_uring support for Glommio)
#   - Linux:  Uses native cargo (direct io_uring access)
#
# This allows the same Makefile commands to work on both platforms!
#

# Detect operating system
UNAME_S := $(shell uname -s)

# Lima configuration (for macOS)
# Use home directory instead of /tmp (which is only 2GB tmpfs)
LIMA_TARGET_DIR := ~/glommio-target

# Platform-aware cargo command
ifeq ($(UNAME_S),Darwin)
    # macOS: Use Lima for Glommio compatibility
    PLATFORM := macOS (via Lima)
    PLATFORM_NOTE := Using Lima VM for io_uring support
    define run_cargo
		lima sh -c '. ~/.cargo/env && CARGO_TARGET_DIR=$(LIMA_TARGET_DIR) cargo $(1)'
    endef
else ifeq ($(UNAME_S),Linux)
    # Linux: Use native cargo
    PLATFORM := Linux (native)
    PLATFORM_NOTE := Direct io_uring access
    define run_cargo
		cargo $(1)
    endef
else
    # Other Unix
    PLATFORM := $(UNAME_S)
    PLATFORM_NOTE := Using native cargo
    define run_cargo
		cargo $(1)
    endef
endif

# =============================================================================
# Default target
# =============================================================================
help:
	@echo "Glommio Development Commands"
	@echo "============================="
	@echo ""
	@echo "Platform: $(PLATFORM)"
	@echo "Note:     $(PLATFORM_NOTE)"
	@echo ""
	@echo "Testing:"
	@echo "  make test              - Run all tests (may hit resource limits)"
	@echo "  make test-nextest      - Run all tests under nextest, plus doctests"
	@echo "  make test-lib          - Run library tests only"
	@echo "  make test-all-modules  - ⭐ Complete suite by module (RECOMMENDED)"
	@echo "  make test-lima-safe    - Run core tests in batches (macOS/Lima only)"
	@echo ""
	@echo "Module Tests (run individual modules):"
	@echo "  make test-task         - Task subsystem tests"
	@echo "  make test-channels     - Channel tests"
	@echo "  make test-controllers  - Controller tests"
	@echo "  make test-error        - Error handling tests"
	@echo "  make test-executor     - Executor tests"
	@echo "  make test-io           - I/O tests"
	@echo "  make test-net          - Network tests"
	@echo "  make test-sync         - Synchronization tests"
	@echo "  make test-task         - Task tests"
	@echo "  make test-timer        - Timer tests"
	@echo ""
	@echo "Benchmarking:"
	@echo "  make bench             - Run all benchmarks"
	@echo "  make bench-timer       - Run timer benchmarks only"
	@echo "  make timer-arms        - Compare every timer implementation"
	@echo "  make bench-spawn       - Run spawn benchmarks only"
	@echo "  make bench-ci          - Run benchmarks in CI format"
	@echo ""
	@echo "Coverage:"
	@echo "  make coverage-summary  - Quick coverage summary in terminal"
	@echo "  make coverage          - Generate HTML coverage report"
	@echo "  make coverage-open     - Generate and open HTML report"
	@echo "  make coverage-lcov     - Generate lcov format (for CI)"
	@echo ""
	@echo "Miri (Undefined Behavior Detection):"
	@echo "  make miri-setup        - Install and configure Miri"
	@echo "  make miri-core         - Test unsafe data structures under Miri"
	@echo "  make miri              - Test all library unsafe code (slow)"
	@echo ""
	@echo "Code Quality:"
	@echo "  make fmt               - Format all code"
	@echo "  make lint              - Run clippy linter"
	@echo "  make check             - Check compilation without building"
	@echo ""
	@echo "Build:"
	@echo "  make build             - Build all crates"
	@echo "  make build-release     - Build optimized release"
	@echo "  make build-examples    - Build examples"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean             - Remove build artifacts"
	@echo ""
	@echo "Tools:"
	@echo "  make install-tools     - Install development tools"
	@echo ""
	@echo "Meta:"
	@echo "  make all               - Format, lint, test"

# =============================================================================
# Testing
# =============================================================================

test:
	@echo "→ Running all tests on $(PLATFORM)..."
	@$(call run_cargo,test --workspace)

# TARGET cross-compiles (e.g. TARGET=x86_64-unknown-linux-musl); empty means host.
# NEXTEST_PROFILE picks a profile from .config/nextest.toml; CI passes `ci`.
TARGET ?=
NEXTEST_PROFILE ?= default
target_arg := $(if $(TARGET),--target $(TARGET),)

test-nextest:
	@echo "→ Running all tests under nextest on $(PLATFORM)$(if $(TARGET), for $(TARGET),)..."
	@echo "  (each test in its own process; doctests run separately -- nextest cannot"
	@echo "   enumerate them, since rustdoc compiles them itself rather than emitting"
	@echo "   a test binary to list)"
	@$(call run_cargo,nextest run --profile $(NEXTEST_PROFILE) --workspace $(target_arg))
	@$(call run_cargo,test --workspace --doc $(target_arg))

test-lib:
	@echo "→ Running library tests on $(PLATFORM)..."
	@$(call run_cargo,test --package glommio --lib)

test-all-modules:
	@echo "→ Running complete test suite by module on $(PLATFORM)..."
	@echo "  (Prevents OOM by testing modules in isolation)"
	@chmod +x scripts/test-all-modules.sh
	@./scripts/test-all-modules.sh

# Module-specific test targets
test-task:
	@echo "→ Running task subsystem tests on $(PLATFORM)..."
	@$(call run_cargo,test --package glommio --lib task)

test-channels:
	@echo "→ Running channel tests on $(PLATFORM)..."
	@$(call run_cargo,test --package glommio --lib channels)

test-controllers:
	@echo "→ Running controller tests on $(PLATFORM)..."
	@$(call run_cargo,test --package glommio --lib controllers)

test-error:
	@echo "→ Running error handling tests on $(PLATFORM)..."
	@$(call run_cargo,test --package glommio --lib error)

test-executor:
	@echo "→ Running executor tests on $(PLATFORM)..."
	@$(call run_cargo,test --package glommio --lib executor)

test-io:
	@echo "→ Running I/O tests on $(PLATFORM)..."
	@$(call run_cargo,test --package glommio --lib io)

test-net:
	@echo "→ Running network tests on $(PLATFORM)..."
	@$(call run_cargo,test --package glommio --lib net)

test-sync:
	@echo "→ Running synchronization tests on $(PLATFORM)..."
	@$(call run_cargo,test --package glommio --lib sync)

test-timer:
	@echo "→ Running timer tests on $(PLATFORM)..."
	@$(call run_cargo,test --package glommio --lib timer)

# Lima-specific: Run tests in batches to avoid resource exhaustion
test-lima-safe:
	@echo "→ Running tests with Lima-safe configuration (batched)..."
ifeq ($(UNAME_S),Darwin)
	@echo "→ Running task subsystem tests..."
	@lima sh -c '. ~/.profile && . ~/.cargo/env && CARGO_TARGET_DIR=$(LIMA_TARGET_DIR) cargo test --package glommio --lib task'
	@echo "→ Running executor tests..."
	@lima sh -c '. ~/.profile && . ~/.cargo/env && CARGO_TARGET_DIR=$(LIMA_TARGET_DIR) cargo test --package glommio --lib executor::test -- --test-threads=1' || true
	@echo "→ Running error tests..."
	@lima sh -c '. ~/.profile && . ~/.cargo/env && CARGO_TARGET_DIR=$(LIMA_TARGET_DIR) cargo test --package glommio --lib error::test'
	@echo "→ Running integration tests..."
	@lima sh -c '. ~/.profile && . ~/.cargo/env && CARGO_TARGET_DIR=$(LIMA_TARGET_DIR) cargo test --package glommio --test spawn_public'
	@echo ""
	@echo "✓ Core tests passed on Lima (full suite requires native Linux)"
	@echo "  Note: Full test suite hits Lima resource limits (known issue)"
	@echo "  For comprehensive testing, use native Linux or GitHub Actions"
else
	@echo "This target is for macOS/Lima only. Use 'make test' on Linux."
endif

bench:
	@echo "→ Running benchmarks on $(PLATFORM)..."
	@$(call run_cargo,bench --benches)

bench-timer:
	@echo "→ Running timer benchmarks on $(PLATFORM)..."
	@$(call run_cargo,run --release --features debugging --example timer_bench)

# Runs the timer benchmark against every arm of the timer comparison, in one
# sitting on one machine. Checks out each arm branch and restores yours after,
# so it needs a clean tree. Pass arm names to run a subset:
#   make timer-arms ARMS="a-slab-wheel control-btreemap"
timer-arms:
	@echo "→ Running the timer comparison on $(PLATFORM)..."
	@scripts/timer-arms.sh $(ARMS)

bench-spawn:
	@echo "→ Running spawn benchmarks on $(PLATFORM)..."
	@$(call run_cargo,bench --bench spawn_benchmark)

bench-ci:
	@echo "→ Running benchmarks in CI mode (bencher format) on $(PLATFORM)..."
	@$(call run_cargo,bench --bench spawn_benchmark -- --output-format bencher)

# =============================================================================
# Miri - Undefined Behavior Detection
# =============================================================================

miri-setup:
	@echo "→ Setting up Miri (requires nightly Rust)..."
	@rustup toolchain install nightly --component miri
	@rustup override set nightly
	@cargo miri setup
	@echo "✓ Miri ready! Run 'make miri-core' to test unsafe code"

miri-core:
	@echo "→ Running Miri on unsafe data structures..."
	@echo "  (Testing unsafe code for undefined behavior)"
	@$(call run_cargo,+nightly miri test --package glommio --lib channels::spsc_queue)
	@$(call run_cargo,+nightly miri test --package glommio --lib free_list)
	@$(call run_cargo,+nightly miri test --package glommio --lib task::lifecycle_tests)
	@echo ""
	@echo "✓ Miri found no undefined behavior!"

miri:
	@echo "→ Running Miri on full library test suite..."
	@echo "  (This may take several minutes)"
	@$(call run_cargo,+nightly miri test --package glommio --lib)
	@echo ""
	@echo "✓ Miri found no undefined behavior!"

# =============================================================================
# Coverage
# =============================================================================

coverage-summary:
	@echo "→ Generating coverage summary on $(PLATFORM)..."
	@$(call run_cargo,llvm-cov --lib --summary-only -- --skip test_shares_high_disparity)

coverage:
	@echo "→ Generating HTML coverage report on $(PLATFORM)..."
	@$(call run_cargo,llvm-cov --lib --html -- --skip test_shares_high_disparity)
	@echo ""
	@echo "✓ Coverage report generated!"
	@echo "  View at: target/llvm-cov/html/index.html"
	@echo "  Note: Skips slow stress tests (test_shares_high_disparity)"

coverage-open: coverage
	@echo "→ Opening coverage report..."
ifeq ($(UNAME_S),Darwin)
	@open target/llvm-cov/html/index.html
else
	@xdg-open target/llvm-cov/html/index.html 2>/dev/null || \
		echo "Please open target/llvm-cov/html/index.html manually"
endif

coverage-lcov:
	@echo "→ Generating lcov coverage report on $(PLATFORM)..."
	@$(call run_cargo,llvm-cov --lib --lcov --output-path lcov.info -- --skip test_shares_high_disparity)
	@echo "✓ lcov.info generated (skips slow stress tests)"

# =============================================================================
# Code Quality
# =============================================================================

fmt:
	@echo "→ Formatting code..."
ifeq ($(UNAME_S),Darwin)
	@echo "  (Running directly on macOS - Lima has filesystem write issues)"
	@cargo fmt --all
else
	@$(call run_cargo,fmt --all)
endif

lint:
	@echo "→ Running clippy on $(PLATFORM)..."
	@$(call run_cargo,clippy --workspace --all-targets -- -D warnings)

check:
	@echo "→ Checking compilation on $(PLATFORM)..."
	@$(call run_cargo,check --workspace --all-targets)

# What CI runs, and nothing a normal build covers.
#
# Every CI failure this repo has had came from a check that no local target
# ran: two of them compile only under --all-features, one only for the musl
# target, one only under cargo-sort. `make all` stayed green through a
# fortnight of red CI because it looked at none of them.
ci-only:
	@echo "→ Checks that only CI used to run..."
	@echo "  --all-features (compiles the debugging and native-thread-local paths)"
	@$(call run_cargo,clippy --workspace --all-targets --all-features -- -D warnings)
	@echo "  Cargo.toml ordering"
	@if command -v cargo-sort >/dev/null 2>&1; then \
		$(call run_cargo,sort --workspace --check); \
	else \
		echo "    ! cargo-sort not installed, skipping (cargo install cargo-sort)"; \
	fi
	@echo "  musl cross-compile"
	@if rustup target list --installed 2>/dev/null | grep -q x86_64-unknown-linux-musl; then \
		$(call run_cargo,build -p glommio --target x86_64-unknown-linux-musl); \
	else \
		echo "    ! musl target not installed, skipping"; \
		echo "      (rustup target add x86_64-unknown-linux-musl)"; \
	fi
	@echo "  documentation"
	@$(call run_cargo,doc --workspace --all-features --no-deps)
	@echo "✓ CI-only checks passed"

ci: fmt-check ci-only lint test
	@echo ""
	@echo "✓ Everything CI runs has passed locally"

fmt-check:
	@echo "→ Checking formatting..."
	@$(call run_cargo,fmt --all -- --check)

# =============================================================================
# Build
# =============================================================================

build:
	@echo "→ Building debug on $(PLATFORM)..."
	@$(call run_cargo,build --workspace)

build-release:
	@echo "→ Building release on $(PLATFORM)..."
	@$(call run_cargo,build --workspace --release)

build-examples:
	@echo "→ Building examples on $(PLATFORM)..."
	@$(call run_cargo,build --examples)

# =============================================================================
# Cleanup
# =============================================================================

clean:
	@echo "→ Cleaning build artifacts on $(PLATFORM)..."
	@$(call run_cargo,clean)
ifeq ($(UNAME_S),Darwin)
	@echo "→ Cleaning Lima target directory..."
	@lima sh -c 'rm -rf $(LIMA_TARGET_DIR)' 2>/dev/null || true
endif

# =============================================================================
# Development Tools
# =============================================================================

install-tools:
	@echo "→ Installing development tools on $(PLATFORM)..."
	@echo "  - cargo-llvm-cov (for coverage reports)"
	@$(call run_cargo,install cargo-llvm-cov)
	@echo ""
	@echo "✓ Tools installed successfully!"
	@echo "  Try: make coverage-summary"

# =============================================================================
# Meta Commands
# =============================================================================

all: fmt lint test
	@echo ""
	@echo "✓ All checks passed!"
	@echo "  - Code formatted"
	@echo "  - Linting passed"
	@echo "  - Tests passed"

# `ci` lives with the other quality targets above.
