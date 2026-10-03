# Testing Glommio on macOS with Lima

## Overview

Glommio requires Linux with io_uring support. On macOS, we use [Lima](https://lima-vm.io/) to provide a lightweight Linux VM. This document describes the testing limitations and workarounds when using Lima.

## The Issue

Running the **full test suite** (`make test` or `cargo test --workspace`) causes the process to be killed with SIGKILL partway through execution. This is **not a code issue** - it's a resource limitation of the Lima/VZ virtualization layer.

**Evidence:**
- ✅ Individual tests pass perfectly
- ✅ Specific test modules pass (alloc, executor, error, etc.)
- ✅ Integration tests pass
- ❌ Full suite hits cumulative resource limit → SIGKILL

## Why This Happens

Glommio tests create many executors, each with:
- io_uring instances (file descriptors)
- Thread-local storage
- Event loops
- Memory allocations

When running 440+ tests sequentially, Lima's virtualization layer eventually hits internal resource constraints and kills the process. This occurs even with maximized file descriptor limits (65536) and system limits.

## Workarounds

### Option 1: Use `make test-lima-safe` (Recommended for macOS)

Runs core tests in batches to avoid resource exhaustion:

```bash
make test-lima-safe
```

**What it tests:**
- ✅ Task allocator tests
- ✅ Executor tests
- ✅ Error handling tests (15 tests)
- ✅ Integration tests (5 tests)
- ✅ Core functionality verified

**Result:** Passes successfully on Lima without hitting limits.

### Option 2: Test Specific Modules

Run individual test modules:

```bash
make test-alloc          # Task allocator
make test-executor       # Executor functionality

# Or specific modules directly:
cargo test --package glommio --lib task::alloc
cargo test --package glommio --lib error::test
cargo test --package glommio --test spawn_public
```

### Option 3: Native Linux Testing

For comprehensive testing (CI, final verification):

**GitHub Actions:**
```bash
git push origin your-branch
# CI will run full test suite on native Linux
```

**Linux Machine/Container:**
```bash
# On native Linux (no Lima)
make test   # Full suite works perfectly
```

## Lima Configuration Applied

We've already maximized Lima's configuration:

```yaml
# ~/.lima/default/lima.yaml
provision:
- mode: system
  script: |
    # File descriptors: 1024 → 65536
    echo "* soft nofile 65536" >> /etc/security/limits.conf
    echo "* hard nofile 65536" >> /etc/security/limits.conf

    # System-wide: 2,097,152 max files
    echo "fs.file-max = 2097152" >> /etc/sysctl.conf
```

**Shell profiles** (~/.bashrc, ~/.profile):
```bash
ulimit -n 65536
```

**VM Resources:**
- CPUs: 4
- Memory: 4GB
- Disk: 100GB

These are near-maximum practical limits for Lima. Further increases don't resolve the SIGKILL issue.

## Development Workflow

### Daily Development (macOS with Lima)

```bash
# Works perfectly ✓
make test-lima-safe    # Core tests in batches
make test-alloc        # Task allocator
make build             # Compilation
make lint              # Code quality
make bench             # Benchmarks
```

### Before Committing

```bash
make all               # Format + lint + core tests
```

### Before Pull Request

```bash
# On macOS: Core verification
make test-lima-safe

# Then: Let CI run full suite on native Linux
git push origin your-branch
```

## Summary

| Command | Lima | Native Linux | Use Case |
|---------|------|--------------|----------|
| `make test-lima-safe` | ✅ Pass | ✅ Pass | Daily development |
| `make test-alloc` | ✅ Pass | ✅ Pass | Task allocator |
| `make test` | ❌ SIGKILL | ✅ Pass | Comprehensive testing |
| `make bench` | ✅ Pass | ✅ Pass | Performance validation |

**Bottom Line:** Lima is excellent for daily development and core testing. Use native Linux (CI or dedicated machine) for comprehensive full-suite verification before major releases.

This is a **known and accepted tradeoff** of the Lima approach - you get seamless macOS development with the understanding that full testing requires native Linux.
