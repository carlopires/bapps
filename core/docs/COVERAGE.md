# Code Coverage Guide

This project uses [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) for code coverage analysis, which provides accurate LLVM-based instrumentation.

## Quick Start

### Installation

Install coverage tools:
```bash
make install-tools
```

Or manually:
```bash
cargo install cargo-llvm-cov
rustup component add llvm-tools-preview
```

### Running Coverage

**Quick terminal summary:**
```bash
make coverage-summary
```

**Full HTML report:**
```bash
make coverage          # Generate report
make coverage-open     # Generate and open in browser
```

**For CI/Codecov:**
```bash
make coverage-lcov     # Generates lcov.info
```

## Understanding the Output

### Terminal Summary

The `coverage-summary` target shows:
- **Filename**: Each source file
- **Lines**: Line coverage percentage
- **Functions**: Function coverage percentage
- **Branches**: Branch coverage percentage (conditionals, match arms)
- **Regions**: Code region coverage

Example output:
```
Filename                                        Lines    Functions  Branches   Regions
glommio/src/timer/timing_wheel.rs              87.23%    92.31%     76.47%    86.54%
glommio/src/timer/staged_wheel.rs              95.45%    100.00%    88.24%    94.12%
TOTAL                                          91.34%    96.15%     82.35%    90.33%
```

### HTML Report

The HTML report (`target/llvm-cov/html/index.html`) provides:
- **Interactive browsing** of source code
- **Line-by-line highlighting** (green = covered, red = missed)
- **Function coverage** details
- **Branch coverage** visualization
- **Sortable tables** by coverage percentage

## CI Integration

Coverage runs automatically on every push via GitHub Actions:

1. **Coverage Job**: Generates coverage and uploads to Codecov
2. **HTML Artifact**: Available as downloadable artifact for each run
3. **Codecov Badge**: Can be added to README (once repo is public)

### Codecov Setup

To enable Codecov integration:

1. Sign up at [codecov.io](https://codecov.io)
2. Add the repository
3. Get the upload token
4. Add to GitHub secrets as `CODECOV_TOKEN`

The CI workflow will automatically upload coverage on each run.

## Coverage Targets

We aim for:
- **Line coverage**: > 80% (good practice for systems code)
- **Function coverage**: > 90% (most functions tested)
- **Branch coverage**: > 70% (error paths and edge cases)

Critical paths (timer logic, reactor core) should have higher coverage (>95%).

## Tips for Improving Coverage

### Finding Uncovered Code

1. Run `make coverage-open` to view the HTML report
2. Look for red (uncovered) lines
3. Focus on:
   - Error handling paths
   - Edge cases
   - Rare timing conditions

### Common Uncovered Areas

- **Error handling**: Often untested in happy-path tests
- **Debug code**: `#[cfg(debug_assertions)]` blocks
- **Panic paths**: Deliberate panics for invalid states
- **Platform-specific**: Code behind `#[cfg(target_os = "...")]`

### Writing Effective Tests

```rust
#[test]
fn test_error_path() {
    // Test error conditions explicitly
    let result = operation_that_can_fail();
    assert!(result.is_err());
}

#[test]
fn test_edge_case() {
    // Test boundary conditions
    assert!(handle_zero_items().is_ok());
    assert!(handle_max_items().is_ok());
}
```

## Excluding Code from Coverage

Sometimes code shouldn't count toward coverage:

```rust
// Exclude entire function
#[cfg(not(tarpaulin_include))]
fn debug_only_helper() { }

// Exclude lines (use sparingly!)
if unlikely_condition {
    // LCOV_EXCL_START
    unreachable!("This should never happen");
    // LCOV_EXCL_STOP
}
```

**Note**: Use exclusions sparingly. If code can execute, it should be tested.

## Platform Notes

### macOS (via Lima)

Coverage runs inside the Lima VM:
```bash
make coverage-summary  # Automatic via Lima
```

The Makefile handles Lima integration automatically.

### Linux

Direct coverage with native io_uring:
```bash
make coverage-summary  # Native execution
```

May need memlock limits for io_uring tests:
```bash
ulimit -Sl 512
make coverage
```

## Advanced Usage

### Custom Coverage Options

Run cargo-llvm-cov directly for advanced options:

```bash
# Coverage for specific test
cargo llvm-cov --lib --test specific_test --html

# Include integration tests
cargo llvm-cov --tests --html

# Workspace coverage
cargo llvm-cov --workspace --html

# Show uncovered lines only
cargo llvm-cov --lib | grep "0.00%"
```

### Coverage in Watch Mode

Combine with `cargo-watch` for TDD:

```bash
cargo install cargo-watch
cargo watch -x "llvm-cov --lib --summary-only"
```

## Troubleshooting

### "cargo-llvm-cov not found"

Install it:
```bash
make install-tools
```

### "llvm-tools-preview not installed"

Add the component:
```bash
rustup component add llvm-tools-preview
```

### Coverage seems low

1. Check if tests are actually running: `cargo test --lib`
2. Look at HTML report to see what's missing
3. Verify test coverage: `cargo llvm-cov --lib --summary-only`

### Lima/macOS issues

Ensure Lima is running:
```bash
lima uname -a
```

If Lima isn't running, start it:
```bash
limactl start
```

## Resources

- [cargo-llvm-cov documentation](https://github.com/taiki-e/cargo-llvm-cov)
- [LLVM coverage mapping](https://llvm.org/docs/CoverageMappingFormat.html)
- [Codecov documentation](https://docs.codecov.com)
