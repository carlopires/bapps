# Continuous Benchmarking Guide

This project uses [Criterion](https://github.com/bheisler/criterion.rs) for benchmarking and [github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark) for continuous performance tracking.

## Quick Start

### Running Benchmarks Locally

**Run all benchmarks:**
```bash
make bench
```

**Run specific benchmark:**
```bash
cargo bench --bench timer_benchmark
```

**Run with memlock limits (required for io_uring benchmarks):**
```bash
ulimit -Sl 512
ulimit -Hl 512
cargo bench --bench timer_benchmark
```

### Viewing Results

Criterion stores results in `target/criterion/`:
- **HTML reports**: `target/criterion/report/index.html` (open in browser)
- **Comparison data**: Automatically compares against previous runs
- **Statistics**: Mean, median, standard deviation, outliers

## CI Integration

### Automatic Performance Tracking

The benchmark workflow (`.github/workflows/bench.yml`) runs on:
- **Every push to master**: Stores results and updates trend chart
- **Every pull request**: Compares against master baseline

### Features

#### 1. Historical Trend Chart

View long-term performance trends at:
```
https://<username>.github.io/<repo>/dev/bench/
```

The chart shows:
- Performance over time for each benchmark
- Regression/improvement trends
- Hover for exact values and commit info

#### 2. Pull Request Comparison

Every PR gets an automated comment showing:
- **Performance delta** vs. master baseline
- **Per-benchmark comparison** with percentage change
- **Regression alerts** if performance degrades >20%

Example PR comment:
```
Timer Benchmarks
┌─────────────────────────┬──────────┬──────────┬─────────┐
│ Benchmark               │ Master   │ PR       │ Change  │
├─────────────────────────┼──────────┼──────────┼─────────┤
│ insert/256              │ 12.3 ns  │ 11.8 ns  │ -4.07%  │
│ insert/1024             │ 15.7 ns  │ 16.9 ns  │ +7.64%  │
│ remove/256              │ 8.2 ns   │ 8.5 ns   │ +3.66%  │
└─────────────────────────┴──────────┴──────────┴─────────┘

⚠️ Performance regression detected:
  - insert/1024: +7.64% (above 5% threshold)
```

#### 3. Regression Alerts

If any benchmark regresses by >20%:
- ⚠️ **Comment posted** highlighting the regression
- 🔔 **Notification** to maintainers
- ❌ **Workflow marked** (but doesn't fail - configurable)

### Alert Threshold

Current threshold: **120%** (20% regression)

This is conservative for micro-benchmarks on shared CI runners. Adjust in `.github/workflows/bench.yml`:
```yaml
alert-threshold: '110%'  # Alert on 10% regression
```

## Understanding Results

### Interpreting Nanosecond-Level Benchmarks

**10-20ns benchmarks** (like timer operations) are affected by:
- **CPU frequency scaling**: TurboBoost variance
- **Cache state**: Hot vs cold cache
- **Noisy neighbors**: Shared CI runners
- **Branch prediction**: First run vs steady state

**Best practices:**
1. **Look at trends**, not absolute values
2. **Focus on ±10% changes** or more
3. **Run locally** for absolute performance verification
4. **Consider CI as smoke test**, not precision measurement

### GitHub Actions Runner Jitter

Free GitHub runners have known variability:
- ±5-10% variation is **normal**
- Shared CPU with other jobs
- Network/disk latency variance
- Non-deterministic scheduling

**Mitigation:**
- Benchmark profile optimizes for consistency (`codegen-units = 1`)
- Multiple samples per benchmark (Criterion default: 100)
- Statistical analysis with outlier detection

## Benchmark Profile Optimization

The `[profile.bench]` in `Cargo.toml` is tuned for CI:

```toml
[profile.bench]
lto = "thin"           # Cross-crate optimizations without long compile times
codegen-units = 1      # Consistent codegen (no parallel variance)
opt-level = 3          # Maximum optimization
debug = true           # Debug symbols for profiling (no perf impact)
```

**Why these settings?**

| Setting | Benefit | Trade-off |
|---------|---------|-----------|
| `lto = "thin"` | Faster than `lto = "fat"`, still optimizes across crates | ~2x slower compile vs no LTO |
| `codegen-units = 1` | Consistent output, better optimization | ~3x slower compile vs default |
| `opt-level = 3` | Maximum performance | Standard for benchmarks |
| `debug = true` | Can profile with `perf`/`cargo flamegraph` | Larger binary size |

## Setting Up GitHub Pages

To enable the trend chart, you need to initialize the `gh-pages` branch **once**:

### One-Time Setup

```bash
# Create orphan branch (no history)
git checkout --orphan gh-pages
git rm -rf .

# Create index page
cat > index.html <<'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Benchmark Dashboard</title>
    <meta charset="utf-8">
</head>
<body>
    <h1>Glommio Benchmark Dashboard</h1>
    <p>Navigate to <a href="dev/bench/">dev/bench/</a> for timer benchmarks.</p>
</body>
</html>
EOF

# Commit and push
git add index.html
git commit -m "Initialize benchmark dashboard"
git push origin gh-pages

# Switch back to master
git checkout master
```

### Enable GitHub Pages

1. Go to **Settings** → **Pages**
2. Source: **Deploy from a branch**
3. Branch: **gh-pages** / **(root)**
4. Click **Save**

The dashboard will be available at `https://<username>.github.io/<repo>/` within a few minutes.

## Adding More Benchmarks

### 1. Create the Benchmark File

```rust
// glommio/benches/my_feature.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_my_feature(c: &mut Criterion) {
    c.bench_function("my_feature/baseline", |b| {
        b.iter(|| {
            // Your benchmark code
            black_box(my_function());
        });
    });
}

criterion_group!(benches, benchmark_my_feature);
criterion_main!(benches);
```

### 2. Register in Cargo.toml

```toml
[[bench]]
name = "my_feature"
harness = false
```

### 3. Add to CI Workflow

Update `.github/workflows/bench.yml` to run multiple benchmarks:

```yaml
- name: Run benchmarks
  run: |
    sudo -E env "PATH=$PATH" bash -c "ulimit -Sl 512 && ulimit -Hl 512 && \
      cargo bench --bench timer_benchmark -- --output-format bencher | tee timer_output.txt && \
      cargo bench --bench my_feature -- --output-format bencher | tee my_feature_output.txt"
```

## Advanced Usage

### Comparing Specific Commits

```bash
# Benchmark current state
cargo bench --bench timer_benchmark -- --save-baseline current

# Checkout different commit
git checkout feature-branch

# Benchmark and compare
cargo bench --bench timer_benchmark -- --baseline current
```

Criterion will show the comparison automatically.

### Flamegraphs for Profiling

```bash
cargo install flamegraph
cargo flamegraph --bench timer_benchmark -- --profile-time 10
```

Opens an interactive flamegraph showing where time is spent.

### Statistical Analysis

Criterion provides detailed statistics in `target/criterion/*/base/estimates.json`:
- **Mean**: Average time
- **Median**: Middle value (less affected by outliers)
- **Std Dev**: Consistency measure
- **MAD**: Median Absolute Deviation

### Custom Measurement Duration

For very fast benchmarks (<10ns), increase measurement time:

```rust
use criterion::{Criterion, BenchmarkId};

fn bench_fast_operation(c: &mut Criterion) {
    let mut group = c.benchmark_group("fast_ops");
    group.measurement_time(std::time::Duration::from_secs(10));

    group.bench_function("operation", |b| b.iter(|| fast_op()));
    group.finish();
}
```

## Troubleshooting

### "No benchmarks found" Error

**Cause**: Criterion didn't generate output in bencher format.

**Fix**: Ensure `--output-format bencher` is passed:
```bash
cargo bench --bench timer_benchmark -- --output-format bencher
```

### Benchmark Times Out in CI

**Cause**: io_uring benchmarks need memlock limits.

**Fix**: Ensure `ulimit` is set before running:
```bash
sudo bash -c "ulimit -Sl 512 && ulimit -Hl 512 && cargo bench"
```

### gh-pages Branch Not Found

**Cause**: Branch not initialized or workflow doesn't have write permissions.

**Fix**:
1. Initialize gh-pages branch (see setup section)
2. Ensure workflow has `contents: write` permission (default in most repos)

### "Baseline not found" in PR

**Cause**: No data on gh-pages branch yet (first run).

**Fix**: Wait for one master push to populate the baseline, then PRs will have comparison data.

### High Variance in CI Results

**Expected**: CI runners are shared resources with inherent jitter.

**If excessive (>15% variance)**:
- Increase `measurement_time` in benchmark
- Use `--sample-size` to increase samples: `cargo bench -- --sample-size 200`
- Consider self-hosted runner for consistent hardware

## Best Practices

### Writing Effective Benchmarks

1. **Use `black_box()`** to prevent compiler optimizations:
   ```rust
   b.iter(|| black_box(function(black_box(input))));
   ```

2. **Benchmark realistic scenarios**:
   - Timer insert under load (existing timers)
   - Mixed insert/remove patterns
   - Different timer counts (8, 256, 1024)

3. **Avoid setup in timing loop**:
   ```rust
   // Bad: setup in hot loop
   b.iter(|| {
       let wheel = TimingWheel::new();  // Measured!
       wheel.insert(...)
   });

   // Good: setup outside
   let wheel = TimingWheel::new();
   b.iter(|| {
       wheel.insert(...)  // Only this is measured
   });
   ```

4. **Use benchmark groups** for related tests:
   ```rust
   let mut group = c.benchmark_group("timer_operations");
   for count in [8, 256, 1024] {
       group.bench_with_input(BenchmarkId::new("insert", count), &count, |b, &n| {
           // benchmark with n timers
       });
   }
   group.finish();
   ```

### Interpreting Performance Changes

**Green flags (likely real improvement):**
- Consistent across all related benchmarks
- Explained by code changes
- Reproducible locally
- >10% improvement

**Yellow flags (investigate further):**
- Only one benchmark affected
- Opposite direction from expectation
- 5-10% change (could be noise)

**Red flags (likely CI noise):**
- Wildly inconsistent (±20%+ variance)
- No code changes in related paths
- Not reproducible locally

### When to Optimize

Use benchmarks to:
1. **Verify optimizations** actually improve performance
2. **Catch regressions** before merging
3. **Guide optimization** efforts (profile + benchmark)

Don't over-optimize:
- Micro-optimizations <5% often aren't worth code complexity
- Focus on hot paths (profiling shows where time is spent)
- Real-world performance > synthetic benchmarks

## Resources

- [Criterion.rs User Guide](https://bheisler.github.io/criterion.rs/book/)
- [github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark)
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [flamegraph](https://github.com/flamegraph-rs/flamegraph)
