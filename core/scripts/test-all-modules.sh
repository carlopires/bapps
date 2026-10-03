#!/bin/bash
# Run all Glommio tests by module to avoid OOM
# Each module runs in isolation with full memory cleanup between runs

set -e

# Source cargo environment if needed
if ! command -v cargo &> /dev/null; then
    if [ -f "$HOME/.cargo/env" ]; then
        source "$HOME/.cargo/env"
    fi
fi

echo "🧪 Running Glommio Complete Test Suite (Modular)"
echo "=================================================="
echo ""

# Define test modules
MODULES=(
  "channels::channel_mesh"
  "channels::local_channel"
  "channels::sharding"
  "channels::shared_channel"
  "controllers"
  "error"
  "executor::latch"
  "executor::placement"
  "executor::stall"
  "executor::test"
  "io::dma_file"
  "io::directory"
  "io::buffered_file"
  "net::tcp_socket"
  "net::udp_socket"
  "task"
  "task::raw"
  "task::state"
  "task::waker"
  "timer"
)

PASSED=0
FAILED=0
TOTAL=${#MODULES[@]}
FAILED_MODULES=()
START_TIME=$(date +%s)

for module in "${MODULES[@]}"; do
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "Testing: $module"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Run test and capture output, check PIPESTATUS for cargo exit code
  cargo test --lib "$module" -- --test-threads=2 2>&1 | tee /tmp/test-$module.log | tail -3
  TEST_EXIT_CODE=${PIPESTATUS[0]}

  if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✅ $module passed"
    PASSED=$((PASSED + 1))
  else
    echo "❌ $module FAILED (exit code: $TEST_EXIT_CODE)"
    FAILED=$((FAILED + 1))
    FAILED_MODULES+=("$module")
    echo "   Log saved: /tmp/test-$module.log"
  fi
  echo ""
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "=================================================="
echo "📊 Test Suite Summary"
echo "=================================================="
echo "Total modules: $TOTAL"
echo "Passed: $PASSED ✅"
echo "Failed: $FAILED ❌"
echo "Duration: ${DURATION}s"
echo ""

if [ $FAILED -gt 0 ]; then
  echo "❌ Failed modules:"
  for failed in "${FAILED_MODULES[@]}"; do
    echo "   - $failed"
  done
  echo ""
  echo "View logs:"
  for failed in "${FAILED_MODULES[@]}"; do
    echo "   cat /tmp/test-$failed.log"
  done
else
  echo "✅ All tests passed!"
fi
echo "=================================================="

exit $FAILED
