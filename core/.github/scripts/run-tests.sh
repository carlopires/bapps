#!/usr/bin/env bash
# Runs the test suite the same way a developer does: `make test-nextest`.
#
# Deliberately the same entry point as local development. When this script and
# the Makefile drifted apart, CI checked things no local target ran, and stayed
# red for a fortnight while every local run was green.
set -euo pipefail

target="${1:-}"

# nextest is not in the toolchain; install it if the runner has not cached it.
if ! command -v cargo-nextest >/dev/null 2>&1; then
    echo "installing cargo-nextest"
    curl -LsSf https://get.nexte.st/latest/linux \
        | tar zxf - -C "${CARGO_HOME:-$HOME/.cargo}/bin"
fi

sudo -E \
    PATH="${PATH}:/usr/share/rust/.cargo/bin" \
    TEST_TARGET="${target}" \
    bash -c '
        set -euo pipefail

        # glommio registers io_uring buffers, which are locked pages.
        ulimit -Sl 512
        ulimit -Hl 512

        echo "$(nproc) CPU(s) available"
        rustup show

        exec make test-nextest TARGET="${TEST_TARGET}" NEXTEST_PROFILE=ci
    '
