#!/usr/bin/env bash
# Run the timer benchmark against every arm of the comparison, one after the
# other, on this machine and in one sitting.
#
# The arms differ only in how timers are stored. Everything measuring them --
# the benchmark, the counters, the reactor path -- lives on `master` and is
# merged into each arm, so a difference in the numbers is a difference in the
# structure. An arm that does not contain the benchmark commit is refused
# rather than measured, because a stale copy is worse than no result.
#
# Usage:
#   scripts/timer-arms.sh                 # every arm
#   scripts/timer-arms.sh a-slab-wheel    # named arms only
set -euo pipefail

cd "$(dirname "$0")/.."

ARMS_DEFAULT=(control-btreemap a-slab-wheel b-bitwheel)
if [[ $# -gt 0 ]]; then
    ARMS=("$@")
else
    ARMS=("${ARMS_DEFAULT[@]}")
fi

OUT="${TIMER_ARMS_OUT:-target/timer-arms}"
BENCH="timer"

if [[ -n "$(git status --porcelain)" ]]; then
    echo "working tree is dirty; commit or stash first" >&2
    exit 1
fi

ORIGINAL="$(git rev-parse --abbrev-ref HEAD)"
restore() {
    git checkout --quiet "${ORIGINAL}" || true
}
trap restore EXIT

# The benchmark's own commit. Every arm must contain it.
HARNESS="$(git rev-list -1 master -- "glommio/benches/${BENCH}.rs")"
if [[ -z "${HARNESS}" ]]; then
    echo "master has no ${BENCH}; nothing to run" >&2
    exit 1
fi

mkdir -p "${OUT}"
echo "harness  $(git log -1 --format='%h %s' "${HARNESS}")"
echo "machine  $(nproc) cpus, $(uname -sr)"
echo "rustc    $(rustc --version)"
echo

for arm in "${ARMS[@]}"; do
    branch="arm/${arm}"

    # On a fresh clone the arms exist only as remote-tracking refs.
    ref="${branch}"
    if ! git rev-parse --verify --quiet "${ref}" >/dev/null; then
        ref="origin/${branch}"
        if ! git rev-parse --verify --quiet "${ref}" >/dev/null; then
            echo "${arm}: no ${branch} locally or on origin, skipping" >&2
            continue
        fi
    fi

    if ! git merge-base --is-ancestor "${HARNESS}" "${ref}"; then
        echo "${arm}: does not contain the harness commit." >&2
        echo "        git checkout ${branch} && git merge master" >&2
        continue
    fi

    git checkout --quiet --detach "${ref}"
    commit="$(git rev-parse --short HEAD)"

    if ! cargo build --benches >/dev/null 2>&1; then
        echo "${arm}: does not build, skipping" >&2
        cargo build --benches 2>&1 | tail -20 >&2
        continue
    fi

    echo "== ${arm} (${commit})"
    cargo bench --bench "${BENCH}" -- \
        --warm-up-time "${TIMER_ARMS_WARMUP:-1}" \
        --measurement-time "${TIMER_ARMS_TIME:-2}" \
        --sample-size "${TIMER_ARMS_SAMPLES:-10}" 2>&1 |
        grep -E "^timer/" | tee "${OUT}/${arm}.txt"
    echo
done

echo "results in ${OUT}/"
