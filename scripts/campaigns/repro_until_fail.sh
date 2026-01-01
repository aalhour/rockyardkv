#!/usr/bin/env bash
set -euo pipefail

# Rerun a targeted campaignrunner selection until it fails (or until max tries is reached).
#
# Use this to turn flaky/non-deterministic failures into a captured failing run directory
# containing output.log + trace + replay.sh.
#
# Requirements:
# - `bin/campaignrunner` exists
# - Oracle configured via ROCKSDB_PATH (recommended)
#
# Example:
#   export ROCKSDB_PATH=/path/to/rocksdb
#   scripts/campaigns/repro_until_fail.sh --tier quick --group stress --filter 'group=stress.write' --max-tries 50

usage() {
  cat <<'USAGE'
Usage:
  scripts/campaigns/repro_until_fail.sh \
    --tier quick|nightly \
    --group <GROUP> \
    [--filter <FILTER>] \
    [--run-root-base <DIR>] \
    [--max-tries N] \
    [--trace-max-size BYTES] \
    [--minimize] \
    [--sleep-seconds N]

Defaults:
  --run-root-base   tmp/campaign-runs/repros
  --max-tries       25
  --trace-max-size  67108864   (64 MiB)
  --sleep-seconds   0

Notes:
  - Always enables trace capture (so failures are replayable).
  - Exits 0 if no failure is observed within max tries.
  - Exits non-zero as soon as a failure is observed, printing the failing run root.
USAGE
}

TIER=""
GROUP=""
FILTER=""
RUN_ROOT_BASE="tmp/campaign-runs/repros"
MAX_TRIES=25
TRACE_MAX_SIZE=$((64 * 1024 * 1024))
ENABLE_MINIMIZE=0
SLEEP_SECONDS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier) TIER="${2:-}"; shift 2 ;;
    --group) GROUP="${2:-}"; shift 2 ;;
    --filter) FILTER="${2:-}"; shift 2 ;;
    --run-root-base) RUN_ROOT_BASE="${2:-}"; shift 2 ;;
    --max-tries) MAX_TRIES="${2:-}"; shift 2 ;;
    --trace-max-size) TRACE_MAX_SIZE="${2:-}"; shift 2 ;;
    --minimize) ENABLE_MINIMIZE=1; shift 1 ;;
    --sleep-seconds) SLEEP_SECONDS="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$TIER" || -z "$GROUP" ]]; then
  echo "error: --tier and --group are required" >&2
  usage >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -x "./bin/campaignrunner" ]]; then
  echo "error: ./bin/campaignrunner not found or not executable" >&2
  exit 2
fi

if [[ -z "${ROCKSDB_PATH:-}" ]]; then
  echo "error: ROCKSDB_PATH is not set" >&2
  echo "Set it to your RocksDB checkout/build directory so oracle-gated instances can run." >&2
  exit 2
fi

timestamp="$(date +%Y-%m-%d_%H%M%S)"
safe_group="${GROUP//[^a-zA-Z0-9._-]/_}"
root="${RUN_ROOT_BASE%/}/$timestamp/$TIER/$safe_group"
mkdir -p "$root"

echo "campaigns: repro loop root: $root"
echo "campaigns: tier=$TIER group=$GROUP filter=${FILTER:-<none>} max_tries=$MAX_TRIES minimize=$ENABLE_MINIMIZE"

for ((i=1; i<=MAX_TRIES; i++)); do
  attempt="$(printf "%03d" "$i")"
  run_root="$root/attempt_$attempt"
  mkdir -p "$run_root"

  echo ""
  echo "=== campaigns: attempt $attempt/$MAX_TRIES ==="
  echo "run_root=$run_root"

  args=(
    -tier="$TIER"
    -group="$GROUP"
    -run-root "$run_root"
    -capture-trace
    -trace-max-size "$TRACE_MAX_SIZE"
    -v
  )
  if [[ -n "$FILTER" ]]; then
    args+=(-filter "$FILTER")
  fi
  if [[ $ENABLE_MINIMIZE -eq 1 ]]; then
    args+=(-minimize)
  fi

  set +e
  ./bin/campaignrunner "${args[@]}" 2>&1 | tee "$run_root/runner.log"
  rc="${PIPESTATUS[0]}"
  set -e

  if [[ $rc -ne 0 ]]; then
    echo ""
    echo "campaigns: FAILURE CAPTURED"
    echo "failing_run_root=$run_root"
    exit "$rc"
  fi

  if [[ "$SLEEP_SECONDS" != "0" ]]; then
    sleep "$SLEEP_SECONDS"
  fi
done

echo ""
echo "campaigns: no failures observed within max tries"
echo "root=$root"


