#!/usr/bin/env bash
set -euo pipefail

# Run all campaignrunner groups for one or more tiers.
#
# Convenience entrypoint that:
# - runs every group returned by `bin/campaignrunner -list-groups`
# - runs for both tiers by default (quick + nightly)
# - persists all artifacts under a single timestamped run root
# - keeps going even if some groups fail, and exits non-zero at the end if any failed
#
# Requirements:
# - `bin/campaignrunner` exists (build first if needed)
# - Oracle should be configured (ROCKSDB_PATH) to run oracle-required instances

usage() {
  cat <<'USAGE'
Usage:
  scripts/campaigns/run_all.sh [--tier quick|nightly|all] [--run-root <DIR>] [--fail-fast]

Options:
  --tier       Which tier(s) to run. Default: all (quick + nightly).
  --run-root   Base directory for run roots. Default: tmp/campaign-runs/run-all
  --fail-fast  Stop immediately on first failing group (default: keep running all groups)

Environment:
  ROCKSDB_PATH   Required for oracle-gated instances.
USAGE
}

TIER="all"
RUN_ROOT_BASE="tmp/campaign-runs/run-all"
FAIL_FAST=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tier)
      TIER="${2:-}"
      shift 2
      ;;
    --run-root)
      RUN_ROOT_BASE="${2:-}"
      shift 2
      ;;
    --fail-fast)
      FAIL_FAST=1
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

if [[ -z "${ROCKSDB_PATH:-}" ]]; then
  echo "error: ROCKSDB_PATH is not set" >&2
  echo "Set it to your RocksDB checkout/build directory so oracle-gated instances can run." >&2
  echo "Example: export ROCKSDB_PATH=\"/path/to/rocksdb\"" >&2
  exit 2
fi

if [[ ! -x "./bin/campaignrunner" ]]; then
  echo "error: ./bin/campaignrunner not found or not executable" >&2
  echo "Build first (example): make build" >&2
  exit 2
fi

timestamp="$(date +%Y-%m-%d_%H%M%S)"
root="${RUN_ROOT_BASE%/}/$timestamp"
mkdir -p "$root"

echo "campaigns: run root: $root"
echo "campaigns: ROCKSDB_PATH=$ROCKSDB_PATH"

tiers=()
case "$TIER" in
  quick) tiers=("quick") ;;
  nightly) tiers=("nightly") ;;
  all) tiers=("quick" "nightly") ;;
  *)
    echo "error: invalid --tier: $TIER (expected: quick|nightly|all)" >&2
    exit 2
    ;;
esac

groups=()
while IFS= read -r line; do
  # Expected format:
  #   Available groups:
  #     stress
  #     crash
  # ...
  g="$(echo "$line" | awk '{print $1}')"
  if [[ -n "$g" && "$g" != "Available" && "$g" != "groups:" ]]; then
    groups+=("$g")
  fi
done < <(./bin/campaignrunner -list-groups 2>/dev/null | sed 's/^ *//')

if [[ ${#groups[@]} -eq 0 ]]; then
  echo "error: no groups found from ./bin/campaignrunner -list-groups" >&2
  exit 2
fi

failed=0
for tier in "${tiers[@]}"; do
  for group in "${groups[@]}"; do
    safe_group="${group//[^a-zA-Z0-9._-]/_}"
    run_root="$root/$tier/$safe_group"
    mkdir -p "$run_root"

    echo ""
    echo "=== campaigns: tier=$tier group=$group ==="
    echo "run_root=$run_root"

    set +e
    ./bin/campaignrunner \
      -tier="$tier" \
      -group="$group" \
      -run-root "$run_root" \
      -v \
      2>&1 | tee "$run_root/runner.log"
    rc="${PIPESTATUS[0]}"
    set -e

    if [[ $rc -ne 0 ]]; then
      echo "campaigns: group failed: tier=$tier group=$group exit=$rc" >&2
      failed=1
      if [[ $FAIL_FAST -eq 1 ]]; then
        exit "$rc"
      fi
    fi
  done
done

if [[ $failed -ne 0 ]]; then
  echo ""
  echo "campaigns: DONE with failures (see $root/**/summary.json and runner.log)" >&2
  exit 1
fi

echo ""
echo "campaigns: DONE (all groups passed)"


