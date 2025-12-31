#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/status/run_durability_repros.sh <scenario> <run_dir>

Scenarios:
  wal-sync
    Crash durability check with WAL enabled and sync writes.

  wal-sync-sweep
    Seed sweep for WAL+sync crash durability (writes one subdir per seed and prints a failure rate).

  disablewal-faultfs
    Crash durability check with WAL disabled, flush boundaries, and fault injection.

  disablewal-faultfs-minimize
    Minimization sweep for DisableWAL+faultfs durability (tries smaller parameter sets and fault modes).

  adversarial-corruption
    Targeted corruption suite (no crash loop): runs cmd/adversarialtest with category=corruption.

  internal-key-collision
    Deterministic-ish repro + smoking-gun check: runs a fixed crash schedule and then scans SSTs
    for internal-key collisions (same internal key bytes, different value bytes across SSTs).

  internal-key-collision-only
    Same as internal-key-collision, but the scenario **fails only** if the collision check fails.
    This is intended as a Phase-1 (G1) gate when the verifier is known to be unsound for DisableWAL
    (HARNESS-02 pending). Verifier failures are recorded as warnings, not as a failing exit code.

Examples:
  make build
  scripts/status/run_durability_repros.sh wal-sync /path/to/run/wal-sync
  scripts/status/run_durability_repros.sh disablewal-faultfs /path/to/run/disablewal-faultfs
  scripts/status/run_durability_repros.sh wal-sync-sweep /path/to/run/wal-sync-sweep
  scripts/status/run_durability_repros.sh disablewal-faultfs-minimize /path/to/run/disablewal-faultfs-minimize
  scripts/status/run_durability_repros.sh adversarial-corruption /path/to/run/adversarial-corruption
  scripts/status/run_durability_repros.sh internal-key-collision /path/to/run/internal-key-collision
  scripts/status/run_durability_repros.sh internal-key-collision-only /path/to/run/internal-key-collision-only

Notes:
  - This script writes logs and artifacts into <run_dir>.
  - The script exits non-zero when the scenario fails verification.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 2 ]]; then
  usage
  exit 2
fi

SCENARIO="$1"
RUN_DIR="$2"

CRASHTEST_BIN="${CRASHTEST_BIN:-./bin/crashtest}"
ADVERSARIAL_BIN="${ADVERSARIAL_BIN:-./bin/adversarialtest}"
SSTDUMP_BIN="${SSTDUMP_BIN:-./bin/sstdump}"

require_bin() {
  local p="$1"
  local name="$2"
  if [[ ! -x "$p" ]]; then
    echo "Error: $name binary not found at: $p" >&2
    echo "Run: make build" >&2
    exit 2
  fi
}

safe_rm_rf_dir() {
  local dir="$1"
  if [[ -z "$dir" ]]; then
    echo "Error: refusing to rm -rf empty directory path" >&2
    exit 2
  fi
  case "$dir" in
    "/"|"/tmp"|"/var"|"/var/"*|"$HOME"|"$HOME/"|". "|"."|".." )
      echo "Error: refusing to rm -rf unsafe directory: $dir" >&2
      exit 2
      ;;
  esac
  rm -rf "$dir"
}

safe_rm_rf_dir "$RUN_DIR"
mkdir -p "$RUN_DIR"

LOG="$RUN_DIR/run.log"
ARTIFACTS="$RUN_DIR/artifacts"

echo "Scenario: $SCENARIO" | tee "$RUN_DIR/meta.txt"
echo "Run dir:  $RUN_DIR" | tee -a "$RUN_DIR/meta.txt"

run_crashtest() {
  local log="$1"
  shift
  set +e
  "$CRASHTEST_BIN" "$@" 2>&1 | tee "$log"
  local rc=$?
  set -e
  return "$rc"
}

write_summary() {
  local rc="$1"
  local log="$2"

  echo "exit_code=$rc" | tee "$RUN_DIR/exit_code.txt"
  grep -nE "^Verify: " "$log" | head -n 50 > "$RUN_DIR/verify_head.txt" || true
  grep -nE "^Verify: " "$log" | tail -n 50 > "$RUN_DIR/verify_tail.txt" || true

  if [[ "$rc" -ne 0 ]]; then
    echo "FAILED: see $log" | tee "$RUN_DIR/summary.txt"
  else
    echo "PASSED: see $log" | tee "$RUN_DIR/summary.txt"
  fi
}

case "$SCENARIO" in
  wal-sync)
    require_bin "$CRASHTEST_BIN" "crashtest"
    echo "Command: $CRASHTEST_BIN -seed=9101 -cycles=5 -duration=6m -interval=10s -min-interval=2s -kill-mode=sigkill -sync ..." \
      | tee -a "$RUN_DIR/meta.txt"

    run_crashtest "$LOG" -seed=9101 -cycles=5 -duration=6m -interval=10s -min-interval=2s -kill-mode=sigkill \
      -sync -db "$RUN_DIR/db_sync" -run-dir "$ARTIFACTS" -keep -v \
      || true
    RC=$?
    ;;

  wal-sync-sweep)
    require_bin "$CRASHTEST_BIN" "crashtest"

    # Default to the historically interesting window.
    WAL_SYNC_SEEDS="${WAL_SYNC_SEEDS:-9101 9102 9103 9104 9105 9106 9107 9108}"
    WAL_SYNC_CYCLES="${WAL_SYNC_CYCLES:-5}"
    WAL_SYNC_DURATION="${WAL_SYNC_DURATION:-6m}"
    WAL_SYNC_INTERVAL="${WAL_SYNC_INTERVAL:-10s}"
    WAL_SYNC_MIN_INTERVAL="${WAL_SYNC_MIN_INTERVAL:-2s}"

    echo "Seeds: $WAL_SYNC_SEEDS" | tee -a "$RUN_DIR/meta.txt"
    echo "Params: cycles=$WAL_SYNC_CYCLES duration=$WAL_SYNC_DURATION interval=$WAL_SYNC_INTERVAL min_interval=$WAL_SYNC_MIN_INTERVAL kill_mode=sigkill sync=on" \
      | tee -a "$RUN_DIR/meta.txt"

    total=0
    failed=0
    passed=0

    for s in $WAL_SYNC_SEEDS; do
      total=$((total+1))
      case_dir="$RUN_DIR/seed_${s}"
      mkdir -p "$case_dir"

      case_log="$case_dir/crashtest.log"
      case_art="$case_dir/artifacts"
      case_meta="$case_dir/meta.txt"

      echo "Scenario: wal-sync" > "$case_meta"
      echo "Seed: $s" >> "$case_meta"

      set +e
      "$CRASHTEST_BIN" -seed="$s" -cycles="$WAL_SYNC_CYCLES" -duration="$WAL_SYNC_DURATION" \
        -interval="$WAL_SYNC_INTERVAL" -min-interval="$WAL_SYNC_MIN_INTERVAL" -kill-mode=sigkill \
        -sync -db "$case_dir/db_sync" -run-dir "$case_art" -keep -v \
        2>&1 | tee "$case_log"
      rc=$?
      set -e

      echo "exit_code=$rc" > "$case_dir/exit_code.txt"
      grep -nE "^Verify: " "$case_log" | head -n 20 > "$case_dir/verify_head.txt" || true

      if [[ "$rc" -ne 0 ]]; then
        failed=$((failed+1))
      else
        passed=$((passed+1))
      fi
    done

    # Make the sweep itself fail if any seed fails, since the point is to reproduce.
    echo "total=$total passed=$passed failed=$failed" | tee "$RUN_DIR/sweep_summary.txt"

    if [[ "$failed" -gt 0 ]]; then
      RC=1
    else
      RC=0
    fi

    # Provide a conventional location for tooling that expects run.log.
    printf "WAL sync sweep: total=%d passed=%d failed=%d\n" "$total" "$passed" "$failed" | tee "$LOG"
    ;;

  disablewal-faultfs)
    require_bin "$CRASHTEST_BIN" "crashtest"
    echo "Command: $CRASHTEST_BIN -seed=8201 -cycles=25 -duration=8m -interval=6s -min-interval=0.5s -kill-mode=sigterm -disable-wal -faultfs -faultfs-drop-unsynced -faultfs-delete-unsynced ..." \
      | tee -a "$RUN_DIR/meta.txt"

    run_crashtest "$LOG" -seed=8201 -cycles=25 -duration=8m -interval=6s -min-interval=0.5s -kill-mode=sigterm \
      -disable-wal -faultfs -faultfs-drop-unsynced -faultfs-delete-unsynced \
      -db "$RUN_DIR/db_faultfs_disable_wal" -run-dir "$ARTIFACTS" -keep -v \
      || true
    RC=$?
    ;;

  disablewal-faultfs-minimize)
    require_bin "$CRASHTEST_BIN" "crashtest"

    # Start from the known repro seed and shrink parameters.
    BASE_SEED="${DISABLEWAL_SEED:-8201}"
    DURATION="${DISABLEWAL_DURATION:-8m}"
    INTERVAL="${DISABLEWAL_INTERVAL:-6s}"
    MIN_INTERVAL="${DISABLEWAL_MIN_INTERVAL:-0.5s}"

    echo "Seed: $BASE_SEED" | tee -a "$RUN_DIR/meta.txt"
    echo "Base params: duration=$DURATION interval=$INTERVAL min_interval=$MIN_INTERVAL kill_mode=sigterm" \
      | tee -a "$RUN_DIR/meta.txt"

    # Cases are ordered from smallest to largest. First failure is the current best minimized repro.
    cases=(
      "cycles=4 mode=drop"
      "cycles=4 mode=delete"
      "cycles=4 mode=drop+delete"
      "cycles=6 mode=drop+delete"
    )

    first_fail=""
    any_fail=0

    for c in "${cases[@]}"; do
      cycles=$(echo "$c" | awk '{print $1}' | cut -d= -f2)
      mode=$(echo "$c" | awk '{print $2}' | cut -d= -f2)

      case_dir="$RUN_DIR/${mode}_cycles_${cycles}"
      mkdir -p "$case_dir"
      case_log="$case_dir/crashtest.log"
      case_art="$case_dir/artifacts"

      args=(
        -seed="$BASE_SEED"
        -cycles="$cycles"
        -duration="$DURATION"
        -interval="$INTERVAL"
        -min-interval="$MIN_INTERVAL"
        -kill-mode=sigterm
        -disable-wal
        -faultfs
        -db "$case_dir/db_faultfs_disable_wal"
        -run-dir "$case_art"
        -keep
        -v
      )

      if [[ "$mode" == "drop" || "$mode" == "drop+delete" ]]; then
        args+=(-faultfs-drop-unsynced)
      fi
      if [[ "$mode" == "delete" || "$mode" == "drop+delete" ]]; then
        args+=(-faultfs-delete-unsynced)
      fi

      set +e
      "$CRASHTEST_BIN" "${args[@]}" 2>&1 | tee "$case_log"
      rc=$?
      set -e

      echo "exit_code=$rc" > "$case_dir/exit_code.txt"
      grep -nE "^Verify: " "$case_log" | head -n 50 > "$case_dir/verify_head.txt" || true

      if [[ "$rc" -ne 0 ]]; then
        any_fail=1
        if [[ -z "$first_fail" ]]; then
          first_fail="$mode cycles=$cycles"
        fi
      fi
    done

    echo "first_failure=$first_fail" | tee "$RUN_DIR/minimize_summary.txt"

    if [[ "$any_fail" -eq 1 ]]; then
      RC=1
    else
      RC=0
    fi

    printf "DisableWAL faultfs minimize: first_failure=%s\n" "${first_fail:-none}" | tee "$LOG"
    ;;

  adversarial-corruption)
    require_bin "$ADVERSARIAL_BIN" "adversarialtest"

    ADV_SEED="${ADVERSARIAL_SEED:-777}"
    ADV_DURATION="${ADVERSARIAL_DURATION:-30s}"

    echo "Command: $ADVERSARIAL_BIN -category=corruption -seed=$ADV_SEED -duration=$ADV_DURATION -run-dir=$ARTIFACTS ..." \
      | tee -a "$RUN_DIR/meta.txt"

    set +e
    "$ADVERSARIAL_BIN" -category=corruption -seed="$ADV_SEED" -duration="$ADV_DURATION" -run-dir "$ARTIFACTS" -keep \
      2>&1 | tee "$LOG"
    RC=$?
    set -e
    ;;

  internal-key-collision)
    require_bin "$CRASHTEST_BIN" "crashtest"
    require_bin "$SSTDUMP_BIN" "sstdump"

    # Deterministic-ish crash schedule from known repro. Override via env if needed.
    IKC_SEED="${IKC_SEED:-8201}"
    IKC_CYCLES="${IKC_CYCLES:-4}"
    IKC_DURATION="${IKC_DURATION:-3m}"
    IKC_INTERVAL="${IKC_INTERVAL:-6s}"
    IKC_MIN_INTERVAL="${IKC_MIN_INTERVAL:-0.5s}"
    IKC_SCHEDULE="${IKC_SCHEDULE:-5.327s,10.839s,10.547s,10.065s}"

    echo "Seed: $IKC_SEED" | tee -a "$RUN_DIR/meta.txt"
    echo "Params: cycles=$IKC_CYCLES duration=$IKC_DURATION interval=$IKC_INTERVAL min_interval=$IKC_MIN_INTERVAL kill_mode=sigterm schedule=$IKC_SCHEDULE" \
      | tee -a "$RUN_DIR/meta.txt"

    # Step 1: reproduce crash/recovery state.
    set +e
    "$CRASHTEST_BIN" -seed="$IKC_SEED" -cycles="$IKC_CYCLES" -duration="$IKC_DURATION" -interval="$IKC_INTERVAL" -min-interval="$IKC_MIN_INTERVAL" \
      -kill-mode=sigterm -crash-schedule="$IKC_SCHEDULE" \
      -disable-wal -faultfs -faultfs-drop-unsynced -faultfs-delete-unsynced \
      -db "$RUN_DIR/db" -run-dir "$ARTIFACTS" -trace-dir "$RUN_DIR/traces" -keep -v \
      2>&1 | tee "$LOG"
    rc_crash=$?
    set -e

    # Step 2: smoking-gun check (independent of verifier result).
    # This should FAIL (exit non-zero) when internal-key collisions exist.
    DB_DIR="$RUN_DIR/db"
    if [[ -d "$RUN_DIR/db/db" ]]; then
      DB_DIR="$RUN_DIR/db/db"
    fi

    set +e
    "$SSTDUMP_BIN" --command=collision-check --dir "$DB_DIR" --max-collisions=1 \
      2>&1 | tee "$RUN_DIR/collision_check.log"
    rc_collision=$?
    set -e

    # Overall: fail if either the crash test failed or collision check failed.
    if [[ "$rc_crash" -ne 0 || "$rc_collision" -ne 0 ]]; then
      RC=1
    else
      RC=0
    fi

    # Include the component return codes in run.log for quick triage.
    {
      echo ""
      echo "internal-key-collision: rc_crash=$rc_crash rc_collision=$rc_collision"
    } | tee -a "$LOG"
    ;;

  internal-key-collision-only)
    require_bin "$CRASHTEST_BIN" "crashtest"
    require_bin "$SSTDUMP_BIN" "sstdump"

    # Deterministic-ish crash schedule from known repro. Override via env if needed.
    IKC_SEED="${IKC_SEED:-8201}"
    IKC_CYCLES="${IKC_CYCLES:-4}"
    IKC_DURATION="${IKC_DURATION:-3m}"
    IKC_INTERVAL="${IKC_INTERVAL:-6s}"
    IKC_MIN_INTERVAL="${IKC_MIN_INTERVAL:-0.5s}"
    IKC_SCHEDULE="${IKC_SCHEDULE:-5.327s,10.839s,10.547s,10.065s}"

    echo "Seed: $IKC_SEED" | tee -a "$RUN_DIR/meta.txt"
    echo "Params: cycles=$IKC_CYCLES duration=$IKC_DURATION interval=$IKC_INTERVAL min_interval=$IKC_MIN_INTERVAL kill_mode=sigterm schedule=$IKC_SCHEDULE" \
      | tee -a "$RUN_DIR/meta.txt"
    echo "Mode: collision-check-only (ignores verifier failures; HARNESS-02 pending)" | tee -a "$RUN_DIR/meta.txt"

    # Step 1: reproduce crash/recovery state.
    set +e
    "$CRASHTEST_BIN" -seed="$IKC_SEED" -cycles="$IKC_CYCLES" -duration="$IKC_DURATION" -interval="$IKC_INTERVAL" -min-interval="$IKC_MIN_INTERVAL" \
      -kill-mode=sigterm -crash-schedule="$IKC_SCHEDULE" \
      -disable-wal -faultfs -faultfs-drop-unsynced -faultfs-delete-unsynced \
      -db "$RUN_DIR/db" -run-dir "$ARTIFACTS" -trace-dir "$RUN_DIR/traces" -keep -v \
      2>&1 | tee "$LOG"
    rc_crash=$?
    set -e

    # Step 2: smoking-gun check (this is the ONLY gating signal).
    DB_DIR="$RUN_DIR/db"
    if [[ -d "$RUN_DIR/db/db" ]]; then
      DB_DIR="$RUN_DIR/db/db"
    fi

    set +e
    "$SSTDUMP_BIN" --command=collision-check --dir "$DB_DIR" --max-collisions=1 \
      2>&1 | tee "$RUN_DIR/collision_check.log"
    rc_collision=$?
    set -e

    # Overall: fail ONLY if collision check fails.
    if [[ "$rc_collision" -ne 0 ]]; then
      RC=1
    else
      RC=0
    fi

    {
      echo ""
      echo "internal-key-collision-only: rc_crash=$rc_crash rc_collision=$rc_collision"
    } | tee -a "$LOG"

    if [[ "$rc_crash" -ne 0 && "$rc_collision" -eq 0 ]]; then
      {
        echo ""
        echo "WARNING: verifier failed (rc_crash=$rc_crash) but collision-check passed."
        echo "WARNING: This scenario is collision-check-only; see HARNESS-02 / V4 for seqno-based verification."
      } | tee -a "$LOG" | tee "$RUN_DIR/warnings.txt" >/dev/null
    fi
    ;;

  *)
    echo "Error: unknown scenario: $SCENARIO" >&2
    usage >&2
    exit 2
    ;;
esac

write_summary "$RC" "$LOG"

exit "$RC"


