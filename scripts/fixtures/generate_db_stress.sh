#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
scripts/fixtures/generate_db_stress.sh

Generates a RocksDB database directory using the RocksDB db_stress tool.

This script is for producing oracle databases for compatibility/corpus-style validation.
It does NOT write into repository testdata by default.

Usage:
  scripts/fixtures/generate_db_stress.sh --out-dir <OUT_DIR> [--seconds <N>] [--seed <N>] [--force]

Required:
  - ROCKSDB_PATH: path to a RocksDB checkout/build that contains db_stress.

Examples:
  export ROCKSDB_PATH="$HOME/Workspace/rocksdb"
  scripts/fixtures/generate_db_stress.sh --out-dir tmp/redteam/fixtures/db_stress_run1 --seconds 20 --seed 12345
EOF
}

out_dir=""
seconds="20"
seed="12345"
force=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --out-dir) out_dir="${2:-}"; shift 2 ;;
    --seconds) seconds="${2:-}"; shift 2 ;;
    --seed) seed="${2:-}"; shift 2 ;;
    --force) force=1; shift ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$out_dir" ]]; then
  echo "missing --out-dir" >&2
  usage >&2
  exit 2
fi

if [[ -z "${ROCKSDB_PATH:-}" ]]; then
  echo "missing ROCKSDB_PATH" >&2
  exit 2
fi

DB_STRESS="$ROCKSDB_PATH/db_stress"
if [[ ! -x "$DB_STRESS" ]]; then
  echo "missing or non-executable: $DB_STRESS" >&2
  echo "build db_stress in your RocksDB checkout, or point ROCKSDB_PATH to a build that includes it" >&2
  exit 2
fi

mkdir -p "$out_dir"

if [[ "$force" -eq 0 ]]; then
  if [[ -n "$(ls -A "$out_dir" 2>/dev/null || true)" ]]; then
    echo "refusing to use non-empty out-dir without --force: $out_dir" >&2
    exit 2
  fi
fi

# Safety: never allow out-dir to be a high-level path.
case "$out_dir" in
  "/"|"/tmp"|"$HOME"|"$HOME/"|".")
    echo "refusing unsafe out-dir: $out_dir" >&2
    exit 2
    ;;
esac

echo "rocksdb: db_stress=$DB_STRESS"
echo "rocksdb: out-dir=$out_dir"
echo "rocksdb: seconds=$seconds seed=$seed"

# db_stress writes the DB in-place under --db.
# Keep args conservative: avoid exotic fs options, but exercise common paths.
"$DB_STRESS" \
  --db="$out_dir" \
  --create_if_missing=1 \
  --duration="$seconds" \
  --seed="$seed" \
  --write_buffer_size=1048576 \
  --max_write_buffer_number=4 \
  --target_file_size_base=1048576 \
  --compression_type=snappy \
  --verify_checksum=1 \
  --destroy_db=0 \
  --disable_auto_compactions=0 \
  --reopen=50 \
  --ops_per_thread=0 \
  --threads=8

echo "rocksdb: done"


