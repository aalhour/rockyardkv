#!/usr/bin/env bash
set -euo pipefail

# Validate repository test fixtures under testdata/ and cmd/*/testdata/.
#
# This script is intended to catch:
# - missing/renamed fixture files referenced by tests
# - fixture directories that were partially copied
# - accidental inclusion of machine-local junk files
#
# It also runs a small set of Go tests that consume these fixtures to validate
# basic correctness without requiring the C++ oracle.
#
# Usage:
#   scripts/check-testdata.sh
#
# Optional:
#   scripts/check-testdata.sh --with-oracle
#
# --with-oracle runs the full goldentest suite. It requires the RocksDB oracle
# tools (`ldb`, `sst_dump`) to be configured via ROCKSDB_PATH.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

WITH_ORACLE=0
if [[ "${1:-}" == "--with-oracle" ]]; then
  WITH_ORACLE=1
  shift
fi

if [[ $# -ne 0 ]]; then
  echo "error: unknown arguments: $*" >&2
  echo "usage: scripts/check-testdata.sh [--with-oracle]" >&2
  exit 2
fi

require_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "error: missing file: ${path#$ROOT_DIR/}" >&2
    exit 1
  fi
}

require_dir() {
  local path="$1"
  if [[ ! -d "$path" ]]; then
    echo "error: missing directory: ${path#$ROOT_DIR/}" >&2
    exit 1
  fi
}

echo "[check-testdata] repo root: ${ROOT_DIR}"

# -----------------------------------------------------------------------------
# 1) Basic fixture shape checks (fast, no tooling)
# -----------------------------------------------------------------------------

require_dir "$ROOT_DIR/testdata"
require_dir "$ROOT_DIR/testdata/rocksdb/v10.7.5"

require_dir "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/CURRENT"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/options.txt"

require_dir "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/wal"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/wal/simple.log"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/wal/multi.log"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/wal/fragmented.log"

require_dir "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/manifest"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/manifest/simple.manifest"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/manifest/newfile.manifest"

require_dir "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/sst"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/formats/sst/simple.sst"

require_dir "$ROOT_DIR/testdata/rocksdb/v10.7.5/sst_samples"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/sst_samples/000008.sst"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/sst_samples/000013.sst"

require_dir "$ROOT_DIR/testdata/rocksdb/v10.7.5/db_samples/simple_db"
require_file "$ROOT_DIR/testdata/rocksdb/v10.7.5/db_samples/simple_db/CURRENT"

manifest_count="$(ls "$ROOT_DIR/testdata/rocksdb/v10.7.5/db_samples/simple_db"/MANIFEST-* 2>/dev/null | wc -l | tr -d ' ')"
if [[ "$manifest_count" -lt 1 ]]; then
  echo "error: expected at least one MANIFEST-* under testdata/rocksdb/v10.7.5/db_samples/simple_db" >&2
  exit 1
fi

sst_count="$(ls "$ROOT_DIR/testdata/rocksdb/v10.7.5/db_samples/simple_db"/*.sst 2>/dev/null | wc -l | tr -d ' ')"
if [[ "$sst_count" -lt 1 ]]; then
  echo "error: expected at least one *.sst under testdata/rocksdb/v10.7.5/db_samples/simple_db" >&2
  exit 1
fi

# -----------------------------------------------------------------------------
# 2) Go correctness checks (fixture consumers)
# -----------------------------------------------------------------------------

echo "[check-testdata] running Go fixture consumer tests..."

(
  cd "$ROOT_DIR"

  # Contract: Go can read the small C++-generated SST files in testdata/.
  go test ./internal/table -run 'TestReadCppRocksDBSST($|/)' -count=1

  # Contract: Go can read the C++-generated fixtures used by goldentest without
  # requiring oracle binaries.
  go test ./cmd/goldentest -run 'TestCppWritesGoReads_Fixtures|TestManifest_Contract_CppWritesGoReads|TestWAL_CppWritesGoReads|TestDatabase_Contract_CppWritesGoReads' -count=1
)

# -----------------------------------------------------------------------------
# 3) Optional: oracle-enabled verification
# -----------------------------------------------------------------------------

if [[ "$WITH_ORACLE" -eq 1 ]]; then
  if [[ -z "${ROCKSDB_PATH:-}" ]]; then
    echo "error: --with-oracle requires ROCKSDB_PATH to be set" >&2
    echo "example: export ROCKSDB_PATH=/path/to/rocksdb" >&2
    exit 2
  fi
  if [[ ! -x "$ROCKSDB_PATH/ldb" || ! -x "$ROCKSDB_PATH/sst_dump" ]]; then
    echo "error: --with-oracle requires runnable tools:" >&2
    echo "  - $ROCKSDB_PATH/ldb" >&2
    echo "  - $ROCKSDB_PATH/sst_dump" >&2
    exit 2
  fi

  echo "[check-testdata] running oracle-enabled goldentests..."
  (
    cd "$ROOT_DIR"
    go test -v ./cmd/goldentest/... -count=1
  )
fi

echo "[check-testdata] OK"


