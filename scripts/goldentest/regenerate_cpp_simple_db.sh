#!/usr/bin/env bash
set -euo pipefail

# Regenerate the C++-generated "simple_db" fixtures used by cmd/goldentest.
#
# Requirements:
#   - ROCKSDB_PATH must point to a RocksDB v10.7.5 checkout.
#   - Either:
#     - RocksDB tools are runnable (db_bench), OR
#     - RocksDB shared library is available (librocksdb*.dylib / .so) so the fallback
#       minimal C++ generator can link and run.
#
# Notes:
#   - This script deletes RocksDB LOG files (LOG/LOG.old.*) from the fixture directory
#     to avoid committing machine-local absolute paths.
#   - The fixture file numbers (MANIFEST-*, *.sst, etc.) are determined by RocksDB.
#
# How to run (clean clone friendly):
#
#   cd "<REPO_ROOT>"
#
#   # 1) Point to RocksDB v10.7.5
#   export ROCKSDB_PATH="/path/to/rocksdb"
#
#   # 2) Ensure librocksdb is built (needed for the fallback generator)
#   ( cd "$ROCKSDB_PATH" && make shared_lib )
#
#   # 3) If your librocksdb depends on external dylibs (snappy/lz4/zstd), point the
#   #    script at the directory containing them (optional; macOS often needs this)
#   export ROCKSDB_DEPS_LIBDIR="/path/to/deps/lib"
#
#   # 4) Regenerate fixtures in-place
#   scripts/goldentest/regenerate_cpp_simple_db.sh
#
#   # 5) Verify the fixture-based goldentests
#   go test -v ./cmd/goldentest -run 'TestCppWritesGoReads_Fixtures|TestManifest_Contract_CppWritesGoReads|TestDatabase_Contract_CppWritesGoReads|TestWAL_CppWritesGoReads'

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/testdata/rocksdb/v10.7.5/db_samples/simple_db}"

if [[ -z "${ROCKSDB_PATH:-}" ]]; then
  echo "Error: ROCKSDB_PATH is not set" >&2
  echo "Example: export ROCKSDB_PATH=\"/path/to/rocksdb\"" >&2
  exit 2
fi

DB_BENCH="$ROCKSDB_PATH/db_bench"

# Ensure the dynamic loader can find RocksDB + its compression deps.
#
# - RocksDB dylib: in $ROCKSDB_PATH
# - Optional deps (snappy/lz4/zstd): user-configurable via ROCKSDB_DEPS_LIBDIR
DEPS_LIBDIR="${ROCKSDB_DEPS_LIBDIR:-}"

export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:-}:$ROCKSDB_PATH${DEPS_LIBDIR:+:$DEPS_LIBDIR}"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:$ROCKSDB_PATH${DEPS_LIBDIR:+:$DEPS_LIBDIR}"

TMP_PARENT="$(mktemp -d)"
trap 'rm -rf "$TMP_PARENT"' EXIT
TMP_DB="$TMP_PARENT/simple_db_gen"

echo "Generating RocksDB fixture DB..."
echo "  ROCKSDB_PATH=$ROCKSDB_PATH"
echo "  OUT_DIR=$OUT_DIR"
echo "  TMP_DB=$TMP_DB"

rm -rf "$TMP_DB"
mkdir -p "$TMP_DB"

gen_with_db_bench() {
  if [[ ! -x "$DB_BENCH" ]]; then
    return 127
  fi
  # Create a small DB that produces at least one SST + MANIFEST.
  # We keep settings simple and deterministic-ish:
  # - disable auto compactions to reduce churn
  # - small write buffer so fillseq forces flushes
  "$DB_BENCH" \
    --db="$TMP_DB" \
    --benchmarks=fillseq \
    --num=1000 \
    --value_size=100 \
    --compression_type=none \
    --create_if_missing=1 \
    --use_existing_db=0 \
    --disable_auto_compactions=1 \
    --write_buffer_size=1048576 \
    --max_background_compactions=1 \
    --max_background_flushes=1 \
    --stats_interval=0 \
    >/dev/null
}

gen_with_minimal_cpp() {
  # Fallback for environments where RocksDB tools are not runnable (e.g., gflags missing).
  # This compiles a tiny C++ program that uses the RocksDB public API to create the fixture.
  local cpp="$TMP_PARENT/gen_simple_db.cc"
  local bin="$TMP_PARENT/gen_simple_db"

  cat >"$cpp" <<'CPP'
#include <rocksdb/db.h>
#include <rocksdb/options.h>
using rocksdb::DB;
using rocksdb::Options;
using rocksdb::Status;
using rocksdb::WriteOptions;
using rocksdb::FlushOptions;

int main(int argc, char** argv) {
  if (argc != 2) return 2;
  const char* db_path = argv[1];

  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 1 << 20; // 1MB
  options.compression = rocksdb::kNoCompression;

  DB* db = nullptr;
  Status s = DB::Open(options, db_path, &db);
  if (!s.ok()) return 3;

  WriteOptions wo;
  for (int i = 0; i < 1000; i++) {
    std::string k = "key" + std::to_string(i);
    std::string v(100, 'v');
    Status ps = db->Put(wo, k, v);
    if (!ps.ok()) return 4;
  }

  FlushOptions fo;
  Status fs = db->Flush(fo);
  if (!fs.ok()) return 5;

  delete db;
  return 0;
}
CPP

  # Prefer clang++ if available; fallback to c++.
  local cxx="${CXX:-clang++}"
  if ! command -v "$cxx" >/dev/null 2>&1; then
    cxx="c++"
  fi

  "$cxx" -std=c++20 -O2 \
    -I"$ROCKSDB_PATH/include" \
    "$ROCKSDB_PATH/librocksdb.dylib" \
    -Wl,-rpath,"$ROCKSDB_PATH" \
    -o "$bin" "$cpp"

  "$bin" "$TMP_DB"
}

if gen_with_db_bench 2>/dev/null; then
  :
else
  echo "db_bench not available or failed to run; falling back to minimal C++ generator."
  gen_with_minimal_cpp
fi

# Replace the fixture dir atomically-ish.
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

safe_rm_rf_dir "$OUT_DIR"
mkdir -p "$(dirname "$OUT_DIR")"
cp -R "$TMP_DB" "$OUT_DIR"

# Remove RocksDB LOG files (they embed machine-local absolute paths).
rm -f "$OUT_DIR/LOG" "$OUT_DIR"/LOG.old.* || true

echo "Fixture regenerated."
echo "Sanity check (recommended):"
echo "  cd \"$ROOT_DIR\""
echo "  go test -v ./cmd/goldentest -run 'TestCppWritesGoReads_Fixtures|TestManifest_Contract_CppWritesGoReads|TestDatabase_Contract_CppWritesGoReads'"
