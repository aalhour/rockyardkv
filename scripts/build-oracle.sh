#!/usr/bin/env bash
# Build RocksDB oracle tools (ldb and sst_dump).
#
# Prerequisites:
#   - ROCKSDB_PATH must be set to the RocksDB source directory
#   - C++ compiler (clang++ or g++)
#   - Make
#
# Usage:
#   ./scripts/build-oracle.sh
#
# After running, ldb and sst_dump binaries will be in ROCKSDB_PATH.

set -euo pipefail

if [ -z "$ROCKSDB_PATH" ]; then
    echo "error: ROCKSDB_PATH is not set"
    echo "Set it to the path of your RocksDB source directory:"
    echo "  export ROCKSDB_PATH=/path/to/rocksdb"
    exit 1
fi

if [ ! -d "$ROCKSDB_PATH" ]; then
    echo "error: ROCKSDB_PATH does not exist: $ROCKSDB_PATH"
    exit 1
fi

if [ ! -f "$ROCKSDB_PATH/Makefile" ]; then
    echo "error: ROCKSDB_PATH does not contain a Makefile: $ROCKSDB_PATH"
    exit 1
fi

cd "$ROCKSDB_PATH"

echo "Building RocksDB oracle tools in $ROCKSDB_PATH..."
echo ""

# Build ldb and sst_dump.
# On macOS, also build shared_lib so the tools can locate librocksdb_tools.dylib.
jobs="$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)"
case "$(uname -s)" in
  Darwin)
    make -j"$jobs" shared_lib ldb sst_dump
    ;;
  *)
    make -j"$jobs" ldb sst_dump
    ;;
esac

echo ""
echo "Oracle tools built successfully:"
echo "  ldb:      $ROCKSDB_PATH/ldb"
echo "  sst_dump: $ROCKSDB_PATH/sst_dump"

# Verify binaries exist
if [ ! -x "$ROCKSDB_PATH/ldb" ]; then
    echo "error: ldb binary not found or not executable"
    exit 1
fi

if [ ! -x "$ROCKSDB_PATH/sst_dump" ]; then
    echo "error: sst_dump binary not found or not executable"
    exit 1
fi

echo ""
echo "Verification:"
echo -n "  ldb version: "
"$ROCKSDB_PATH/ldb" --version 2>&1 | head -1 || echo "(version not available)"
echo -n "  sst_dump:    "
"$ROCKSDB_PATH/sst_dump" 2>&1 | head -1 || echo "(help output)"

echo ""
echo "Done. Oracle tools are ready."

