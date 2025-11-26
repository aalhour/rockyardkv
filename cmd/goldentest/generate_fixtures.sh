#!/bin/bash
# Generate C++ test fixtures for golden tests
#
# This script uses RocksDB's ldb tool to create test databases
# that will be used to verify Go's compatibility.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TESTDATA_DIR="${SCRIPT_DIR}/testdata/cpp_generated"
ROCKSDB_DIR="${ROCKSDB_DIR:-/Users/ahmad/Workspace/rocksdb}"
LDB="${ROCKSDB_DIR}/ldb"

# Set library path for dynamic linking
export DYLD_LIBRARY_PATH="${ROCKSDB_DIR}:${DYLD_LIBRARY_PATH}"
export LD_LIBRARY_PATH="${ROCKSDB_DIR}:${LD_LIBRARY_PATH}"

# Check for ldb
if [ ! -x "$LDB" ]; then
    echo "Error: ldb not found at $LDB"
    echo "Please set ROCKSDB_DIR or build ldb first"
    exit 1
fi

echo "Using ldb: $LDB"
echo "Output directory: $TESTDATA_DIR"

# Create output directories
mkdir -p "$TESTDATA_DIR/sst"
mkdir -p "$TESTDATA_DIR/wal"
mkdir -p "$TESTDATA_DIR/manifest"
mkdir -p "$TESTDATA_DIR/block"

# Generate a simple test database
echo "Generating simple_db..."
rm -rf "$TESTDATA_DIR/sst/simple_db"
mkdir -p "$TESTDATA_DIR/sst/simple_db"

# Use ldb to put some data
"$LDB" --db="$TESTDATA_DIR/sst/simple_db" --create_if_missing put key1 value1
"$LDB" --db="$TESTDATA_DIR/sst/simple_db" put key2 value2
"$LDB" --db="$TESTDATA_DIR/sst/simple_db" put key3 value3

# Flush to create SST files
"$LDB" --db="$TESTDATA_DIR/sst/simple_db" compact

echo "Generated fixtures in $TESTDATA_DIR"

# List generated files
echo ""
echo "Generated files:"
find "$TESTDATA_DIR" -type f -name "*.sst" -o -name "*.log" -o -name "MANIFEST-*" | head -20

