#!/bin/bash
# Run golden tests with proper library paths
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROCKSDB_DIR="${ROCKSDB_DIR:-/Users/ahmad/Workspace/rocksdb}"

# Set library path for dynamic linking
export DYLD_LIBRARY_PATH="${ROCKSDB_DIR}:${DYLD_LIBRARY_PATH}"
export LD_LIBRARY_PATH="${ROCKSDB_DIR}:${LD_LIBRARY_PATH}"

# Generate fixtures if needed
if [ ! -d "${SCRIPT_DIR}/testdata/cpp_generated/sst/simple_db" ]; then
    echo "Generating C++ fixtures..."
    "${SCRIPT_DIR}/generate_fixtures.sh"
fi

# Run the golden tests
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"
go run ./cmd/goldentest \
  -fixtures "${SCRIPT_DIR}/testdata/cpp_generated" \
  -output "${SCRIPT_DIR}/testdata/go_generated" \
  -ldb "${ROCKSDB_DIR}/ldb" \
  -sst-dump "${ROCKSDB_DIR}/sst_dump" \
  "$@"

