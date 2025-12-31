#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
scripts/fixtures/doctor.sh

Checks whether the RocksDB C++ oracle tools are configured and runnable.

Inputs:
  - ROCKSDB_PATH: path to a RocksDB checkout/build that contains:
      - ldb
      - sst_dump

Optional convenience:
  If ROCKSDB_PATH is unset and $HOME/Workspace/rocksdb exists, the script will
  suggest using that location but will not export it for you.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ -z "${ROCKSDB_PATH:-}" ]]; then
  if [[ -d "$HOME/Workspace/rocksdb" ]]; then
    echo "oracle: ROCKSDB_PATH is not set"
    echo "oracle: found candidate at: \$HOME/Workspace/rocksdb"
    echo "oracle: suggestion: export ROCKSDB_PATH=\"\$HOME/Workspace/rocksdb\""
    exit 2
  fi

  echo "oracle: ROCKSDB_PATH is not set"
  echo "oracle: set it to a RocksDB checkout/build that contains ldb and sst_dump"
  exit 2
fi

if [[ ! -d "$ROCKSDB_PATH" ]]; then
  echo "oracle: ROCKSDB_PATH is not a directory: $ROCKSDB_PATH"
  exit 2
fi

LDB="$ROCKSDB_PATH/ldb"
SST_DUMP="$ROCKSDB_PATH/sst_dump"

if [[ ! -x "$LDB" ]]; then
  echo "oracle: missing or non-executable: $LDB"
  exit 2
fi

if [[ ! -x "$SST_DUMP" ]]; then
  echo "oracle: missing or non-executable: $SST_DUMP"
  exit 2
fi

echo "oracle: ROCKSDB_PATH=$ROCKSDB_PATH"
echo "oracle: ldb=$LDB"
echo "oracle: sst_dump=$SST_DUMP"

tool_env() {
  local os
  os="$(uname -s)"
  case "$os" in
    Darwin)
      # Ensure librocksdb_tools.dylib is discoverable when invoking RocksDB tools directly.
      export DYLD_LIBRARY_PATH="$ROCKSDB_PATH${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
      ;;
    Linux)
      export LD_LIBRARY_PATH="$ROCKSDB_PATH${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
      ;;
  esac
}

tool_env

echo
echo "oracle: ldb --version"
if ! "$LDB" --version; then
  echo "oracle: FAILED to run ldb (check DYLD_LIBRARY_PATH/LD_LIBRARY_PATH and RocksDB build outputs)" >&2
  exit 2
fi

echo
echo "oracle: sst_dump --help (first 5 lines)"
if ! "$SST_DUMP" --help 2>/dev/null | sed -n '1,5p'; then
  echo "oracle: FAILED to run sst_dump (check DYLD_LIBRARY_PATH/LD_LIBRARY_PATH and RocksDB build outputs)" >&2
  exit 2
fi

echo
echo "oracle: OK"


