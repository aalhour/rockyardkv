#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
scripts/fixtures/validate.sh

Wrapper around scripts/check-testdata.sh with optional oracle enablement.

Usage:
  scripts/fixtures/validate.sh
  scripts/fixtures/validate.sh --with-oracle

Oracle:
  - If --with-oracle is used, ROCKSDB_PATH must be set, or you can set it to the
    common workspace location:

      export ROCKSDB_PATH="$HOME/Workspace/rocksdb"
EOF
}

with_oracle=0
for arg in "$@"; do
  case "$arg" in
    -h|--help) usage; exit 0 ;;
    --with-oracle) with_oracle=1 ;;
    *)
      echo "unknown argument: $arg" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$with_oracle" -eq 1 ]]; then
  if [[ -z "${ROCKSDB_PATH:-}" ]]; then
    echo "missing ROCKSDB_PATH (required for --with-oracle)" >&2
    exit 2
  fi
  exec bash scripts/check-testdata.sh --with-oracle
fi

exec bash scripts/check-testdata.sh


