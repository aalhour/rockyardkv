# Release checklist

Use this checklist to cut a RockyardKV release.
Follow it in order.
Keep changes small and reviewable.

## Before you tag a release

1. Run local quality checks.
1. Run `make check`.
1. Run `go test ./...`.

1. Run compatibility checks against the RocksDB oracle.
1. Set `ROCKSDB_PATH` to a RocksDB `v10.7.5` build directory.
1. Run `make test-e2e-golden`.

1. Confirm the working tree is clean.
1. Run `git status`.

## Update release versions

1. Choose a new version.
1. Use a SemVer tag like `v0.3.2`.

1. Update the project version file.
1. Edit `VERSION`.
1. Set `rockyardkv vX.Y.Z` to your new version.

1. Update the Makefile version.
1. Edit `Makefile`.
1. Set `VERSION := vX.Y.Z` to your new version.

1. Update the changelog.
1. Edit `CHANGELOG.md`.
1. Add a new section for `vX.Y.Z`.
1. Use the current date.

1. Update release documentation.
1. Edit `docs/release.md` if it references old versions.
1. Keep examples consistent with the current release series.

## Verify documentation and public entry points

1. Verify the pkg.go.dev badge.
1. Open `README.md`.
1. Confirm the badge targets `github.com/aalhour/rockyardkv`.

1. Verify import paths in docs.
1. Search for stale imports.
1. Run `rg -n "github.com/aalhour/rockyardkv/" README.md docs/`.
1. Prefer `github.com/aalhour/rockyardkv` in new documentation.

1. Verify the architecture doc matches the repo.
1. Open `docs/architecture.md`.
1. Confirm the package tree matches `go list ./...`.

## Build and test release artifacts

1. Build release binaries.
1. Run `make build-release`.
1. Confirm artifacts exist under `dist/`.

1. Verify examples still run.
1. Run `make examples`.

## Tag and publish

1. Tag the release.
1. Create a git tag `vX.Y.Z`.

1. Publish the release.
1. Use your normal release process for your hosting provider.

## After you tag a release

1. Run cleanup.
1. Run `make clean`.

1. Verify CI results.
1. Confirm the CI workflow passes for the tag.


