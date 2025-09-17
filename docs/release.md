# RockyardKV Release Checklist

This document describes the process for creating a new release of RockyardKV.

## Pre-Release Checklist

### 1. Code Quality

- [ ] All tests pass: `make test`
- [ ] All lints pass: `make lint`
- [ ] Race detector passes: `go test -race ./...`
- [ ] No known critical bugs

### 2. Documentation

- [ ] README.md is up to date
- [ ] CHANGELOG.md updated with new changes
- [ ] API documentation reviewed
- [ ] Performance guide updated if needed

### 3. Compatibility

- [ ] Golden tests pass (C++ ↔ Go format compatibility)
- [ ] Backward compatibility verified (if not breaking release)
- [ ] Migration notes documented (if breaking changes)

### 4. Performance

- [ ] Benchmarks run: `make bench`
- [ ] No performance regressions
- [ ] Benchmark results documented

### 5. Testing

- [ ] Unit tests: `go test ./...`
- [ ] Integration tests pass
- [ ] Stress test passes: `go run ./cmd/stresstest -duration=5m`
- [ ] Smoke test passes: `go run ./cmd/smoketest`
- [ ] Fuzz tests run for key packages

---

## Release Process

### Step 1: Version Bump

1. Update version in relevant files
2. Update CHANGELOG.md

```bash
# Update CHANGELOG.md with release notes
vim CHANGELOG.md
```

### Step 2: Create Release Branch (for major/minor)

```bash
# For major/minor releases
git checkout -b release/v1.0.0
git push -u origin release/v1.0.0
```

### Step 3: Final Tests

```bash
# Run full test suite
make check

# Run stress test
go run ./cmd/stresstest -duration=10m -threads=100 -keys=100000

# Run benchmarks
make bench > benchmarks_v1.0.0.txt
```

### Step 4: Create Tag

```bash
# Create annotated tag
git tag -a v1.0.0 -m "Release v1.0.0"

# Push tag
git push origin v1.0.0
```

### Step 5: Create GitHub Release

1. Go to GitHub Releases
2. Click "Create a new release"
3. Select the tag
4. Add release notes from CHANGELOG.md
5. Publish release

### Step 6: Post-Release

- [ ] Announce release (if applicable)
- [ ] Update documentation site (if applicable)
- [ ] Monitor for issues

---

## Version Numbering

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking API changes
- **MINOR**: New features, backward compatible
- **PATCH**: Bug fixes, backward compatible

Examples:
- `v0.1.0` - Initial development
- `v1.0.0` - First stable release
- `v1.1.0` - New features
- `v1.1.1` - Bug fixes

---

## Changelog Format

```markdown
## [Version] - YYYY-MM-DD

### Added
- New features

### Changed
- Changes in existing functionality

### Fixed
- Bug fixes

### Removed
- Removed features

### Security
- Security fixes

### Breaking Changes
- API breaking changes (major versions only)
```

---

## Release Types

### Alpha (`v0.x.y`)
- API may change
- Not recommended for production
- Testing and feedback welcome

### Beta (`v1.0.0-beta.x`)
- Feature complete
- API relatively stable
- Limited production use

### Stable (`v1.x.y`)
- Production ready
- API stable within major version
- Security fixes provided

---

## Emergency Hotfix Process

For critical bugs in released versions:

1. Create hotfix branch from release tag:
   ```bash
   git checkout -b hotfix/v1.0.1 v1.0.0
   ```

2. Apply minimal fix

3. Run targeted tests

4. Create patch release:
   ```bash
   git tag -a v1.0.1 -m "Hotfix v1.0.1"
   git push origin v1.0.1
   ```

5. Cherry-pick to main if applicable

---

## Checklist Template

Copy this for each release:

```markdown
## Release v1.x.y Checklist

### Pre-Release
- [ ] Tests pass
- [ ] Lints pass
- [ ] CHANGELOG updated
- [ ] Stress test passes
- [ ] Benchmarks recorded

### Release
- [ ] Tag created
- [ ] GitHub release published
- [ ] Documentation updated

### Post-Release
- [ ] Monitored for issues
- [ ] Announced (if major)
```

