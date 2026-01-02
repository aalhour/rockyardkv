# Contributing to RockyardKV

This document provides guidelines for contributing to the project.

## Code of conduct

Be respectful and constructive in all interactions.
Contributors of all experience levels are welcome.

## Prerequisites

Before you begin, install:

- **Go 1.25 or later** (we support the latest 2 stable versions starting from 1.25)
- Git
- Make (optional, for convenience commands)
- Docker (optional, for local stress testing)

### Go Version Policy

We follow Go's official support policy: **support for the latest two stable releases**.
Our baseline is Go 1.25 (for `sync.WaitGroup.Go` and other modern features).
As of this writing, we support Go 1.25. When Go 1.26 is released, we will support 1.25 and 1.26.

When a new Go version is released:
1. Add it to the CI matrix
2. Remove the oldest supported version
3. Update this document and `go.mod`

Reference: https://go.dev/doc/devel/release#policy

## Get started

Fork the repository and clone your fork:

```bash
git clone https://github.com/YOUR_USERNAME/rockyardkv.git
cd rockyardkv
git remote add upstream https://github.com/aalhour/rockyardkv.git
```

## Development setup

Build and test the project:

```bash
go mod download
go build ./...
go test ./...
```

Use make for common tasks:

```bash
make help        # Show available commands
make build       # Build binaries
make test        # Run tests
make lint        # Run linters
make check       # Full check (lint + test)
make bench       # Run benchmarks
```

## Make changes

### Branch naming

Use descriptive branch names:

- `feature/add-bloom-filter` for new features
- `fix/iterator-tombstone` for bug fixes
- `refactor/memtable-arena` for code refactoring
- `docs/performance-guide` for documentation updates
- `test/wal-fuzz` for test additions

### Commit messages

Follow the conventional commit format:

```
type(scope): description

[optional body]

[optional footer]
```

Examples:

```
feat(db): add write stalling for L0 overflow
fix(iterator): correct tombstone handling after flush
test(wal): add fuzz tests for reader/writer
docs(readme): update installation instructions
```

Valid types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`

## Testing

### Run tests

```bash
go test ./...                           # All tests
go test -race ./...                     # With race detector
go test -cover ./...                    # With coverage
go test ./db/... -run TestDBPutGet -v   # Specific test
```

### Write tests

Follow these guidelines:

1. **Unit tests** verify individual functions and methods
1. **Integration tests** verify component interactions
1. **Fuzz tests** verify behavior with random inputs
1. **Golden tests** verify format compatibility with C++ RocksDB

Example test:

```go
func TestFeatureName(t *testing.T) {
    db, cleanup := setupTestDB(t)
    defer cleanup()

    err := db.Put(nil, []byte("key"), []byte("value"))
    if err != nil {
        t.Fatalf("Put failed: %v", err)
    }

    value, err := db.Get(nil, []byte("key"))
    if err != nil {
        t.Fatalf("Get failed: %v", err)
    }
    if string(value) != "value" {
        t.Errorf("Got %q, want %q", value, "value")
    }
}
```

### Coverage goals

- Core packages (`db`, `wal`, `manifest`): 80% or higher
- All new features must include tests
- Bug fixes must include regression tests

## Code style

### Go conventions

Follow standard Go practices:

- Use `gofmt` for formatting
- Use `golint` and `go vet` for linting
- Follow [Effective Go](https://go.dev/doc/effective_go)

### Naming

```go
// Exported functions: PascalCase
func OpenDatabase(path string) (*DB, error)

// Unexported functions: camelCase
func parseHeader(data []byte) (*header, error)

// Constants: PascalCase or SCREAMING_SNAKE_CASE
const MaxSequenceNumber = 1<<56 - 1
const DEFAULT_BLOCK_SIZE = 4096
```

### Comments

```go
// Package rockyardkv provides a pure Go port of RocksDB.
package rockyardkv

// Open opens a database at the given path.
// If the database doesn't exist and CreateIfMissing is true, it creates one.
func Open(path string, opts *Options) (DB, error) {
    // ...
}
```

### Error handling

```go
// Use descriptive error messages with wrapping
return fmt.Errorf("db: failed to open manifest: %w", err)

// Define package-level error variables
var ErrNotFound = errors.New("db: key not found")
```

## Submit changes

### Before submitting

Verify your changes:

1. Tests pass: `go test ./...`
1. Code is formatted: `gofmt -w .`
1. No lint errors: `golangci-lint run`
1. Documentation is updated

### Pull request process

1. Create a pull request against the `main` branch
1. Fill out the description template
1. Wait for CI to pass
1. Address reviewer feedback
1. Squash commits if requested

### Pull request template

```markdown
## Description

Brief description of changes.

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing

- How was this tested?
- What tests were added?

## Checklist

- [ ] Tests pass
- [ ] Code is formatted
- [ ] Documentation updated
```

## Report issues

### Bug reports

Include:

- Go version (`go version`)
- Operating system
- Steps to reproduce
- Expected and actual behavior
- Minimal code example

### Feature requests

Include:

- Use case description
- Proposed solution
- Alternatives considered
- RocksDB parity notes (if applicable)

## Areas for contribution

### Good first issues

Look for issues labeled `good first issue`:

- Documentation improvements
- Test coverage additions
- Small bug fixes

### Larger projects

Check `tmp/TODO.md` for:

- Missing features
- Performance optimizations
- Test infrastructure improvements

## Get help

- Open a GitHub issue for questions
- Check existing issues and pull requests for similar problems
- Review the codebase documentation

Thank you for contributing to RockyardKV.
