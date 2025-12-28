# Contributing Tests

This guide explains how to add new tests to RockyardKV.

## Decision Tree: Where to Add Coverage

```
Where should I add test coverage?
│
├── Is it a format compatibility issue?
│   ├── Yes → Add fixture to testdata/cpp_generated/
│   │         + golden test in cmd/goldentest/
│   └── No ↓
│
├── Is it a crash/recovery issue?
│   ├── Yes, specific crash point → Whitebox test in
│   │                               cmd/crashtest/scenario_whitebox_test.go
│   ├── Yes, general durability → Scenario test in
│   │                             cmd/crashtest/scenario_test.go
│   └── No ↓
│
├── Is it a concurrency issue?
│   ├── Yes → Stress test in cmd/stresstest/
│   │         + db/db_concurrent_test.go
│   └── No ↓
│
├── Is it a parsing or corruption issue?
│   ├── Yes → Fuzz test + adversarial test
│   │         in cmd/adversarialtest/
│   └── No ↓
│
└── Is it an API behavior issue?
    ├── Yes → Unit test in
    │         db/*_test.go or internal/*_test.go
    └── No → Unit test in
              internal/*_test.go
```

## Naming Conventions

Name tests after the behavior they verify, not the bug they fix.

| Do | Don't |
|---|---|
| `TestIterator_Prev_CrossesSSTBoundary` | `TestBug123` |
| `TestWAL_Recovery_SyncedWritesSurvive` | `TestFixIssue456` |
| `TestSST_ReadZlibCompressed_FormatV6` | `TestZlibFix` |

## Adding Unit Tests

Unit tests live in `*_test.go` files next to the code they test.

### Pattern

```go
func TestComponent_Method_Condition(t *testing.T) {
    // Arrange
    input := setupInput()
    
    // Act
    result := component.Method(input)
    
    // Assert
    if result != expected {
        t.Errorf("Method() = %v, want %v", result, expected)
    }
}
```

### Table-Driven Tests

Use tables for matrix testing:

```go
func TestSST_FormatMatrix(t *testing.T) {
    versions := []uint32{3, 4, 5, 6}
    compressions := []compression.Type{
        compression.None,
        compression.Snappy,
    }
    
    for _, v := range versions {
        for _, c := range compressions {
            t.Run(fmt.Sprintf("v%d/%s", v, c), func(t *testing.T) {
                // Test this combination
            })
        }
    }
}
```

## Adding Golden Tests

Golden tests live in `cmd/goldentest/`.

### Add a C++ Fixture

1. Generate the fixture with C++ RocksDB
2. Place it in `testdata/cpp_generated/`
3. The test suite automatically picks it up

### Add a Go-Writes-C++-Verifies Test

```go
func TestSST_CppVerifiesGoOutput(t *testing.T) {
    path := t.TempDir() + "/test.sst"
    
    // Write with Go
    writeSST(t, path, testData)
    
    // Verify with C++ sst_dump
    runSstDump(t, path, "--command=check", "--verify_checksums")
}
```

## Adding Whitebox Tests

Whitebox tests live in `cmd/crashtest/scenario_whitebox_test.go`.

### Add a Kill Point

1. Define the constant in `internal/testutil/killpoint.go`:
   ```go
   KPMyComponent0 = "MyComponent.Action:0"
   ```

2. Wire it in production code:
   ```go
   // Whitebox [crashtest]: crash before X — tests Y
   testutil.MaybeKill(testutil.KPMyComponent0)
   ```

3. Add to sweep test:
   ```go
   writeFlushKillpoints := []string{
       // ... existing
       testutil.KPMyComponent0,
   }
   ```

4. Add dedicated scenario test:
   ```go
   func TestScenarioWhitebox_MyComponent0_ContractDescription(t *testing.T) {
       dir := t.TempDir()
       
       // Create baseline
       {
           database := createDB(t, dir)
           // ... setup
           database.Close()
       }
       
       // Crash at kill point
       runWhiteboxChild(t, dir, testutil.KPMyComponent0, func(database db.DB) {
           // ... trigger the code path
       })
       
       // Verify recovery
       database := openDB(t, dir)
       defer database.Close()
       // ... verify invariants
   }
   ```

## Adding Blackbox Scenario Tests

Scenario tests live in `cmd/crashtest/scenario_test.go`.

### Pattern

```go
func TestScenario_ContractDescription(t *testing.T) {
    dir := t.TempDir()
    
    // Run child process
    runScenarioChild(t, dir, func(database db.DB) {
        // Perform operation
        opts := db.DefaultWriteOptions()
        opts.Sync = true
        database.Put(opts, key, value)
    })
    
    // Verify after crash
    database := openDB(t, dir)
    defer database.Close()
    
    got, err := database.Get(nil, key)
    require.NoError(t, err)
    require.Equal(t, value, got)
}
```

## Adding Fuzz Tests

Fuzz tests live next to their targets.

### Pattern

```go
func FuzzComponent_Method(f *testing.F) {
    // Add seed corpus
    f.Add([]byte("test input"))
    
    f.Fuzz(func(t *testing.T, input []byte) {
        // Call the method
        result, err := component.Method(input)
        
        // Assert properties (not just "no crash")
        if err == nil {
            // Valid input: verify semantic properties
            if !isValid(result) {
                t.Errorf("invalid result for input %q", input)
            }
        }
    })
}
```

## Adding Stress Test Scenarios

Extend `cmd/stresstest/` for new operation types.

### Add a New Operation

1. Define operation in `operations.go`
2. Add to operation generator
3. Add oracle validation

## Test Quality Checklist

Before submitting:

- [ ] Test names describe the contract, not the bug
- [ ] Tests fail when the contract is violated
- [ ] Tests pass when the contract is satisfied
- [ ] Tests survive implementation refactoring
- [ ] No "test that only asserts no error"
- [ ] Matrix tests cover relevant combinations
- [ ] Golden tests use C++ oracle when applicable

## Running Tests Locally

```bash
# Quick check
make check

# Full suite
make test

# Specific category
make test-e2e-golden
make test-e2e-crash
make test-e2e-stress

# With build tags
go test -tags crashtest ./cmd/crashtest/...
go test -tags synctest ./internal/testutil/...
```

## CI Requirements

All PRs must pass:

1. `make check` — lint + short tests
2. `make test` — full test suite with race detector
3. `make test-e2e-golden` — C++ compatibility

Long-running tests run nightly:

- `make test-e2e-crash-long`
- `make test-e2e-stress-long`

