package campaign

import (
	"os"
	"testing"
)

func TestNewOracleFromEnv(t *testing.T) {
	// Save and restore original env
	orig := os.Getenv("ROCKSDB_PATH")
	defer os.Setenv("ROCKSDB_PATH", orig)

	// Test with empty env
	os.Setenv("ROCKSDB_PATH", "")
	oracle := NewOracleFromEnv()
	if oracle != nil {
		t.Error("NewOracleFromEnv() should return nil when ROCKSDB_PATH is not set")
	}

	// Test with env set
	os.Setenv("ROCKSDB_PATH", "/path/to/rocksdb")
	oracle = NewOracleFromEnv()
	if oracle == nil {
		t.Fatal("NewOracleFromEnv() should return non-nil when ROCKSDB_PATH is set")
	}
	if oracle.RocksDBPath != "/path/to/rocksdb" {
		t.Errorf("oracle.RocksDBPath = %q, want %q", oracle.RocksDBPath, "/path/to/rocksdb")
	}
}

func TestOracleAvailable_NilOracle(t *testing.T) {
	var oracle *Oracle
	if oracle.Available() {
		t.Error("nil Oracle.Available() should return false")
	}
}

func TestOracleAvailable_NoPath(t *testing.T) {
	oracle := &Oracle{}
	if oracle.Available() {
		t.Error("Oracle.Available() should return false when no paths are set")
	}
}

func TestGateCheck(t *testing.T) {
	tests := []struct {
		name           string
		requiresOracle bool
		oracle         *Oracle
		wantErr        bool
	}{
		{
			name:           "no oracle required",
			requiresOracle: false,
			oracle:         nil,
			wantErr:        false,
		},
		{
			name:           "oracle required but nil",
			requiresOracle: true,
			oracle:         nil,
			wantErr:        true,
		},
		{
			name:           "oracle required but not available",
			requiresOracle: true,
			oracle:         &Oracle{},
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inst := &Instance{
				Name:           "test",
				RequiresOracle: tt.requiresOracle,
			}

			err := GateCheck(inst, tt.oracle)
			if (err != nil) != tt.wantErr {
				t.Errorf("GateCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestToolResultOK(t *testing.T) {
	tests := []struct {
		name     string
		result   ToolResult
		expected bool
	}{
		{
			name:     "success",
			result:   ToolResult{ExitCode: 0, Err: nil},
			expected: true,
		},
		{
			name:     "non-zero exit",
			result:   ToolResult{ExitCode: 1, Err: nil},
			expected: false,
		},
		{
			name:     "error set",
			result:   ToolResult{ExitCode: 0, Err: ErrOracleNotConfigured},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.result.OK()
			if got != tt.expected {
				t.Errorf("ToolResult.OK() = %v, want %v", got, tt.expected)
			}
		})
	}
}
