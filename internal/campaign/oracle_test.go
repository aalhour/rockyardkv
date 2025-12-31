package campaign

import (
	"os"
	"runtime"
	"strings"
	"testing"
)

// Contract: NewOracleFromEnv returns nil when ROCKSDB_PATH is not set.
func TestNewOracleFromEnv_Empty(t *testing.T) {
	orig := os.Getenv("ROCKSDB_PATH")
	defer func() { _ = os.Setenv("ROCKSDB_PATH", orig) }()

	_ = os.Unsetenv("ROCKSDB_PATH")

	oracle := NewOracleFromEnv()
	if oracle != nil {
		t.Error("NewOracleFromEnv() should return nil when ROCKSDB_PATH is not set")
	}
}

// Contract: NewOracleFromEnv returns an Oracle with RocksDBPath set from the environment.
func TestNewOracleFromEnv_Set(t *testing.T) {
	orig := os.Getenv("ROCKSDB_PATH")
	defer func() { _ = os.Setenv("ROCKSDB_PATH", orig) }()

	_ = os.Setenv("ROCKSDB_PATH", "/path/to/rocksdb")

	oracle := NewOracleFromEnv()
	if oracle == nil {
		t.Fatal("NewOracleFromEnv() should return non-nil when ROCKSDB_PATH is set")
	}

	if oracle.RocksDBPath != "/path/to/rocksdb" {
		t.Errorf("RocksDBPath = %q, want %q", oracle.RocksDBPath, "/path/to/rocksdb")
	}
}

// Contract: A nil Oracle is not available.
func TestOracle_Available_NilOracle(t *testing.T) {
	var oracle *Oracle
	if oracle.Available() {
		t.Error("nil Oracle should not be available")
	}
}

// Contract: findLDB prefers explicit LDBPath over RocksDBPath.
func TestOracle_findLDB(t *testing.T) {
	tests := []struct {
		name     string
		oracle   Oracle
		wantPath string
		wantErr  bool
	}{
		{
			name:     "explicit LDBPath",
			oracle:   Oracle{LDBPath: "/custom/ldb"},
			wantPath: "/custom/ldb",
		},
		{
			name:     "RocksDBPath fallback",
			oracle:   Oracle{RocksDBPath: "/rocksdb"},
			wantPath: "/rocksdb/ldb",
		},
		{
			name:    "no paths configured",
			oracle:  Oracle{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.oracle.findLDB()
			if tt.wantErr {
				if err == nil {
					t.Error("findLDB() should return error")
				}
				return
			}
			if err != nil {
				t.Errorf("findLDB() error = %v", err)
				return
			}
			if got != tt.wantPath {
				t.Errorf("findLDB() = %q, want %q", got, tt.wantPath)
			}
		})
	}
}

// Contract: findSSTDump prefers explicit SSTDumpPath over RocksDBPath.
func TestOracle_findSSTDump(t *testing.T) {
	tests := []struct {
		name     string
		oracle   Oracle
		wantPath string
		wantErr  bool
	}{
		{
			name:     "explicit SSTDumpPath",
			oracle:   Oracle{SSTDumpPath: "/custom/sst_dump"},
			wantPath: "/custom/sst_dump",
		},
		{
			name:     "RocksDBPath fallback",
			oracle:   Oracle{RocksDBPath: "/rocksdb"},
			wantPath: "/rocksdb/sst_dump",
		},
		{
			name:    "no paths configured",
			oracle:  Oracle{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.oracle.findSSTDump()
			if tt.wantErr {
				if err == nil {
					t.Error("findSSTDump() should return error")
				}
				return
			}
			if err != nil {
				t.Errorf("findSSTDump() error = %v", err)
				return
			}
			if got != tt.wantPath {
				t.Errorf("findSSTDump() = %q, want %q", got, tt.wantPath)
			}
		})
	}
}

// Contract: On macOS, toolEnv sets DYLD_LIBRARY_PATH to include RocksDBPath.
func TestOracle_toolEnv_Darwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("test requires darwin")
	}

	tmpDir := t.TempDir()
	oracle := &Oracle{RocksDBPath: tmpDir}

	env := oracle.toolEnv()

	found := false
	for _, e := range env {
		if strings.HasPrefix(e, "DYLD_LIBRARY_PATH=") {
			found = true
			if !strings.Contains(e, tmpDir) {
				t.Errorf("DYLD_LIBRARY_PATH should contain %q, got %q", tmpDir, e)
			}
		}
	}

	if !found {
		t.Error("toolEnv() should set DYLD_LIBRARY_PATH on darwin")
	}
}

// Contract: On Linux, toolEnv sets LD_LIBRARY_PATH to include RocksDBPath.
func TestOracle_toolEnv_Linux(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("test requires linux")
	}

	tmpDir := t.TempDir()
	oracle := &Oracle{RocksDBPath: tmpDir}

	env := oracle.toolEnv()

	found := false
	for _, e := range env {
		if strings.HasPrefix(e, "LD_LIBRARY_PATH=") {
			found = true
			if !strings.Contains(e, tmpDir) {
				t.Errorf("LD_LIBRARY_PATH should contain %q, got %q", tmpDir, e)
			}
		}
	}

	if !found {
		t.Error("toolEnv() should set LD_LIBRARY_PATH on linux")
	}
}

// Contract: GateCheck succeeds when oracle is not required.
func TestGateCheck_OracleNotRequired(t *testing.T) {
	inst := &Instance{RequiresOracle: false}
	err := GateCheck(inst, nil)
	if err != nil {
		t.Errorf("GateCheck() should not error when oracle not required: %v", err)
	}
}

// Contract: GateCheck fails when oracle is required but nil.
func TestGateCheck_OracleRequiredButNil(t *testing.T) {
	inst := &Instance{Name: "test", RequiresOracle: true}
	err := GateCheck(inst, nil)
	if err == nil {
		t.Error("GateCheck() should error when oracle required but nil")
	}
}

// Contract: ToolResult.OK returns true only when ExitCode is 0 and Err is nil.
func TestToolResult_OK(t *testing.T) {
	tests := []struct {
		name   string
		result ToolResult
		want   bool
	}{
		{"success", ToolResult{ExitCode: 0, Err: nil}, true},
		{"non-zero exit", ToolResult{ExitCode: 1, Err: nil}, false},
		{"error set", ToolResult{ExitCode: 0, Err: ErrOracleNotConfigured}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.result.OK(); got != tt.want {
				t.Errorf("OK() = %v, want %v", got, tt.want)
			}
		})
	}
}
