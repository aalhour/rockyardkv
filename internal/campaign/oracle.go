package campaign

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

// Oracle provides access to the C++ RocksDB tools (ldb, sst_dump).
// These tools are used to verify database consistency and format correctness.
type Oracle struct {
	// RocksDBPath is the path to the RocksDB source/build directory.
	// Should contain ldb and sst_dump binaries.
	RocksDBPath string

	// LDBPath is the explicit path to the ldb binary.
	// If empty, uses RocksDBPath/ldb.
	LDBPath string

	// SSTDumpPath is the explicit path to the sst_dump binary.
	// If empty, uses RocksDBPath/sst_dump.
	SSTDumpPath string
}

// ErrOracleNotConfigured indicates the oracle tools are not available.
var ErrOracleNotConfigured = errors.New("oracle not configured: set ROCKSDB_PATH or provide explicit tool paths")

// ErrOracleToolNotFound indicates a specific oracle tool was not found.
var ErrOracleToolNotFound = errors.New("oracle tool not found")

// NewOracleFromEnv creates an Oracle from environment variables.
// Uses ROCKSDB_PATH if set, otherwise returns nil.
func NewOracleFromEnv() *Oracle {
	rocksdbPath := os.Getenv("ROCKSDB_PATH")
	if rocksdbPath == "" {
		return nil
	}
	return &Oracle{RocksDBPath: rocksdbPath}
}

// Available returns true if the oracle tools are configured and accessible.
func (o *Oracle) Available() bool {
	if o == nil {
		return false
	}
	ldb, err := o.findLDB()
	if err != nil {
		return false
	}
	sst, err := o.findSSTDump()
	if err != nil {
		return false
	}
	// Check both are executable
	if _, err := os.Stat(ldb); err != nil {
		return false
	}
	if _, err := os.Stat(sst); err != nil {
		return false
	}
	return true
}

// findLDB returns the path to the ldb binary.
func (o *Oracle) findLDB() (string, error) {
	if o.LDBPath != "" {
		return o.LDBPath, nil
	}
	if o.RocksDBPath != "" {
		return filepath.Join(o.RocksDBPath, "ldb"), nil
	}
	return "", ErrOracleNotConfigured
}

// findSSTDump returns the path to the sst_dump binary.
func (o *Oracle) findSSTDump() (string, error) {
	if o.SSTDumpPath != "" {
		return o.SSTDumpPath, nil
	}
	if o.RocksDBPath != "" {
		return filepath.Join(o.RocksDBPath, "sst_dump"), nil
	}
	return "", ErrOracleNotConfigured
}

// ToolResult contains the result of running an oracle tool.
type ToolResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Err      error
}

// OK returns true if the tool exited successfully.
func (r *ToolResult) OK() bool {
	return r.Err == nil && r.ExitCode == 0
}

// CheckConsistency runs `ldb checkconsistency` on the database.
// Returns OK if the database passes all consistency checks.
func (o *Oracle) CheckConsistency(dbPath string) *ToolResult {
	ldb, err := o.findLDB()
	if err != nil {
		return &ToolResult{Err: err, ExitCode: -1}
	}

	return o.runTool(ldb, "--db="+dbPath, "checkconsistency")
}

// DumpManifest runs `ldb manifest_dump` on the database.
func (o *Oracle) DumpManifest(dbPath string) *ToolResult {
	ldb, err := o.findLDB()
	if err != nil {
		return &ToolResult{Err: err, ExitCode: -1}
	}

	return o.runTool(ldb, "--db="+dbPath, "manifest_dump")
}

// DumpSST runs `sst_dump` on an SST file.
func (o *Oracle) DumpSST(sstPath string, args ...string) *ToolResult {
	sstDump, err := o.findSSTDump()
	if err != nil {
		return &ToolResult{Err: err, ExitCode: -1}
	}

	cmdArgs := append([]string{"--file=" + sstPath}, args...)
	return o.runTool(sstDump, cmdArgs...)
}

// runTool executes an oracle tool and captures output.
// Handles library path setup for macOS (DYLD_LIBRARY_PATH).
func (o *Oracle) runTool(tool string, args ...string) *ToolResult {
	cmd := exec.Command(tool, args...)

	// Set up environment with library path for RocksDB dependencies
	cmd.Env = o.toolEnv()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	result := &ToolResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}

	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			result.ExitCode = exitErr.ExitCode()
		} else {
			result.ExitCode = -1
			result.Err = err
		}
	}

	return result
}

// toolEnv returns the environment for running oracle tools.
// On macOS, sets DYLD_LIBRARY_PATH to include RocksDB library directory.
// On Linux, sets LD_LIBRARY_PATH.
func (o *Oracle) toolEnv() []string {
	env := os.Environ()

	if o.RocksDBPath == "" {
		return env
	}

	// Common library directories within RocksDB build
	libDirs := []string{
		o.RocksDBPath,
		filepath.Join(o.RocksDBPath, "lib"),
		filepath.Join(o.RocksDBPath, "build"),
	}

	libPath := ""
	for _, dir := range libDirs {
		if _, err := os.Stat(dir); err == nil {
			if libPath != "" {
				libPath += string(filepath.ListSeparator)
			}
			libPath += dir
		}
	}

	if libPath == "" {
		return env
	}

	// Set the appropriate library path variable for the platform
	switch runtime.GOOS {
	case "darwin":
		env = append(env, "DYLD_LIBRARY_PATH="+libPath)
	case "linux":
		env = append(env, "LD_LIBRARY_PATH="+libPath)
	}

	return env
}

// GateCheck verifies that the oracle is available if required by the instance.
// Returns an error if oracle is required but not available.
func GateCheck(instance *Instance, oracle *Oracle) error {
	if !instance.RequiresOracle {
		return nil
	}
	if oracle == nil || !oracle.Available() {
		return fmt.Errorf("instance %q requires oracle but oracle is not available: %w",
			instance.Name, ErrOracleNotConfigured)
	}
	return nil
}
