// Oracle helpers for running C++ RocksDB tools on test artifacts.
//
// These helpers run ldb and sst_dump to verify format compatibility.
// They are used by durability and whitebox scenario tests.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CppOraclePathEnv is the environment variable to specify the C++ RocksDB tools directory.
// When set, scenario tests will run C++ oracle checks on the DB.
const CppOraclePathEnv = "ROCKYARDKV_CPP_ORACLE_PATH"

// runCppOracleChecks runs C++ RocksDB tools (ldb, sst_dump) on the artifact DB
// to verify format compatibility. Results are saved to the artifact directory.
func runCppOracleChecks(t *testing.T, artifactDir, dbPath string) {
	t.Helper()

	cppOraclePath := os.Getenv(CppOraclePathEnv)
	if cppOraclePath == "" {
		return // Oracle hook not enabled
	}

	ldbPath := filepath.Join(cppOraclePath, "ldb")
	sstDumpPath := filepath.Join(cppOraclePath, "sst_dump")

	// Check if ldb exists
	if _, err := os.Stat(ldbPath); err == nil {
		// Run checkconsistency (primary structural integrity check)
		runOracleTool(t, artifactDir, "ldb_checkconsistency.txt", ldbPath,
			"checkconsistency", "--db="+dbPath)

		// Run manifest_dump with proper CURRENT-based manifest selection
		runLdbManifestDump(t, artifactDir, dbPath, ldbPath)

		// Run ldb scan (verify DB is readable)
		runOracleTool(t, artifactDir, "ldb_scan.txt", ldbPath,
			"scan", "--db="+dbPath)
	}

	// Check if sst_dump exists
	if _, err := os.Stat(sstDumpPath); err == nil {
		// Run sst_dump on each SST file
		entries, err := os.ReadDir(dbPath)
		if err == nil {
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sst") {
					sstPath := filepath.Join(dbPath, entry.Name())
					outFile := fmt.Sprintf("sst_dump_%s.txt", entry.Name())
					runOracleTool(t, artifactDir, outFile, sstDumpPath,
						"--file="+sstPath, "--command=check")
				}
			}
		}
	}
}

// runLdbManifestDump handles ldb manifest_dump with proper CURRENT file parsing.
func runLdbManifestDump(t *testing.T, artifactDir, dbPath, ldbPath string) {
	t.Helper()

	// Read CURRENT file
	currentPath := filepath.Join(dbPath, "CURRENT")
	currentBytes, err := os.ReadFile(currentPath)
	if err != nil {
		errNote := fmt.Sprintf("CURRENT file missing or unreadable: %v", err)
		_ = os.WriteFile(filepath.Join(artifactDir, "current.txt"), []byte(errNote), 0644)
		_ = os.WriteFile(filepath.Join(artifactDir, "ldb_manifest_dump.txt"),
			[]byte("Skipped: "+errNote), 0644)
		t.Logf("⚠️  Skipping manifest_dump: %s", errNote)
		return
	}

	// Save CURRENT file contents
	_ = os.WriteFile(filepath.Join(artifactDir, "current.txt"), currentBytes, 0644)

	// Parse CURRENT to get active manifest name
	activeManifestName := strings.TrimSpace(string(currentBytes))
	if activeManifestName == "" || !strings.HasPrefix(activeManifestName, "MANIFEST-") {
		errNote := fmt.Sprintf("Invalid CURRENT contents: %q", string(currentBytes))
		_ = os.WriteFile(filepath.Join(artifactDir, "ldb_manifest_dump.txt"),
			[]byte("Skipped: "+errNote), 0644)
		t.Logf("⚠️  Skipping manifest_dump: %s", errNote)
		return
	}

	// Build full path to manifest
	activeManifestPath := filepath.Join(dbPath, activeManifestName)

	// Validate manifest file exists
	if _, err := os.Stat(activeManifestPath); err != nil {
		errNote := fmt.Sprintf("CURRENT points to missing file: %s (%v)", activeManifestName, err)
		_ = os.WriteFile(filepath.Join(artifactDir, "ldb_manifest_dump.txt"),
			[]byte("Skipped: "+errNote), 0644)
		t.Logf("⚠️  Skipping manifest_dump: %s", errNote)
		return
	}

	// Run ldb manifest_dump
	runOracleTool(t, artifactDir, "ldb_manifest_dump.txt", ldbPath,
		"manifest_dump", "--db="+dbPath, "--path="+activeManifestPath)
}

// runOracleTool runs a C++ oracle tool and saves output to artifactDir.
func runOracleTool(t *testing.T, artifactDir, outputFile, toolPath string, args ...string) {
	t.Helper()

	cmd := exec.Command(toolPath, args...)
	cmd.Env = oracleToolEnv(filepath.Dir(toolPath))
	output, err := cmd.CombinedOutput()

	outPath := filepath.Join(artifactDir, outputFile)
	if err != nil {
		content := fmt.Sprintf("Command: %s %s\nError: %v\nOutput:\n%s",
			toolPath, strings.Join(args, " "), err, output)
		_ = os.WriteFile(outPath, []byte(content), 0644)
		t.Logf("⚠️  Oracle tool failed: %s (%v)", outputFile, err)
	} else {
		content := fmt.Sprintf("Command: %s %s\nOutput:\n%s",
			toolPath, strings.Join(args, " "), output)
		_ = os.WriteFile(outPath, []byte(content), 0644)
		t.Logf("✓  Oracle tool succeeded: %s", outputFile)
	}
}

// oracleToolEnv builds an environment for invoking C++ oracle tools (ldb/sst_dump),
// ensuring the tool directory is on the dynamic linker path and optionally adding
// a dependency library directory via ROCKSDB_DEPS_LIBDIR.
//
// This is required on macOS when the oracle tools are linked against
// librocksdb_tools.dylib (or compression deps) that are not in a default search path.
func oracleToolEnv(toolDir string) []string {
	env := os.Environ()
	depsDir := strings.TrimSpace(os.Getenv("ROCKSDB_DEPS_LIBDIR"))

	if toolDir != "" {
		env = append(env,
			"DYLD_LIBRARY_PATH="+joinPathList(toolDir, depsDir, os.Getenv("DYLD_LIBRARY_PATH")),
			"LD_LIBRARY_PATH="+joinPathList(toolDir, depsDir, os.Getenv("LD_LIBRARY_PATH")),
		)
	}

	return env
}

func joinPathList(parts ...string) string {
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, ":")
}
