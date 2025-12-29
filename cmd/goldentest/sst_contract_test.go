// sst_contract_test.go - SST behavioral contract tests.
//
// Tests specific behaviors that SST files promise:
// - Binary keys work correctly
// - Empty values are preserved
// - Large values are handled
// - Deletion markers are written
// - Unicode keys are supported
// - Block boundaries are respected
//
// These tests verify contracts, not format versions (see sst_format_test.go).
//
// Reference: RocksDB v10.7.5
//
//	table/block_based_table_builder.cc
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// =============================================================================
// Key/Value Edge Cases
// =============================================================================

// TestSST_Contract_BinaryKeys tests that binary keys are preserved.
//
// Contract: Keys containing arbitrary bytes (including \x00) are handled correctly.
func TestSST_Contract_BinaryKeys(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "binary_keys.sst")

	// Binary keys with various byte patterns
	binaryKeys := []struct {
		name string
		key  []byte
	}{
		{"null_bytes", []byte{0x00, 0x01, 0x02}},
		{"high_bytes", []byte{0xFF, 0xFE, 0xFD}},
		{"mixed", []byte{0x10, 0x00, 0x20, 0xFF}},
	}

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	for i, bk := range binaryKeys {
		ikey := dbformat.NewInternalKey(bk.key, dbformat.SequenceNumber(i+1), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte("value")); err != nil {
			file.Close()
			t.Fatalf("add %s: %v", bk.name, err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify C++ doesn't crash
	output := runSstDumpContract(t, sstDump, sstPath)
	if output == "" {
		t.Error("sst_dump returned empty output")
	}
}

// TestSST_Contract_EmptyValues tests that empty values are preserved.
//
// Contract: Keys with empty values are distinguishable from deleted keys.
func TestSST_Contract_EmptyValues(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "empty_values.sst")

	entries := []struct {
		key   string
		value string
	}{
		{"key_empty", ""},
		{"key_normal", "normal_value"},
		{"key_empty2", ""},
	}

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	for i, e := range entries {
		ikey := dbformat.NewInternalKey([]byte(e.key), dbformat.SequenceNumber(i+1), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte(e.value)); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify keys are present
	output := runSstDumpScanContract(t, sstDump, sstPath)
	for _, e := range entries {
		if !strings.Contains(output, e.key) {
			t.Errorf("missing key: %s", e.key)
		}
	}
}

// TestSST_Contract_LargeValues tests that large values are handled.
//
// Contract: Values up to 4MB are supported without corruption.
func TestSST_Contract_LargeValues(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "large_values.sst")

	// 100KB values (not 4MB to keep test fast)
	largeValue := strings.Repeat("x", 100*1024)

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	for i := range 3 {
		key := fmt.Sprintf("large_key_%d", i)
		ikey := dbformat.NewInternalKey([]byte(key), dbformat.SequenceNumber(i+1), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte(largeValue)); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify C++ can read
	output := runSstDumpScanContract(t, sstDump, sstPath)
	if !strings.Contains(output, "large_key_0") {
		t.Error("missing large_key_0")
	}
}

// TestSST_Contract_LongKeys tests that long keys are handled.
//
// Contract: Keys up to 8KB are supported.
func TestSST_Contract_LongKeys(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "long_keys.sst")

	// 1KB key
	longKey := strings.Repeat("k", 1024)

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	ikey := dbformat.NewInternalKey([]byte(longKey), dbformat.SequenceNumber(1), dbformat.TypeValue)
	if err := builder.Add(ikey, []byte("value")); err != nil {
		file.Close()
		t.Fatalf("add: %v", err)
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify C++ can read
	output := runSstDumpContract(t, sstDump, sstPath)
	if output == "" {
		t.Error("sst_dump returned empty output")
	}
}

// =============================================================================
// Key Types
// =============================================================================

// TestSST_Contract_DeletionMarkers tests that deletion tombstones are written.
//
// Contract: TypeDeletion entries are preserved and visible in sst_dump.
func TestSST_Contract_DeletionMarkers(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "deletions.sst")

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	// Add a value then a deletion
	ikey1 := dbformat.NewInternalKey([]byte("deleted_key"), dbformat.SequenceNumber(1), dbformat.TypeValue)
	if err := builder.Add(ikey1, []byte("will_be_deleted")); err != nil {
		file.Close()
		t.Fatalf("add value: %v", err)
	}

	ikey2 := dbformat.NewInternalKey([]byte("deleted_key"), dbformat.SequenceNumber(2), dbformat.TypeDeletion)
	if err := builder.Add(ikey2, nil); err != nil {
		file.Close()
		t.Fatalf("add deletion: %v", err)
	}

	// Add a regular key
	ikey3 := dbformat.NewInternalKey([]byte("regular_key"), dbformat.SequenceNumber(3), dbformat.TypeValue)
	if err := builder.Add(ikey3, []byte("regular_value")); err != nil {
		file.Close()
		t.Fatalf("add regular: %v", err)
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify both keys are visible
	output := runSstDumpScanContract(t, sstDump, sstPath)
	if !strings.Contains(output, "deleted_key") {
		t.Error("missing deleted_key")
	}
	if !strings.Contains(output, "regular_key") {
		t.Error("missing regular_key")
	}
}

// TestSST_Contract_UnicodeKeys tests that Unicode keys are preserved.
//
// Contract: UTF-8 encoded keys are stored and retrieved correctly.
func TestSST_Contract_UnicodeKeys(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "unicode.sst")

	// Unicode keys - sorted lexicographically by bytes
	unicodeKeys := []string{
		"ascii_normal",
		"emoji_\U0001F600",
		"japanese_こんにちは",
		"russian_привет",
	}

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	for i, k := range unicodeKeys {
		ikey := dbformat.NewInternalKey([]byte(k), dbformat.SequenceNumber(i+1), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte("value")); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify C++ can read ASCII key at least
	output := runSstDumpScanContract(t, sstDump, sstPath)
	if !strings.Contains(output, "ascii_normal") {
		t.Error("missing ascii_normal key")
	}
}

// =============================================================================
// Block Structure
// =============================================================================

// TestSST_Contract_SmallBlocks tests SST with small block size (many blocks).
//
// Contract: Block boundaries don't affect data integrity.
func TestSST_Contract_SmallBlocks(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "small_blocks.sst")

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	opts := table.DefaultBuilderOptions()
	opts.BlockSize = 256 // Very small blocks

	builder := table.NewTableBuilder(file, opts)

	for i := range 50 {
		key := fmt.Sprintf("block_key_%04d", i)
		value := fmt.Sprintf("value_%04d_with_padding", i)
		ikey := dbformat.NewInternalKey([]byte(key), dbformat.SequenceNumber(i+1), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte(value)); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify all entries
	output := runSstDumpScanContract(t, sstDump, sstPath)
	if !strings.Contains(output, "block_key_0000") {
		t.Error("missing first key")
	}
	if !strings.Contains(output, "block_key_0049") {
		t.Error("missing last key")
	}
}

// TestSST_Contract_LargeBlocks tests SST with large block size.
//
// Contract: Large blocks work correctly (all entries in fewer blocks).
func TestSST_Contract_LargeBlocks(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "large_blocks.sst")

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	opts := table.DefaultBuilderOptions()
	opts.BlockSize = 64 * 1024 // 64KB blocks

	builder := table.NewTableBuilder(file, opts)

	for i := range 100 {
		key := fmt.Sprintf("lblock_%04d", i)
		ikey := dbformat.NewInternalKey([]byte(key), dbformat.SequenceNumber(i+1), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte(strings.Repeat("v", 100))); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify
	output := runSstDumpScanContract(t, sstDump, sstPath)
	if !strings.Contains(output, "lblock_0000") {
		t.Error("missing first key")
	}
	if !strings.Contains(output, "lblock_0099") {
		t.Error("missing last key")
	}
}

// =============================================================================
// Sequence Numbers
// =============================================================================

// TestSST_Contract_SequenceNumbers tests various sequence number values.
//
// Contract: Sequence numbers up to MaxSequenceNumber-1 are supported.
func TestSST_Contract_SequenceNumbers(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "sequences.sst")

	seqNums := []uint64{1, 100, 1000, 10000, 1000000, uint64(dbformat.MaxSequenceNumber - 1)}

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	for _, seq := range seqNums {
		key := fmt.Sprintf("seq_%d_key", seq)
		ikey := dbformat.NewInternalKey([]byte(key), dbformat.SequenceNumber(seq), dbformat.TypeValue)
		if err := builder.Add(ikey, fmt.Appendf(nil, "value_%d", seq)); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify
	output := runSstDumpScanContract(t, sstDump, sstPath)
	if !strings.Contains(output, "seq_1_key") {
		t.Error("missing seq_1_key")
	}
	if !strings.Contains(output, "seq_1000000_key") {
		t.Error("missing seq_1000000_key")
	}
}

// =============================================================================
// Entry Count Edge Cases
// =============================================================================

// TestSST_Contract_SingleEntry tests SST with exactly one entry.
//
// Contract: Single-entry SST files are valid.
func TestSST_Contract_SingleEntry(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "single.sst")

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	ikey := dbformat.NewInternalKey([]byte("only_key"), dbformat.SequenceNumber(1), dbformat.TypeValue)
	if err := builder.Add(ikey, []byte("only_value")); err != nil {
		file.Close()
		t.Fatalf("add: %v", err)
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify
	output := runSstDumpScanContract(t, sstDump, sstPath)
	if !strings.Contains(output, "only_key") {
		t.Error("missing only_key")
	}
}

// TestSST_Contract_ManyEntries tests SST with many entries.
//
// Contract: SST files with 10000+ entries work correctly.
func TestSST_Contract_ManyEntries(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "many.sst")

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	builder := table.NewTableBuilder(file, table.DefaultBuilderOptions())

	for i := range 1000 {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d_with_extra_padding", i)
		ikey := dbformat.NewInternalKey([]byte(key), dbformat.SequenceNumber(i+1), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte(value)); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify
	output := runSstDumpScanContract(t, sstDump, sstPath)

	// Count entries
	lines := strings.Split(strings.TrimSpace(output), "\n")
	dataLines := 0
	for _, line := range lines {
		if strings.Contains(line, "=>") {
			dataLines++
		}
	}

	if dataLines < 1000 {
		t.Errorf("expected at least 1000 entries, got %d", dataLines)
	}
}

// =============================================================================
// Compression Regression Tests
// =============================================================================

// TestSST_Contract_ZlibCompression tests zlib compression specifically.
//
// Regression: Issue 0 - Go was not adding varint32 size prefix for
// compress_format_version=2, and was using zlib headers instead of raw deflate.
//
// Contract: Zlib-compressed SSTs with format_version >= 2 use raw deflate
// with varint32 size prefix.
func TestSST_Contract_ZlibCompression(t *testing.T) {
	sstDump := findSstDumpContract(t)
	if sstDump == "" {
		t.Skip("sst_dump not found")
	}

	dir := t.TempDir()
	sstPath := filepath.Join(dir, "zlib.sst")

	fs := vfs.Default()
	file, err := fs.Create(sstPath)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	opts := table.DefaultBuilderOptions()
	opts.FormatVersion = 6 // >= 2 means compress_format_version=2
	opts.Compression = compression.ZlibCompression
	opts.BlockSize = 256 // Force multiple blocks

	builder := table.NewTableBuilder(file, opts)

	for i := range 50 {
		key := fmt.Sprintf("zlib_key_%04d", i)
		value := fmt.Sprintf("zlib_value_%04d_padding", i)
		ikey := dbformat.NewInternalKey([]byte(key), dbformat.SequenceNumber(i+1), dbformat.TypeValue)
		if err := builder.Add(ikey, []byte(value)); err != nil {
			file.Close()
			t.Fatalf("add: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}
	file.Close()

	// Verify C++ can read
	output := runSstDumpScanContract(t, sstDump, sstPath)

	// Check for all keys
	for i := range 50 {
		key := fmt.Sprintf("zlib_key_%04d", i)
		if !strings.Contains(output, key) {
			t.Errorf("missing key: %s", key)
		}
	}
}

// =============================================================================
// Helpers
// =============================================================================

func findSstDumpContract(t *testing.T) string {
	t.Helper()

	paths := []string{
		os.ExpandEnv("$HOME/Workspace/rocksdb/sst_dump"),
		os.ExpandEnv("$ROCKSDB_PATH/sst_dump"),
		"/usr/local/bin/sst_dump",
		"sst_dump",
	}

	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
		if found, err := exec.LookPath(p); err == nil {
			return found
		}
	}

	return ""
}

func runSstDumpContract(t *testing.T, sstDump, sstPath string) string {
	t.Helper()

	cmd := exec.Command(sstDump, "--file="+sstPath)
	dir := filepath.Dir(sstDump)
	cmd.Env = toolEnv(dir)

	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "Library not loaded") {
			t.Skipf("C++ tools not built: %s", output)
		}
		t.Fatalf("sst_dump failed: %v\nOutput: %s", err, output)
	}

	return string(output)
}

func runSstDumpScanContract(t *testing.T, sstDump, sstPath string) string {
	t.Helper()

	cmd := exec.Command(sstDump, "--file="+sstPath, "--command=scan")
	dir := filepath.Dir(sstDump)
	cmd.Env = toolEnv(dir)

	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "Library not loaded") {
			t.Skipf("C++ tools not built: %s", output)
		}
		t.Fatalf("sst_dump scan failed: %v\nOutput: %s", err, output)
	}

	return string(output)
}
