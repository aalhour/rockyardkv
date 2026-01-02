// sst_format_test.go - Table-driven SST format compatibility tests.
//
// Tests the SST format contract: Go can write/read SST files that are
// compatible with C++ RocksDB across format versions and compression types.
//
// Reference: RocksDB v10.7.5
//
//	table/format.h         - Format version definitions
//	table/block_based_table_builder.cc - SST building
//	util/compression.h     - Compression types
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/dbformat"
	"github.com/aalhour/rockyardkv/internal/table"
	"github.com/aalhour/rockyardkv/vfs"
)

// =============================================================================
// Format Version × Compression Matrix Tests
// =============================================================================

// TestSST_FormatVersionMatrix tests all supported format versions.
//
// Contract: Go-written SST files are readable by C++ sst_dump for all
// supported format versions.
func TestSST_FormatVersionMatrix(t *testing.T) {
	sstDump := findSstDump(t)
	if sstDump == "" {
		t.Skip("sst_dump not found - build C++ RocksDB first")
	}

	versions := []struct {
		version uint32
		notes   string
	}{
		{3, "key delta encoding"},
		{4, "value delta encoding in index"},
		{5, "xxhash64 checksums"},
		{6, "context checksums, index in metaindex"},
	}

	for _, v := range versions {
		t.Run(fmt.Sprintf("v%d", v.version), func(t *testing.T) {
			dir := t.TempDir()
			sstPath := filepath.Join(dir, fmt.Sprintf("format_v%d.sst", v.version))

			// Write SST with this format version
			writeSST(t, sstPath, sst{
				formatVersion: v.version,
				compression:   compression.NoCompression,
				entries:       generateEntries("format_test", 20),
			})

			// Verify C++ can read it
			output := runSstDumpScan(t, sstDump, sstPath)
			assertContainsKeys(t, output, "format_test", 20)

			t.Logf("Format V%d (%s): OK", v.version, v.notes)
		})
	}
}

// TestSST_CompressionMatrix tests all supported compression types.
//
// Contract: Go-written compressed SST files are readable by C++ sst_dump.
func TestSST_CompressionMatrix(t *testing.T) {
	sstDump := findSstDump(t)
	if sstDump == "" {
		t.Skip("sst_dump not found - build C++ RocksDB first")
	}

	compressions := []struct {
		typ   compression.Type
		name  string
		notes string
	}{
		{compression.NoCompression, "none", "baseline"},
		{compression.SnappyCompression, "snappy", "fast compression"},
		{compression.ZstdCompression, "zstd", "high ratio"},
		{compression.LZ4Compression, "lz4", "very fast"},
		{compression.ZlibCompression, "zlib", "raw deflate (compress_format_version=2)"},
	}

	for _, c := range compressions {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			sstPath := filepath.Join(dir, fmt.Sprintf("comp_%s.sst", c.name))

			// Write SST with this compression
			writeSST(t, sstPath, sst{
				formatVersion: 6, // Latest format
				compression:   c.typ,
				blockSize:     256, // Small blocks to ensure compression kicks in
				entries:       generateRepetitiveEntries("comp_test", 50),
			})

			// Verify C++ can read it
			output := runSstDumpScan(t, sstDump, sstPath)

			// Check for compression not supported error
			if strings.Contains(output, "not supported") ||
				strings.Contains(output, "Not implemented") {
				t.Skipf("Compression %s not supported in C++ build", c.name)
			}

			assertContainsKeys(t, output, "comp_test", 50)
			t.Logf("Compression %s (%s): OK", c.name, c.notes)
		})
	}
}

// TestSST_FormatVersionCompressionMatrix tests the full matrix.
//
// Contract: All (format_version, compression) combinations produce valid SSTs.
func TestSST_FormatVersionCompressionMatrix(t *testing.T) {
	sstDump := findSstDump(t)
	if sstDump == "" {
		t.Skip("sst_dump not found - build C++ RocksDB first")
	}

	versions := []uint32{3, 4, 5, 6}
	compressions := []compression.Type{
		compression.NoCompression,
		compression.SnappyCompression,
		compression.ZstdCompression,
	}

	for _, version := range versions {
		for _, comp := range compressions {
			name := fmt.Sprintf("v%d_%s", version, compressionName(comp))
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				sstPath := filepath.Join(dir, name+".sst")

				writeSST(t, sstPath, sst{
					formatVersion: version,
					compression:   comp,
					blockSize:     256,
					entries:       generateRepetitiveEntries("matrix", 30),
				})

				output := runSstDumpScan(t, sstDump, sstPath)
				if strings.Contains(output, "not supported") {
					t.Skipf("Combination not supported in C++ build")
				}

				assertContainsKeys(t, output, "matrix", 30)
			})
		}
	}
}

// =============================================================================
// Go Round-Trip Tests (no C++ dependency)
// =============================================================================

// TestSST_RoundTrip_FormatVersions tests Go can read its own SST files.
//
// Contract: Go-written SSTs are readable by Go across all format versions.
func TestSST_RoundTrip_FormatVersions(t *testing.T) {
	versions := []uint32{3, 4, 5, 6}

	for _, version := range versions {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			entries := generateEntries("roundtrip", 10)

			// Write to memory buffer
			var buf bytes.Buffer
			opts := table.DefaultBuilderOptions()
			opts.FormatVersion = version

			builder := table.NewTableBuilder(&buf, opts)
			for _, e := range entries {
				if err := builder.Add(e.internalKey, e.value); err != nil {
					t.Fatalf("add failed: %v", err)
				}
			}
			if err := builder.Finish(); err != nil {
				t.Fatalf("finish failed: %v", err)
			}

			// Read back
			data := buf.Bytes()
			reader, err := table.Open(&memFile{data}, table.ReaderOptions{VerifyChecksums: true})
			if err != nil {
				t.Fatalf("open failed: %v", err)
			}
			defer reader.Close()

			// Verify format version
			footer := reader.Footer()
			if footer.FormatVersion != version {
				t.Errorf("format version: got %d, want %d", footer.FormatVersion, version)
			}

			// Verify entries
			iter := reader.NewIterator()
			idx := 0
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				parsed, err := dbformat.ParseInternalKey(iter.Key())
				if err != nil {
					t.Fatalf("parse key failed: %v", err)
				}

				wantKey := entries[idx].userKey
				if string(parsed.UserKey) != wantKey {
					t.Errorf("key %d: got %q, want %q", idx, parsed.UserKey, wantKey)
				}
				idx++
			}

			if err := iter.Error(); err != nil {
				t.Fatalf("iterator error: %v", err)
			}

			if idx != len(entries) {
				t.Errorf("count: got %d, want %d", idx, len(entries))
			}
		})
	}
}

// TestSST_RoundTrip_Compression tests Go can read its own compressed SSTs.
//
// Contract: Go-written compressed SSTs are readable by Go.
func TestSST_RoundTrip_Compression(t *testing.T) {
	compressions := []compression.Type{
		compression.NoCompression,
		compression.SnappyCompression,
		compression.ZstdCompression,
		compression.LZ4Compression,
		compression.ZlibCompression,
	}

	for _, comp := range compressions {
		t.Run(compressionName(comp), func(t *testing.T) {
			dir := t.TempDir()
			sstPath := filepath.Join(dir, "test.sst")

			entries := generateRepetitiveEntries("compress_rt", 30)
			writeSST(t, sstPath, sst{
				formatVersion: 6,
				compression:   comp,
				blockSize:     256,
				entries:       entries,
			})

			// Read back
			fs := vfs.Default()
			file, err := fs.OpenRandomAccess(sstPath)
			if err != nil {
				t.Fatalf("open failed: %v", err)
			}
			defer file.Close()

			reader, err := table.Open(file, table.ReaderOptions{VerifyChecksums: true})
			if err != nil {
				t.Fatalf("reader failed: %v", err)
			}
			defer reader.Close()

			// Verify all entries
			iter := reader.NewIterator()
			count := 0
			for iter.SeekToFirst(); iter.Valid(); iter.Next() {
				count++
			}

			if err := iter.Error(); err != nil {
				t.Fatalf("iterator error: %v", err)
			}

			if count != len(entries) {
				t.Errorf("count: got %d, want %d", count, len(entries))
			}
		})
	}
}

// =============================================================================
// Helpers
// =============================================================================

// sst defines parameters for writing an SST file.
type sst struct {
	formatVersion uint32
	compression   compression.Type
	blockSize     int
	entries       []entry
}

// entry represents a key-value pair.
type entry struct {
	userKey     string
	value       []byte
	internalKey []byte
}

// generateEntries creates n entries with a given prefix.
func generateEntries(prefix string, n int) []entry {
	entries := make([]entry, n)
	for i := range n {
		userKey := fmt.Sprintf("%s_%04d", prefix, i)
		value := fmt.Appendf(nil, "value_%04d", i)
		entries[i] = entry{
			userKey:     userKey,
			value:       value,
			internalKey: dbformat.NewInternalKey([]byte(userKey), dbformat.SequenceNumber(i+1), dbformat.TypeValue),
		}
	}
	return entries
}

// generateRepetitiveEntries creates entries with repetitive data for better compression.
func generateRepetitiveEntries(prefix string, n int) []entry {
	entries := make([]entry, n)
	for i := range n {
		userKey := fmt.Sprintf("%s_%04d", prefix, i)
		value := fmt.Appendf(nil, "value_%04d_repeated_data_for_compression_ratio", i)
		entries[i] = entry{
			userKey:     userKey,
			value:       value,
			internalKey: dbformat.NewInternalKey([]byte(userKey), dbformat.SequenceNumber(i+1), dbformat.TypeValue),
		}
	}
	return entries
}

// writeSST writes an SST file with the given parameters.
func writeSST(t *testing.T, path string, s sst) {
	t.Helper()

	fs := vfs.Default()
	file, err := fs.Create(path)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}

	opts := table.DefaultBuilderOptions()
	opts.FormatVersion = s.formatVersion
	opts.Compression = s.compression
	if s.blockSize > 0 {
		opts.BlockSize = s.blockSize
	}

	builder := table.NewTableBuilder(file, opts)

	for _, e := range s.entries {
		if err := builder.Add(e.internalKey, e.value); err != nil {
			file.Close()
			t.Fatalf("add entry: %v", err)
		}
	}

	if err := builder.Finish(); err != nil {
		file.Close()
		t.Fatalf("finish: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// findSstDump locates the sst_dump binary.
func findSstDump(t *testing.T) string {
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

// runSstDumpScan runs sst_dump --command=scan and returns output.
func runSstDumpScan(t *testing.T, sstDump, sstPath string) string {
	t.Helper()

	cmd := exec.Command(sstDump, "--file="+sstPath, "--command=scan")
	dir := filepath.Dir(sstDump)
	cmd.Env = toolEnv(dir)

	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), "Library not loaded") ||
			strings.Contains(string(output), "dylib") {
			t.Skipf("C++ RocksDB tools not properly built: %s", output)
		}
		t.Fatalf("sst_dump failed: %v\nOutput: %s", err, output)
	}

	return string(output)
}

// assertContainsKeys verifies the output contains keys with the given prefix.
func assertContainsKeys(t *testing.T, output, prefix string, n int) {
	t.Helper()

	// Check first and last keys
	firstKey := fmt.Sprintf("%s_%04d", prefix, 0)
	lastKey := fmt.Sprintf("%s_%04d", prefix, n-1)

	missing := false
	if !strings.Contains(output, firstKey) {
		t.Errorf("missing first key: %s", firstKey)
		missing = true
	}
	if !strings.Contains(output, lastKey) {
		t.Errorf("missing last key: %s", lastKey)
		missing = true
	}
	if missing {
		const max = 2000
		snippet := output
		if len(snippet) > max {
			snippet = snippet[:max] + "\n... (truncated) ..."
		}
		t.Logf("sst_dump output (prefix=%q, n=%d):\n%s", prefix, n, snippet)
	}
}

// compressionName returns a human-readable name for a compression type.
func compressionName(c compression.Type) string {
	switch c {
	case compression.NoCompression:
		return "none"
	case compression.SnappyCompression:
		return "snappy"
	case compression.ZstdCompression:
		return "zstd"
	case compression.LZ4Compression:
		return "lz4"
	case compression.ZlibCompression:
		return "zlib"
	default:
		return fmt.Sprintf("unknown_%d", c)
	}
}

// memFile implements vfs.RandomAccessFile for in-memory data.
type memFile struct {
	data []byte
}

func (m *memFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, nil
	}
	return copy(p, m.data[off:]), nil
}

func (m *memFile) Size() int64 {
	return int64(len(m.data))
}

func (m *memFile) Close() error {
	return nil
}
