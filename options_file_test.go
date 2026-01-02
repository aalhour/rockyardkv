package rockyardkv

// options_file_test.go implements tests for options file.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/options"
	"github.com/aalhour/rockyardkv/vfs"
)

func TestWriteAndReadOptionsFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "options-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	fs := vfs.Default()

	// Create options with non-default values
	opts := DefaultOptions()
	opts.MaxOpenFiles = 1000
	opts.WriteBufferSize = 128 * 1024 * 1024
	opts.MaxWriteBufferNumber = 4
	opts.Compression = compression.LZ4Compression
	opts.CompactionStyle = CompactionStyleUniversal
	opts.MaxSubcompactions = 4

	// Write options file
	err = WriteOptionsFile(fs, dir, opts, 1)
	if err != nil {
		t.Fatalf("WriteOptionsFile failed: %v", err)
	}

	// Read options file
	path := filepath.Join(dir, "OPTIONS-000001")
	parsed, err := readOptionsFile(fs, path)
	if err != nil {
		t.Fatalf("readOptionsFile failed: %v", err)
	}

	// Verify values
	if parsed.RocksDBVersion != "10.7.5" {
		t.Errorf("RocksDBVersion = %s, want 10.7.5", parsed.RocksDBVersion)
	}
	if parsed.OptionsFileVersion != 1 {
		t.Errorf("OptionsFileVersion = %d, want 1", parsed.OptionsFileVersion)
	}
	if parsed.MaxOpenFiles != opts.MaxOpenFiles {
		t.Errorf("MaxOpenFiles = %d, want %d", parsed.MaxOpenFiles, opts.MaxOpenFiles)
	}
	if parsed.WriteBufferSize != int64(opts.WriteBufferSize) {
		t.Errorf("WriteBufferSize = %d, want %d", parsed.WriteBufferSize, opts.WriteBufferSize)
	}
	if parsed.MaxWriteBufferNumber != opts.MaxWriteBufferNumber {
		t.Errorf("MaxWriteBufferNumber = %d, want %d", parsed.MaxWriteBufferNumber, opts.MaxWriteBufferNumber)
	}
	if parsed.Compression != opts.Compression {
		t.Errorf("Compression = %d, want %d", parsed.Compression, opts.Compression)
	}
	if int(parsed.CompactionStyle) != int(opts.CompactionStyle) {
		t.Errorf("CompactionStyle = %d, want %d", parsed.CompactionStyle, opts.CompactionStyle)
	}
	if parsed.MaxSubcompactions != opts.MaxSubcompactions {
		t.Errorf("MaxSubcompactions = %d, want %d", parsed.MaxSubcompactions, opts.MaxSubcompactions)
	}
}

func TestParseOptionsFile(t *testing.T) {
	input := `
[Version]
  rocksdb_version=10.7.5
  options_file_version=1

[DBOptions]
  max_open_files=2000
  write_buffer_size=67108864
  compression=kZSTD
  compaction_style=kCompactionStyleFIFO

[CFOptions "default"]
  write_buffer_size=33554432
`

	parsed, err := options.ParseOptionsFile(strings.NewReader(input))
	if err != nil {
		t.Fatalf("ParseOptionsFile failed: %v", err)
	}

	if parsed.RocksDBVersion != "10.7.5" {
		t.Errorf("RocksDBVersion = %s, want 10.7.5", parsed.RocksDBVersion)
	}
	if parsed.MaxOpenFiles != 2000 {
		t.Errorf("MaxOpenFiles = %d, want 2000", parsed.MaxOpenFiles)
	}
	if parsed.Compression != compression.ZstdCompression {
		t.Errorf("Compression = %d, want %d", parsed.Compression, compression.ZstdCompression)
	}
	if parsed.CompactionStyle != options.CompactionStyleFIFO {
		t.Errorf("CompactionStyle = %d, want %d", parsed.CompactionStyle, options.CompactionStyleFIFO)
	}
}

func TestGetLatestOptionsFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "options-latest-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	fs := vfs.Default()
	opts := DefaultOptions()

	// Write multiple options files
	for i := uint64(1); i <= 5; i++ {
		err = WriteOptionsFile(fs, dir, opts, i)
		if err != nil {
			t.Fatalf("WriteOptionsFile failed: %v", err)
		}
	}

	// Get latest
	latest, err := GetLatestOptionsFile(fs, dir)
	if err != nil {
		t.Fatalf("GetLatestOptionsFile failed: %v", err)
	}

	expected := filepath.Join(dir, "OPTIONS-000005")
	if latest != expected {
		t.Errorf("Latest = %s, want %s", latest, expected)
	}
}

func TestCompressionTypeConversions(t *testing.T) {
	tests := []struct {
		ct compression.Type
		s  string
	}{
		{compression.NoCompression, "kNoCompression"},
		{compression.SnappyCompression, "kSnappyCompression"},
		{compression.ZlibCompression, "kZlibCompression"},
		{compression.LZ4Compression, "kLZ4Compression"},
		{compression.LZ4HCCompression, "kLZ4HCCompression"},
		{compression.ZstdCompression, "kZSTD"},
	}

	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			s := compressionTypeToString(tt.ct)
			if s != tt.s {
				t.Errorf("compressionTypeToString(%d) = %s, want %s", tt.ct, s, tt.s)
			}
			ct := options.StringToCompressionType(tt.s)
			if ct != tt.ct {
				t.Errorf("StringToCompressionType(%s) = %d, want %d", tt.s, ct, tt.ct)
			}
		})
	}
}

func TestCompactionStyleConversions(t *testing.T) {
	tests := []struct {
		cs CompactionStyle
		s  string
	}{
		{CompactionStyleLevel, "kCompactionStyleLevel"},
		{CompactionStyleUniversal, "kCompactionStyleUniversal"},
		{CompactionStyleFIFO, "kCompactionStyleFIFO"},
	}

	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			s := compactionStyleToString(tt.cs)
			if s != tt.s {
				t.Errorf("compactionStyleToString(%d) = %s, want %s", tt.cs, s, tt.s)
			}
			cs := options.StringToCompactionStyle(tt.s)
			if int(cs) != int(tt.cs) {
				t.Errorf("StringToCompactionStyle(%s) = %d, want %d", tt.s, cs, tt.cs)
			}
		})
	}
}
