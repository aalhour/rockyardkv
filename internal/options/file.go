// Package options implements OPTIONS file parsing for database configuration.
//
// This package is internal and not part of the public API.
//
// Reference: RocksDB v10.7.5
//   - options/options_helper.cc
//   - options/db_options.cc
package options

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/vfs"
)

// CompactionStyle represents the compaction strategy.
// This mirrors the root package's CompactionStyle type.
type CompactionStyle int

const (
	CompactionStyleLevel CompactionStyle = iota
	CompactionStyleUniversal
	CompactionStyleFIFO
)

// ParsedOptions represents options parsed from an OPTIONS file.
type ParsedOptions struct {
	RocksDBVersion                 string
	OptionsFileVersion             int
	MaxOpenFiles                   int
	WriteBufferSize                int64
	MaxWriteBufferNumber           int
	Level0FileNumCompactionTrigger int
	Level0SlowdownWritesTrigger    int
	Level0StopWritesTrigger        int
	MaxBytesForLevelBase           int64
	MaxBytesForLevelMultiplier     float64
	TargetFileSizeBase             int64
	TargetFileSizeMultiplier       int
	NumLevels                      int
	Compression                    compression.Type
	CompactionStyle                CompactionStyle
	MaxSubcompactions              int
}

// ReadOptionsFile reads and parses an OPTIONS file.
func ReadOptionsFile(fs vfs.FS, path string) (*ParsedOptions, error) {
	file, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	return ParseOptionsFile(file)
}

// ParseOptionsFile parses options from a reader.
func ParseOptionsFile(r io.Reader) (*ParsedOptions, error) {
	opts := &ParsedOptions{
		// Set defaults
		MaxOpenFiles:                   5000,
		WriteBufferSize:                64 * 1024 * 1024,
		MaxWriteBufferNumber:           2,
		Level0FileNumCompactionTrigger: 4,
		Level0SlowdownWritesTrigger:    20,
		Level0StopWritesTrigger:        36,
		MaxBytesForLevelBase:           256 * 1024 * 1024,
		Compression:                    compression.NoCompression,
		CompactionStyle:                CompactionStyleLevel,
		MaxSubcompactions:              1,
	}

	scanner := bufio.NewScanner(r)
	currentSection := ""

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Check for section header
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = line[1 : len(line)-1]
			continue
		}

		// Parse key=value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Handle based on section
		switch {
		case currentSection == "Version":
			switch key {
			case "rocksdb_version":
				opts.RocksDBVersion = value
			case "options_file_version":
				opts.OptionsFileVersion, _ = strconv.Atoi(value)
			}

		case currentSection == "DBOptions":
			switch key {
			case "max_open_files":
				opts.MaxOpenFiles, _ = strconv.Atoi(value)
			case "write_buffer_size":
				opts.WriteBufferSize, _ = strconv.ParseInt(value, 10, 64)
			case "max_write_buffer_number":
				opts.MaxWriteBufferNumber, _ = strconv.Atoi(value)
			case "level0_file_num_compaction_trigger":
				opts.Level0FileNumCompactionTrigger, _ = strconv.Atoi(value)
			case "level0_slowdown_writes_trigger":
				opts.Level0SlowdownWritesTrigger, _ = strconv.Atoi(value)
			case "level0_stop_writes_trigger":
				opts.Level0StopWritesTrigger, _ = strconv.Atoi(value)
			case "max_bytes_for_level_base":
				opts.MaxBytesForLevelBase, _ = strconv.ParseInt(value, 10, 64)
			case "max_bytes_for_level_multiplier":
				opts.MaxBytesForLevelMultiplier, _ = strconv.ParseFloat(value, 64)
			case "target_file_size_base":
				opts.TargetFileSizeBase, _ = strconv.ParseInt(value, 10, 64)
			case "target_file_size_multiplier":
				opts.TargetFileSizeMultiplier, _ = strconv.Atoi(value)
			case "num_levels":
				opts.NumLevels, _ = strconv.Atoi(value)
			case "compression":
				opts.Compression = StringToCompressionType(value)
			case "compaction_style":
				opts.CompactionStyle = StringToCompactionStyle(value)
			case "max_subcompactions":
				opts.MaxSubcompactions, _ = strconv.Atoi(value)
			}

		case strings.HasPrefix(currentSection, "CFOptions"):
			// Column family options (handled similarly)
			switch key {
			case "write_buffer_size":
				opts.WriteBufferSize, _ = strconv.ParseInt(value, 10, 64)
			case "compression":
				opts.Compression = StringToCompressionType(value)
			}
		}
	}

	return opts, scanner.Err()
}

// StringToCompressionType converts a string to compression.Type.
func StringToCompressionType(s string) compression.Type {
	switch s {
	case "kNoCompression":
		return compression.NoCompression
	case "kSnappyCompression":
		return compression.SnappyCompression
	case "kZlibCompression":
		return compression.ZlibCompression
	case "kLZ4Compression":
		return compression.LZ4Compression
	case "kLZ4HCCompression":
		return compression.LZ4HCCompression
	case "kZSTD":
		return compression.ZstdCompression
	default:
		return compression.NoCompression
	}
}

// StringToCompactionStyle converts a string to CompactionStyle.
func StringToCompactionStyle(s string) CompactionStyle {
	switch s {
	case "kCompactionStyleLevel":
		return CompactionStyleLevel
	case "kCompactionStyleUniversal":
		return CompactionStyleUniversal
	case "kCompactionStyleFIFO":
		return CompactionStyleFIFO
	default:
		return CompactionStyleLevel
	}
}
