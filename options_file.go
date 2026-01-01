package rockyardkv

// options_file.go implements OPTIONS file persistence.
//
// RocksDB stores database configuration in OPTIONS files for recovery.
// The file format is a simple text file with sections and key=value pairs.
//
// Format:
//
//	[Version]
//	rocksdb_version=10.7.5
//	options_file_version=1
//
//	[DBOptions]
//	max_open_files=5000
//	...
//
//	[CFOptions "default"]
//	...
//
// Reference: RocksDB v10.7.5
//   - options/options_helper.cc
//   - options/db_options.cc


import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

const (
	// OptionsFileVersion is the current options file format version
	OptionsFileVersion = 1

	// OptionsFilePrefix is the prefix for options file names
	OptionsFilePrefix = "OPTIONS-"
)

// WriteOptionsFile writes the current options to an OPTIONS file.
func WriteOptionsFile(fs vfs.FS, dbPath string, opts *Options, fileNum uint64) error {
	path := fmt.Sprintf("%s/%s%06d", dbPath, OptionsFilePrefix, fileNum)

	file, err := fs.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	w := bufio.NewWriter(file)

	// Write version section
	fmt.Fprintln(w, "[Version]")
	fmt.Fprintln(w, "  rocksdb_version=10.7.5")
	fmt.Fprintf(w, "  options_file_version=%d\n", OptionsFileVersion)
	fmt.Fprintln(w)

	// Write DBOptions section
	fmt.Fprintln(w, "[DBOptions]")
	fmt.Fprintf(w, "  max_open_files=%d\n", opts.MaxOpenFiles)
	fmt.Fprintf(w, "  write_buffer_size=%d\n", opts.WriteBufferSize)
	fmt.Fprintf(w, "  max_write_buffer_number=%d\n", opts.MaxWriteBufferNumber)
	fmt.Fprintf(w, "  level0_file_num_compaction_trigger=%d\n", opts.Level0FileNumCompactionTrigger)
	fmt.Fprintf(w, "  level0_slowdown_writes_trigger=%d\n", opts.Level0SlowdownWritesTrigger)
	fmt.Fprintf(w, "  level0_stop_writes_trigger=%d\n", opts.Level0StopWritesTrigger)
	fmt.Fprintf(w, "  max_bytes_for_level_base=%d\n", opts.MaxBytesForLevelBase)
	fmt.Fprintf(w, "  compression=%s\n", compressionTypeToString(opts.Compression))
	fmt.Fprintf(w, "  compaction_style=%s\n", compactionStyleToString(opts.CompactionStyle))
	fmt.Fprintf(w, "  max_subcompactions=%d\n", opts.MaxSubcompactions)
	fmt.Fprintln(w)

	// Write default CF options
	fmt.Fprintln(w, "[CFOptions \"default\"]")
	fmt.Fprintf(w, "  write_buffer_size=%d\n", opts.WriteBufferSize)
	fmt.Fprintf(w, "  compression=%s\n", compressionTypeToString(opts.Compression))
	fmt.Fprintln(w)

	if err := w.Flush(); err != nil {
		return err
	}

	return file.Sync()
}

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
				opts.Compression = stringToCompressionType(value)
			case "compaction_style":
				opts.CompactionStyle = stringToCompactionStyle(value)
			case "max_subcompactions":
				opts.MaxSubcompactions, _ = strconv.Atoi(value)
			}

		case strings.HasPrefix(currentSection, "CFOptions"):
			// Column family options (handled similarly)
			switch key {
			case "write_buffer_size":
				opts.WriteBufferSize, _ = strconv.ParseInt(value, 10, 64)
			case "compression":
				opts.Compression = stringToCompressionType(value)
			}
		}
	}

	return opts, scanner.Err()
}

// Helper functions for type conversions

func compressionTypeToString(t compression.Type) string {
	switch t {
	case compression.NoCompression:
		return "kNoCompression"
	case compression.SnappyCompression:
		return "kSnappyCompression"
	case compression.ZlibCompression:
		return "kZlibCompression"
	case compression.LZ4Compression:
		return "kLZ4Compression"
	case compression.LZ4HCCompression:
		return "kLZ4HCCompression"
	case compression.ZstdCompression:
		return "kZSTD"
	default:
		return "kNoCompression"
	}
}

func stringToCompressionType(s string) compression.Type {
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

func compactionStyleToString(s CompactionStyle) string {
	switch s {
	case CompactionStyleLevel:
		return "kCompactionStyleLevel"
	case CompactionStyleUniversal:
		return "kCompactionStyleUniversal"
	case CompactionStyleFIFO:
		return "kCompactionStyleFIFO"
	default:
		return "kCompactionStyleLevel"
	}
}

func stringToCompactionStyle(s string) CompactionStyle {
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

// GetLatestOptionsFile finds the latest OPTIONS file in the database directory.
func GetLatestOptionsFile(fs vfs.FS, dbPath string) (string, error) {
	entries, err := fs.ListDir(dbPath)
	if err != nil {
		return "", err
	}

	var latestFile string
	var latestNum uint64

	for _, entry := range entries {
		if !strings.HasPrefix(entry, OptionsFilePrefix) {
			continue
		}

		numStr := entry[len(OptionsFilePrefix):]
		num, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			continue
		}

		if num > latestNum {
			latestNum = num
			latestFile = entry
		}
	}

	if latestFile == "" {
		return "", fmt.Errorf("no OPTIONS file found")
	}

	return dbPath + "/" + latestFile, nil
}
