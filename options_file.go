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
	"strconv"
	"strings"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/options"
	"github.com/aalhour/rockyardkv/vfs"
)

// parsedOptions is an alias to the internal options package type.
type parsedOptions = options.ParsedOptions

// readOptionsFile delegates to the internal options package.
func readOptionsFile(fs vfs.FS, path string) (*parsedOptions, error) {
	return options.ReadOptionsFile(fs, path)
}

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

// Helper functions for type conversions (used by WriteOptionsFile)

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
