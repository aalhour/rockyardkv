package rockyardkv

// blobdb.go defines BlobDB option types and blob-value helpers.
//
// Contract: The presence of BlobDB option types does not imply BlobDB is enabled in the DB.
// Blob file management is internal machinery; DB integration is gated by the DB implementation.
//
// Reference: RocksDB v10.7.5
//   - db/blob/blob_file_builder.h
//   - db/blob/blob_source.h
//   - include/rocksdb/advanced_options.h (blob_options)

import (
	"github.com/aalhour/rockyardkv/internal/blob"
)

// BlobDBOptions configures BlobDB behavior.
// These are user-facing configuration knobs.
//
// Reference: RocksDB v10.7.5 include/rocksdb/advanced_options.h
type BlobDBOptions struct {
	// Enable enables storing large values in blob files
	Enable bool

	// MinBlobSize is the minimum value size to store in a blob file (bytes)
	MinBlobSize int

	// BlobFileSize is the target size for blob files (bytes)
	BlobFileSize int64

	// BlobCompressionType is the compression algorithm for blob files
	BlobCompressionType CompressionType

	// EnableBlobGC enables garbage collection of unreferenced blobs
	EnableBlobGC bool

	// BlobGCAgeCutoff is the age fraction of files to consider for GC (0.0 to 1.0)
	BlobGCAgeCutoff float64
}

// DefaultBlobDBOptions returns sensible defaults for BlobDB.
func DefaultBlobDBOptions() BlobDBOptions {
	return BlobDBOptions{
		Enable:              false,
		MinBlobSize:         4096,              // 4KB
		BlobFileSize:        256 * 1024 * 1024, // 256MB
		BlobCompressionType: CompressionNone,
		EnableBlobGC:        true,
		BlobGCAgeCutoff:     0.25,
	}
}

// IsBlobValue checks if a value is a blob index (reference to blob file).
func IsBlobValue(value []byte) bool {
	return blob.IsBlobIndex(value)
}
