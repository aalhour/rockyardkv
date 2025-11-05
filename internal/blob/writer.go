// writer.go implements blob file writing for BlobDB.
//
// BlobFileBuilder in RocksDB writes blob files containing large values
// that are stored separately from the LSM tree. This reduces write
// amplification for workloads with large values.
//
// Reference: RocksDB v10.7.5
//   - db/blob/blob_file_builder.h
//   - db/blob/blob_file_builder.cc
package blob

import (
	"io"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/internal/vfs"
)

// Writer writes blob records to a blob file.
type Writer struct {
	file            vfs.WritableFile
	header          *Header
	compressionType compression.Type

	// Current position in the file
	offset uint64

	// Statistics
	blobCount uint64
}

// WriterOptions configures the blob writer
type WriterOptions struct {
	ColumnFamilyID  uint32
	CompressionType compression.Type
}

// DefaultWriterOptions returns default writer options
func DefaultWriterOptions() WriterOptions {
	return WriterOptions{
		ColumnFamilyID:  0,
		CompressionType: compression.NoCompression,
	}
}

// NewWriter creates a new blob file writer
func NewWriter(file vfs.WritableFile, opts WriterOptions) (*Writer, error) {
	w := &Writer{
		file:            file,
		compressionType: opts.CompressionType,
		header: &Header{
			Magic:           MagicNumber,
			Version:         CurrentVersion,
			ColumnFamilyID:  opts.ColumnFamilyID,
			CompressionType: opts.CompressionType,
		},
	}

	// Write header
	if err := w.header.Encode(file); err != nil {
		return nil, err
	}
	w.offset = HeaderSize

	return w, nil
}

// AddBlob writes a blob record and returns its index
func (w *Writer) AddBlob(key, value []byte) (*BlobIndex, error) {
	// Compress value if compression is enabled
	var compressedValue []byte
	var err error

	if w.compressionType != compression.NoCompression {
		compressedValue, err = compression.Compress(w.compressionType, value)
		if err != nil {
			// Fall back to uncompressed if compression fails
			compressedValue = value
		}
	} else {
		compressedValue = value
	}

	// Create blob record
	record := &BlobRecord{
		Key:   key,
		Value: compressedValue,
	}

	// Record offset before writing
	startOffset := w.offset

	// Write the record
	if err := record.Encode(w.file); err != nil {
		return nil, err
	}

	recordSize := uint64(record.Size())
	w.offset += recordSize
	w.blobCount++

	return &BlobIndex{
		FileNumber: 0, // Set by caller
		Offset:     startOffset,
		Size:       recordSize,
	}, nil
}

// Finish finalizes the blob file by writing the footer
func (w *Writer) Finish() error {
	footer := &Footer{
		BlobCount:     w.blobCount,
		ExpirationMin: 0,
		ExpirationMax: 0,
		Magic:         MagicNumber,
	}

	if err := footer.Encode(w.file); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	return nil
}

// Close finishes and closes the blob file
func (w *Writer) Close() error {
	if err := w.Finish(); err != nil {
		return err
	}
	return w.file.Close()
}

// BlobCount returns the number of blobs written
func (w *Writer) BlobCount() uint64 {
	return w.blobCount
}

// FileSize returns the current file size
func (w *Writer) FileSize() uint64 {
	return w.offset + FooterSize
}

// writerAdapter adapts vfs.WritableFile to io.Writer
type writerAdapter struct {
	file vfs.WritableFile
}

func (a *writerAdapter) Write(p []byte) (n int, err error) {
	return len(p), a.file.Append(p)
}

// Ensure WritableFile implements io.Writer via Append
var _ io.Writer = (*writerAdapter)(nil)
