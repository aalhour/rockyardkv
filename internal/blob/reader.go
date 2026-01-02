// reader.go implements blob file reading for BlobDB.
//
// BlobFileReader in RocksDB reads blob files and retrieves values
// by their blob index (file number + offset + size).
//
// Reference: RocksDB v10.7.5
//   - db/blob/blob_file_reader.h
//   - db/blob/blob_file_reader.cc
package blob

import (
	"bytes"
	"errors"
	"io"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/vfs"
)

// Reader reads blob records from a blob file.
type Reader struct {
	file   vfs.RandomAccessFile
	size   int64
	header *Header
	footer *Footer
}

// NewReader creates a new blob file reader
func NewReader(file vfs.RandomAccessFile) (*Reader, error) {
	size := file.Size()
	if size < int64(HeaderSize+FooterSize) {
		return nil, ErrInvalidBlobFile
	}

	// Read header
	headerBuf := make([]byte, HeaderSize)
	if _, err := file.ReadAt(headerBuf, 0); err != nil {
		return nil, err
	}

	header, err := DecodeHeader(bytes.NewReader(headerBuf))
	if err != nil {
		return nil, err
	}

	// Read footer
	footerBuf := make([]byte, FooterSize)
	if _, err := file.ReadAt(footerBuf, size-FooterSize); err != nil {
		return nil, err
	}

	footer, err := DecodeFooter(footerBuf)
	if err != nil {
		return nil, err
	}

	return &Reader{
		file:   file,
		size:   size,
		header: header,
		footer: footer,
	}, nil
}

// GetBlob reads a blob at the given index
func (r *Reader) GetBlob(idx *BlobIndex) (*BlobRecord, error) {
	// Read the blob record data
	data := make([]byte, idx.Size)
	if _, err := r.file.ReadAt(data, int64(idx.Offset)); err != nil {
		return nil, err
	}

	// Decode the record
	record, err := DecodeRecord(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	// Decompress if needed
	if r.header.CompressionType != compression.NoCompression {
		decompressed, err := compression.Decompress(r.header.CompressionType, record.Value)
		if err != nil {
			return nil, err
		}
		record.Value = decompressed
	}

	return record, nil
}

// Header returns the blob file header
func (r *Reader) Header() *Header {
	return r.header
}

// Footer returns the blob file footer
func (r *Reader) Footer() *Footer {
	return r.footer
}

// BlobCount returns the number of blobs in the file
func (r *Reader) BlobCount() uint64 {
	return r.footer.BlobCount
}

// Close closes the reader
func (r *Reader) Close() error {
	return r.file.Close()
}

// Iterate iterates over all blobs in the file
func (r *Reader) Iterate(fn func(record *BlobRecord) error) error {
	offset := int64(HeaderSize)
	endOffset := r.size - FooterSize

	for offset < endOffset {
		// Read enough data to decode a record
		// We need to read in chunks since we don't know record sizes upfront
		buf := make([]byte, 4096) // Start with 4KB
		n, err := r.file.ReadAt(buf, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n == 0 {
			break
		}

		// Decode record from buffer
		record, err := DecodeRecord(bytes.NewReader(buf[:n]))
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // End of records
			}
			return err
		}

		// Decompress if needed
		if r.header.CompressionType != compression.NoCompression {
			decompressed, err := compression.Decompress(r.header.CompressionType, record.Value)
			if err != nil {
				return err
			}
			record.Value = decompressed
		}

		if err := fn(record); err != nil {
			return err
		}

		offset += int64(record.Size())
	}

	return nil
}
