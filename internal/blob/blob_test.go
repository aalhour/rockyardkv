package blob

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/aalhour/rockyardkv/internal/compression"
	"github.com/aalhour/rockyardkv/vfs"
)

func TestHeaderEncodeDecode(t *testing.T) {
	h := &Header{
		Magic:           MagicNumber,
		Version:         CurrentVersion,
		ColumnFamilyID:  42,
		CompressionType: compression.LZ4Compression,
	}

	var buf bytes.Buffer
	if err := h.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if buf.Len() != HeaderSize {
		t.Fatalf("Expected header size %d, got %d", HeaderSize, buf.Len())
	}

	decoded, err := DecodeHeader(&buf)
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}

	if decoded.Magic != h.Magic {
		t.Errorf("Magic mismatch: got %x, want %x", decoded.Magic, h.Magic)
	}
	if decoded.Version != h.Version {
		t.Errorf("Version mismatch: got %d, want %d", decoded.Version, h.Version)
	}
	if decoded.ColumnFamilyID != h.ColumnFamilyID {
		t.Errorf("ColumnFamilyID mismatch: got %d, want %d", decoded.ColumnFamilyID, h.ColumnFamilyID)
	}
	if decoded.CompressionType != h.CompressionType {
		t.Errorf("CompressionType mismatch: got %d, want %d", decoded.CompressionType, h.CompressionType)
	}
}

func TestFooterEncodeDecode(t *testing.T) {
	f := &Footer{
		BlobCount:     100,
		ExpirationMin: 1000,
		ExpirationMax: 2000,
		Magic:         MagicNumber,
	}

	var buf bytes.Buffer
	if err := f.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if buf.Len() != FooterSize {
		t.Fatalf("Expected footer size %d, got %d", FooterSize, buf.Len())
	}

	decoded, err := DecodeFooter(buf.Bytes())
	if err != nil {
		t.Fatalf("DecodeFooter failed: %v", err)
	}

	if decoded.BlobCount != f.BlobCount {
		t.Errorf("BlobCount mismatch: got %d, want %d", decoded.BlobCount, f.BlobCount)
	}
	if decoded.ExpirationMin != f.ExpirationMin {
		t.Errorf("ExpirationMin mismatch: got %d, want %d", decoded.ExpirationMin, f.ExpirationMin)
	}
	if decoded.ExpirationMax != f.ExpirationMax {
		t.Errorf("ExpirationMax mismatch: got %d, want %d", decoded.ExpirationMax, f.ExpirationMax)
	}
}

func TestBlobRecordEncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{"small", []byte("key"), []byte("value")},
		{"medium", []byte("medium-key"), bytes.Repeat([]byte("x"), 1000)},
		{"large", []byte("large-key"), bytes.Repeat([]byte("y"), 100000)},
		{"empty-value", []byte("key"), []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &BlobRecord{
				Key:   tt.key,
				Value: tt.value,
			}

			var buf bytes.Buffer
			if err := record.Encode(&buf); err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			decoded, err := DecodeRecord(&buf)
			if err != nil {
				t.Fatalf("DecodeRecord failed: %v", err)
			}

			if !bytes.Equal(decoded.Key, tt.key) {
				t.Errorf("Key mismatch: got %v, want %v", decoded.Key, tt.key)
			}
			if !bytes.Equal(decoded.Value, tt.value) {
				t.Errorf("Value mismatch")
			}
		})
	}
}

func TestBlobIndex(t *testing.T) {
	idx := &BlobIndex{
		FileNumber: 12345,
		Offset:     67890,
		Size:       11111,
	}

	encoded := idx.Encode()
	if len(encoded) != 24 {
		t.Fatalf("Expected 24 bytes, got %d", len(encoded))
	}

	decoded, err := DecodeBlobIndex(encoded)
	if err != nil {
		t.Fatalf("DecodeBlobIndex failed: %v", err)
	}

	if decoded.FileNumber != idx.FileNumber {
		t.Errorf("FileNumber mismatch: got %d, want %d", decoded.FileNumber, idx.FileNumber)
	}
	if decoded.Offset != idx.Offset {
		t.Errorf("Offset mismatch: got %d, want %d", decoded.Offset, idx.Offset)
	}
	if decoded.Size != idx.Size {
		t.Errorf("Size mismatch: got %d, want %d", decoded.Size, idx.Size)
	}
}

func TestWriterReader(t *testing.T) {
	dir, err := os.MkdirTemp("", "blob-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	fs := vfs.Default()
	blobPath := filepath.Join(dir, "test.blob")

	// Write blobs
	file, err := fs.Create(blobPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	writer, err := NewWriter(file, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Write several blobs
	blobs := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), bytes.Repeat([]byte("x"), 10000)},
		{[]byte("key3"), []byte("short")},
	}

	var indexes []*BlobIndex
	for _, b := range blobs {
		idx, err := writer.AddBlob(b.key, b.value)
		if err != nil {
			t.Fatalf("AddBlob failed: %v", err)
		}
		indexes = append(indexes, idx)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if writer.BlobCount() != uint64(len(blobs)) {
		t.Errorf("BlobCount mismatch: got %d, want %d", writer.BlobCount(), len(blobs))
	}

	// Read blobs
	readFile, err := fs.OpenRandomAccess(blobPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	reader, err := NewReader(readFile)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	if reader.BlobCount() != uint64(len(blobs)) {
		t.Errorf("Reader BlobCount mismatch: got %d, want %d", reader.BlobCount(), len(blobs))
	}

	// Read each blob by index
	for i, idx := range indexes {
		record, err := reader.GetBlob(idx)
		if err != nil {
			t.Fatalf("GetBlob %d failed: %v", i, err)
		}

		if !bytes.Equal(record.Key, blobs[i].key) {
			t.Errorf("Blob %d key mismatch", i)
		}
		if !bytes.Equal(record.Value, blobs[i].value) {
			t.Errorf("Blob %d value mismatch", i)
		}
	}
}

func TestWriterReaderWithCompression(t *testing.T) {
	dir, err := os.MkdirTemp("", "blob-test-compress-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	fs := vfs.Default()
	blobPath := filepath.Join(dir, "test.blob")

	// Write blobs with LZ4 compression
	file, err := fs.Create(blobPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	opts := WriterOptions{
		CompressionType: compression.LZ4Compression,
	}
	writer, err := NewWriter(file, opts)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Compressible data
	value := bytes.Repeat([]byte("compressible data "), 1000)
	idx, err := writer.AddBlob([]byte("key"), value)
	if err != nil {
		t.Fatalf("AddBlob failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read and verify
	readFile, err := fs.OpenRandomAccess(blobPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	reader, err := NewReader(readFile)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	record, err := reader.GetBlob(idx)
	if err != nil {
		t.Fatalf("GetBlob failed: %v", err)
	}

	if !bytes.Equal(record.Value, value) {
		t.Errorf("Value mismatch after decompression")
	}
}

func TestReaderIterate(t *testing.T) {
	dir, err := os.MkdirTemp("", "blob-test-iterate-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	fs := vfs.Default()
	blobPath := filepath.Join(dir, "test.blob")

	// Write blobs
	file, err := fs.Create(blobPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	writer, err := NewWriter(file, DefaultWriterOptions())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	numBlobs := 10
	for i := range numBlobs {
		key := []byte("key" + string(rune('0'+i)))
		value := []byte("value" + string(rune('0'+i)))
		if _, err := writer.AddBlob(key, value); err != nil {
			t.Fatalf("AddBlob failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read and iterate
	readFile, err := fs.OpenRandomAccess(blobPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	reader, err := NewReader(readFile)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	count := 0
	err = reader.Iterate(func(record *BlobRecord) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Iterate failed: %v", err)
	}

	if count != numBlobs {
		t.Errorf("Iterate count mismatch: got %d, want %d", count, numBlobs)
	}
}

func TestInvalidHeader(t *testing.T) {
	// Invalid magic number
	buf := bytes.NewBuffer(make([]byte, HeaderSize))
	_, err := DecodeHeader(buf)
	if !errors.Is(err, ErrInvalidBlobFile) {
		t.Errorf("Expected ErrInvalidBlobFile, got %v", err)
	}
}

func TestInvalidFooter(t *testing.T) {
	// Too short
	_, err := DecodeFooter(make([]byte, 10))
	if !errors.Is(err, ErrInvalidBlobFile) {
		t.Errorf("Expected ErrInvalidBlobFile, got %v", err)
	}

	// Invalid magic
	buf := make([]byte, FooterSize)
	_, err = DecodeFooter(buf)
	if !errors.Is(err, ErrInvalidBlobFile) {
		t.Errorf("Expected ErrInvalidBlobFile, got %v", err)
	}
}

func TestBlobRecordCorruption(t *testing.T) {
	record := &BlobRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	var buf bytes.Buffer
	if err := record.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Corrupt the checksum
	data := buf.Bytes()
	data[len(data)-1] ^= 0xFF

	_, err := DecodeRecord(bytes.NewReader(data))
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("Expected ErrChecksumMismatch, got %v", err)
	}
}
