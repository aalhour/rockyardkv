// Package blob implements BlobDB - separated value storage for large values.
//
// BlobDB stores large values in separate blob files to reduce write amplification
// in the LSM tree. When a value exceeds the configured threshold, it's written
// to a blob file and only a blob reference is stored in the LSM tree.
//
// Blob File Format:
//
//	[Header]
//	[Blob Record 1]
//	[Blob Record 2]
//	...
//	[Footer]
//
// Header (32 bytes):
//
//	Magic Number (8 bytes): "ROCKSBLB"
//	Version (4 bytes)
//	Column Family ID (4 bytes)
//	Compression Type (1 byte)
//	Reserved (15 bytes)
//
// Blob Record:
//
//	Key Length (4 bytes, varint)
//	Key (variable)
//	Value Length (4 bytes, varint)
//	Value (variable)
//	CRC32 Checksum (4 bytes)
//
// Footer (48 bytes):
//
//	Blob Count (8 bytes)
//	Expiration Range (16 bytes: min + max)
//	Footer Checksum (4 bytes)
//	Magic Number (8 bytes)
//	Reserved (12 bytes)
//
// Reference: RocksDB v10.7.5
//   - db/blob/blob_file_builder.cc
//   - db/blob/blob_file_reader.cc
//   - db/blob/blob_log_format.h
package blob

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/aalhour/rockyardkv/internal/compression"
)

// Constants for blob file format
const (
	// HeaderSize is the size of the blob file header in bytes
	HeaderSize = 32

	// FooterSize is the size of the blob file footer in bytes
	FooterSize = 48

	// MagicNumber identifies a blob file
	MagicNumber uint64 = 0x524F434B53424C42 // "ROCKSBLB" in ASCII

	// CurrentVersion is the current blob file format version
	CurrentVersion uint32 = 1

	// DefaultMinBlobSize is the minimum value size to store in blob files
	DefaultMinBlobSize = 4 * 1024 // 4KB
)

var (
	// ErrInvalidBlobFile indicates the file is not a valid blob file
	ErrInvalidBlobFile = errors.New("blob: invalid blob file")

	// ErrBlobNotFound indicates the requested blob was not found
	ErrBlobNotFound = errors.New("blob: blob not found")

	// ErrChecksumMismatch indicates a checksum verification failure
	ErrChecksumMismatch = errors.New("blob: checksum mismatch")

	// ErrUnsupportedVersion indicates an unsupported blob file version
	ErrUnsupportedVersion = errors.New("blob: unsupported version")
)

// Header represents the blob file header
type Header struct {
	Magic           uint64
	Version         uint32
	ColumnFamilyID  uint32
	CompressionType compression.Type
}

// Encode writes the header to the given writer
func (h *Header) Encode(w io.Writer) error {
	buf := make([]byte, HeaderSize)

	binary.LittleEndian.PutUint64(buf[0:8], h.Magic)
	binary.LittleEndian.PutUint32(buf[8:12], h.Version)
	binary.LittleEndian.PutUint32(buf[12:16], h.ColumnFamilyID)
	buf[16] = byte(h.CompressionType)
	// bytes 17-31 are reserved

	_, err := w.Write(buf)
	return err
}

// DecodeHeader reads a header from the given reader
func DecodeHeader(r io.Reader) (*Header, error) {
	buf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	h := &Header{
		Magic:           binary.LittleEndian.Uint64(buf[0:8]),
		Version:         binary.LittleEndian.Uint32(buf[8:12]),
		ColumnFamilyID:  binary.LittleEndian.Uint32(buf[12:16]),
		CompressionType: compression.Type(buf[16]),
	}

	if h.Magic != MagicNumber {
		return nil, ErrInvalidBlobFile
	}

	if h.Version > CurrentVersion {
		return nil, ErrUnsupportedVersion
	}

	return h, nil
}

// Footer represents the blob file footer
type Footer struct {
	BlobCount     uint64
	ExpirationMin uint64
	ExpirationMax uint64
	Checksum      uint32
	Magic         uint64
}

// Encode writes the footer to the given writer
func (f *Footer) Encode(w io.Writer) error {
	buf := make([]byte, FooterSize)

	binary.LittleEndian.PutUint64(buf[0:8], f.BlobCount)
	binary.LittleEndian.PutUint64(buf[8:16], f.ExpirationMin)
	binary.LittleEndian.PutUint64(buf[16:24], f.ExpirationMax)
	// Calculate checksum of first 24 bytes
	f.Checksum = crc32.ChecksumIEEE(buf[0:24])
	binary.LittleEndian.PutUint32(buf[24:28], f.Checksum)
	binary.LittleEndian.PutUint64(buf[28:36], MagicNumber)
	// bytes 36-47 are reserved

	_, err := w.Write(buf)
	return err
}

// DecodeFooter reads a footer from the given byte slice
func DecodeFooter(data []byte) (*Footer, error) {
	if len(data) < FooterSize {
		return nil, ErrInvalidBlobFile
	}

	// Read from the last FooterSize bytes
	buf := data[len(data)-FooterSize:]

	f := &Footer{
		BlobCount:     binary.LittleEndian.Uint64(buf[0:8]),
		ExpirationMin: binary.LittleEndian.Uint64(buf[8:16]),
		ExpirationMax: binary.LittleEndian.Uint64(buf[16:24]),
		Checksum:      binary.LittleEndian.Uint32(buf[24:28]),
		Magic:         binary.LittleEndian.Uint64(buf[28:36]),
	}

	if f.Magic != MagicNumber {
		return nil, ErrInvalidBlobFile
	}

	// Verify checksum
	expected := crc32.ChecksumIEEE(buf[0:24])
	if f.Checksum != expected {
		return nil, ErrChecksumMismatch
	}

	return f, nil
}

// BlobRecord represents a single blob entry
type BlobRecord struct {
	Key      []byte
	Value    []byte
	Checksum uint32
}

// Size returns the encoded size of the blob record
func (r *BlobRecord) Size() int {
	// varint key length + key + varint value length + value + crc32
	return varIntSize(len(r.Key)) + len(r.Key) +
		varIntSize(len(r.Value)) + len(r.Value) + 4
}

// Encode writes the blob record to the given writer
func (r *BlobRecord) Encode(w io.Writer) error {
	// Write key length and key
	if err := writeVarInt(w, len(r.Key)); err != nil {
		return err
	}
	if _, err := w.Write(r.Key); err != nil {
		return err
	}

	// Write value length and value
	if err := writeVarInt(w, len(r.Value)); err != nil {
		return err
	}
	if _, err := w.Write(r.Value); err != nil {
		return err
	}

	// Calculate and write checksum
	crc := crc32.NewIEEE()
	crc.Write(r.Key)
	crc.Write(r.Value)
	r.Checksum = crc.Sum32()

	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, r.Checksum)
	_, err := w.Write(buf)
	return err
}

// DecodeRecord reads a blob record from the given reader
func DecodeRecord(r io.Reader) (*BlobRecord, error) {
	// Read key length and key
	keyLen, err := readVarInt(r)
	if err != nil {
		return nil, err
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, err
	}

	// Read value length and value
	valueLen, err := readVarInt(r)
	if err != nil {
		return nil, err
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, err
	}

	// Read and verify checksum
	checksumBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, checksumBuf); err != nil {
		return nil, err
	}
	checksum := binary.LittleEndian.Uint32(checksumBuf)

	crc := crc32.NewIEEE()
	crc.Write(key)
	crc.Write(value)
	expected := crc.Sum32()

	if checksum != expected {
		return nil, ErrChecksumMismatch
	}

	return &BlobRecord{
		Key:      key,
		Value:    value,
		Checksum: checksum,
	}, nil
}

// BlobIndex is a reference to a blob stored in a blob file
type BlobIndex struct {
	FileNumber uint64
	Offset     uint64
	Size       uint64
}

// Encode encodes the blob index to bytes
func (idx *BlobIndex) Encode() []byte {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[0:8], idx.FileNumber)
	binary.LittleEndian.PutUint64(buf[8:16], idx.Offset)
	binary.LittleEndian.PutUint64(buf[16:24], idx.Size)
	return buf
}

// DecodeBlobIndex decodes a blob index from bytes
func DecodeBlobIndex(data []byte) (*BlobIndex, error) {
	if len(data) < 24 {
		return nil, ErrInvalidBlobFile
	}
	return &BlobIndex{
		FileNumber: binary.LittleEndian.Uint64(data[0:8]),
		Offset:     binary.LittleEndian.Uint64(data[8:16]),
		Size:       binary.LittleEndian.Uint64(data[16:24]),
	}, nil
}

// IsBlobIndex checks if a value is a blob index (starts with special marker)
func IsBlobIndex(value []byte) bool {
	// Blob indexes are exactly 24 bytes
	return len(value) == 24
}

// Helper functions for varint encoding

func varIntSize(n int) int {
	if n < 128 {
		return 1
	}
	if n < 16384 {
		return 2
	}
	if n < 2097152 {
		return 3
	}
	if n < 268435456 {
		return 4
	}
	return 5
}

func writeVarInt(w io.Writer, n int) error {
	buf := make([]byte, 5)
	i := 0
	for n >= 128 {
		buf[i] = byte(n&0x7F | 0x80)
		n >>= 7
		i++
	}
	buf[i] = byte(n)
	_, err := w.Write(buf[:i+1])
	return err
}

func readVarInt(r io.Reader) (int, error) {
	buf := make([]byte, 1)
	result := 0
	shift := 0

	for {
		if _, err := r.Read(buf); err != nil {
			return 0, err
		}
		b := buf[0]
		result |= int(b&0x7F) << shift
		if b < 128 {
			break
		}
		shift += 7
	}
	return result, nil
}
