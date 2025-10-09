// reader.go implements WAL log file reading.
//
// Reader is a general purpose log stream reader. It reads records from
// a log file, handling fragmented records that span block boundaries.
//
// Reference: RocksDB v10.7.5
//   - db/log_reader.h
//   - db/log_reader.cc
package wal

import (
	"errors"
	"io"

	"github.com/aalhour/rockyardkv/internal/checksum"
	"github.com/aalhour/rockyardkv/internal/encoding"
)

var (
	// ErrCorruptedRecord indicates a record with an invalid checksum.
	ErrCorruptedRecord = errors.New("wal: corrupted record (bad checksum)")

	// ErrShortRecord indicates a record that is shorter than expected.
	ErrShortRecord = errors.New("wal: short record")

	// ErrInvalidRecordType indicates an unrecognized record type.
	ErrInvalidRecordType = errors.New("wal: invalid record type")

	// ErrOldRecord indicates a record from a recycled log file.
	ErrOldRecord = errors.New("wal: old record from recycled log")

	// ErrUnexpectedEOF indicates an unexpected end of file.
	ErrUnexpectedEOF = errors.New("wal: unexpected end of file")

	// ErrUnexpectedMiddleRecord indicates a middle record without a first record.
	ErrUnexpectedMiddleRecord = errors.New("wal: unexpected middle record")

	// ErrUnexpectedLastRecord indicates a last record without a first record.
	ErrUnexpectedLastRecord = errors.New("wal: unexpected last record")

	// ErrUnexpectedFirstRecord indicates a first record while already in a fragmented record.
	ErrUnexpectedFirstRecord = errors.New("wal: unexpected first record")
)

// Reporter is called when corruption or other issues are detected.
type Reporter interface {
	// Corruption is called when corrupted data is detected.
	Corruption(bytes int, err error)

	// OldLogRecord is called when an old record from a recycled log is found.
	OldLogRecord(bytes int)
}

// Reader reads records from a WAL file.
type Reader struct {
	src           io.Reader
	reporter      Reporter
	checksum      bool   // Whether to verify checksums
	logNumber     uint64 // Expected log number for recyclable format
	backingStore  []byte // Buffer for reading blocks
	buffer        []byte // Current unconsumed data in backingStore
	eof           bool   // Whether we've hit EOF
	endOfBuffer   int    //nolint:unused // Reserved for block boundary tracking
	lastRecordEnd int    // Position after the last record
	blockOffset   int    // Offset within current block

	// Fragment assembly
	fragments          []byte // Accumulated fragments for multi-part records
	inFragmentedRecord bool
}

// NewReader creates a new WAL reader.
//
// Parameters:
//   - src: The source reader (typically a file)
//   - reporter: Optional reporter for corruption (can be nil)
//   - verifyChecksum: Whether to verify record checksums
//   - logNumber: Expected log number (for recyclable format validation)
func NewReader(src io.Reader, reporter Reporter, verifyChecksum bool, logNumber uint64) *Reader {
	return &Reader{
		src:          src,
		reporter:     reporter,
		checksum:     verifyChecksum,
		logNumber:    logNumber,
		backingStore: make([]byte, BlockSize),
		buffer:       nil,
		eof:          false,
	}
}

// ReadRecord reads the next logical record from the log.
// Returns the record data and nil error on success.
// Returns nil and io.EOF when no more records are available.
// Returns nil and another error on failure.
//
// The returned slice is valid until the next call to ReadRecord.
func (r *Reader) ReadRecord() ([]byte, error) {
	r.fragments = r.fragments[:0]
	r.inFragmentedRecord = false

	for {
		recordType, fragment, err := r.readPhysicalRecord()
		if err != nil {
			if errors.Is(err, io.EOF) && r.inFragmentedRecord {
				r.reportCorruption(len(r.fragments), ErrUnexpectedEOF)
				return nil, ErrUnexpectedEOF
			}
			return nil, err
		}

		// Convert recyclable types to legacy for uniform handling
		baseType := ToLegacy(recordType)

		switch baseType {
		case FullType:
			if r.inFragmentedRecord {
				r.reportCorruption(len(r.fragments), ErrUnexpectedFirstRecord)
			}
			return fragment, nil

		case FirstType:
			if r.inFragmentedRecord {
				r.reportCorruption(len(r.fragments), ErrUnexpectedFirstRecord)
			}
			r.fragments = append(r.fragments[:0], fragment...)
			r.inFragmentedRecord = true

		case MiddleType:
			if !r.inFragmentedRecord {
				r.reportCorruption(len(fragment), ErrUnexpectedMiddleRecord)
				continue
			}
			r.fragments = append(r.fragments, fragment...)

		case LastType:
			if !r.inFragmentedRecord {
				r.reportCorruption(len(fragment), ErrUnexpectedLastRecord)
				continue
			}
			r.fragments = append(r.fragments, fragment...)
			r.inFragmentedRecord = false
			// Return a copy to avoid issues with buffer reuse
			result := make([]byte, len(r.fragments))
			copy(result, r.fragments)
			return result, nil

		case ZeroType:
			// Skip zero padding
			continue

		default:
			// Unknown record type
			if recordType&RecordTypeSafeIgnoreMask != 0 {
				// Safe to ignore
				continue
			}
			r.reportCorruption(len(fragment), ErrInvalidRecordType)
			continue
		}
	}
}

// readPhysicalRecord reads a single physical record from the log.
// Returns the record type, payload, and any error.
func (r *Reader) readPhysicalRecord() (RecordType, []byte, error) {
	for {
		// Read more data if needed
		if len(r.buffer) < HeaderSize {
			if r.eof {
				return 0, nil, io.EOF
			}

			// Read a new block
			n, err := io.ReadFull(r.src, r.backingStore)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					r.eof = true
					if n == 0 {
						return 0, nil, io.EOF
					}
					// Process partial block
				} else {
					return 0, nil, err
				}
			}

			r.buffer = r.backingStore[:n]
			r.blockOffset = 0
		}

		// Parse header
		header := r.buffer[:HeaderSize]
		crcStored := encoding.DecodeFixed32(header[0:4])
		length := int(encoding.DecodeFixed16(header[4:6]))
		recordType := RecordType(header[6])

		// Determine header size based on record type
		headerSize := HeaderSize
		if IsRecyclableType(recordType) {
			headerSize = RecyclableHeaderSize
		}

		// Check if we have enough data for the header
		if len(r.buffer) < headerSize {
			if r.eof {
				return 0, nil, io.EOF
			}
			r.reportCorruption(len(r.buffer), ErrShortRecord)
			r.buffer = nil
			continue
		}

		// Check if we have enough data for the full record
		if len(r.buffer) < headerSize+length {
			if r.eof {
				return 0, nil, io.EOF
			}
			r.reportCorruption(len(r.buffer), ErrShortRecord)
			r.buffer = nil
			continue
		}

		// For zero type with zero length, just skip (padding)
		if recordType == ZeroType && length == 0 {
			r.buffer = r.buffer[headerSize:]
			r.blockOffset += headerSize
			continue
		}

		// Extract payload
		payload := r.buffer[headerSize : headerSize+length]

		// Verify checksum if enabled
		if r.checksum {
			// Compute expected CRC
			crc := checksum.Value([]byte{byte(recordType)})

			if IsRecyclableType(recordType) {
				// Check log number
				logNum := encoding.DecodeFixed32(r.buffer[7:11])
				if uint64(logNum) != r.logNumber {
					// This is an old record from a recycled log
					if r.reporter != nil {
						r.reporter.OldLogRecord(headerSize + length)
					}
					r.buffer = r.buffer[headerSize+length:]
					r.blockOffset += headerSize + length
					return 0, nil, ErrOldRecord
				}
				// Extend CRC with log number
				crc = checksum.Extend(crc, r.buffer[7:11])
			}

			// Extend CRC with payload
			crc = checksum.Extend(crc, payload)
			crc = checksum.Mask(crc)

			if crc != crcStored {
				r.reportCorruption(headerSize+length, ErrCorruptedRecord)
				r.buffer = r.buffer[headerSize+length:]
				r.blockOffset += headerSize + length
				continue
			}
		}

		// Advance buffer
		r.buffer = r.buffer[headerSize+length:]
		r.blockOffset += headerSize + length
		r.lastRecordEnd = r.blockOffset

		// Make a copy of payload since buffer may be reused
		result := make([]byte, len(payload))
		copy(result, payload)
		return recordType, result, nil
	}
}

// reportCorruption reports a corruption to the reporter if one is set.
func (r *Reader) reportCorruption(bytes int, err error) {
	if r.reporter != nil {
		r.reporter.Corruption(bytes, err)
	}
}

// IsEOF returns true if the reader has reached end of file.
func (r *Reader) IsEOF() bool {
	return r.eof
}

// LastRecordEnd returns the byte offset after the last successfully read record.
func (r *Reader) LastRecordEnd() int {
	return r.lastRecordEnd
}
