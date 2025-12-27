// writer.go implements WAL log file writing.
//
// Writer is a general purpose log stream writer. It provides an append-only
// abstraction for writing data, fragmenting records across block boundaries.
//
// Reference: RocksDB v10.7.5
//   - db/log_writer.h
//   - db/log_writer.cc
package wal

import (
	"io"

	"github.com/aalhour/rockyardkv/internal/checksum"
	"github.com/aalhour/rockyardkv/internal/encoding"
	"github.com/aalhour/rockyardkv/internal/testutil"
)

// Writer writes records to a WAL file.
//
// Records are written in the RocksDB log format, which fragments large records
// across block boundaries. Each physical record has a header with checksum,
// length, and type.
type Writer struct {
	dest        io.Writer
	blockOffset int    // Current offset within the current block
	logNumber   uint64 // Log file number (used for recyclable format)
	recyclable  bool   // Whether to use recyclable record format
	headerSize  int    // Size of the header (7 for legacy, 11 for recyclable)

	// Pre-computed CRC32C values for each record type
	typeCRC [MaxRecordType + 1]uint32

	// Reusable header buffer
	headerBuf [RecyclableHeaderSize]byte
}

// NewWriter creates a new WAL writer that writes to dest.
//
// Parameters:
//   - dest: The destination writer (typically a file)
//   - logNumber: The log file number (used for recyclable format)
//   - recyclable: Whether to use recyclable record format
func NewWriter(dest io.Writer, logNumber uint64, recyclable bool) *Writer {
	w := &Writer{
		dest:        dest,
		blockOffset: 0,
		logNumber:   logNumber,
		recyclable:  recyclable,
	}

	if recyclable {
		w.headerSize = RecyclableHeaderSize
	} else {
		w.headerSize = HeaderSize
	}

	// Pre-compute CRC32C values for each record type
	for i := 0; i <= int(MaxRecordType); i++ {
		w.typeCRC[i] = checksum.Value([]byte{byte(i)})
	}

	return w
}

// AddRecord writes a complete logical record to the log.
// The record may be split into multiple physical records if it doesn't fit
// in the current block.
//
// Returns the number of bytes written (including headers) and any error.
func (w *Writer) AddRecord(data []byte) (int, error) {
	// Kill point: crash during WAL append (before any write)
	testutil.MaybeKill(testutil.KPWALAppend0)

	ptr := data
	left := len(data)
	totalWritten := 0
	begin := true

	// Fragment the record if necessary
	// Note: even if data is empty, we emit a single zero-length record
	for {
		leftover := BlockSize - w.blockOffset

		// If there's not enough space for a header, pad and move to next block
		if leftover < w.headerSize {
			if leftover > 0 {
				// Write padding zeros
				padding := make([]byte, leftover)
				n, err := w.dest.Write(padding)
				if err != nil {
					return totalWritten + n, err
				}
				totalWritten += n
			}
			w.blockOffset = 0
		}

		// Invariant: we never leave < headerSize bytes in a block
		avail := BlockSize - w.blockOffset - w.headerSize
		fragmentLength := min(left, avail)

		// Determine record type
		end := (left == fragmentLength)
		var recordType RecordType
		if begin && end {
			recordType = FullType
		} else if begin {
			recordType = FirstType
		} else if end {
			recordType = LastType
		} else {
			recordType = MiddleType
		}

		if w.recyclable {
			recordType = ToRecyclable(recordType)
		}

		// Write the physical record
		n, err := w.emitPhysicalRecord(recordType, ptr[:fragmentLength])
		totalWritten += n
		if err != nil {
			return totalWritten, err
		}

		ptr = ptr[fragmentLength:]
		left -= fragmentLength
		begin = false

		if left == 0 {
			break
		}
	}

	return totalWritten, nil
}

// emitPhysicalRecord writes a single physical record.
// Returns the number of bytes written and any error.
func (w *Writer) emitPhysicalRecord(t RecordType, payload []byte) (int, error) {
	n := len(payload)
	if n > 0xFFFF {
		// Pre-condition violation: payload exceeds maximum record size
		panic("wal: record payload too large") //nolint:forbidigo // intentional panic for precondition violation
	}

	// Format the header:
	// [4] CRC
	// [2] Length
	// [1] Type
	// [4] Log number (recyclable only)

	// Set length (little-endian)
	w.headerBuf[4] = byte(n & 0xFF)
	w.headerBuf[5] = byte(n >> 8)
	// Set type
	w.headerBuf[6] = byte(t)

	// Compute CRC
	crc := w.typeCRC[t]

	headerSize := HeaderSize
	if IsRecyclableType(t) {
		headerSize = RecyclableHeaderSize
		// Encode log number
		encoding.EncodeFixed32(w.headerBuf[7:], uint32(w.logNumber))
		// Extend CRC with log number
		crc = checksum.Extend(crc, w.headerBuf[7:11])
	}

	// Extend CRC with payload
	crc = checksum.Extend(crc, payload)
	// Mask the CRC for storage
	crc = checksum.Mask(crc)
	// Write CRC to header
	encoding.EncodeFixed32(w.headerBuf[:], crc)

	// Write header
	totalWritten := 0
	written, err := w.dest.Write(w.headerBuf[:headerSize])
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}

	// Write payload
	written, err = w.dest.Write(payload)
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}

	w.blockOffset += headerSize + n
	return totalWritten, nil
}

// BlockOffset returns the current offset within the current block.
func (w *Writer) BlockOffset() int {
	return w.blockOffset
}

// LogNumber returns the log file number.
func (w *Writer) LogNumber() uint64 {
	return w.logNumber
}

// IsRecyclable returns whether this writer uses recyclable format.
func (w *Writer) IsRecyclable() bool {
	return w.recyclable
}

// Sync flushes the underlying writer if it supports it.
func (w *Writer) Sync() error {
	// Kill point: crash before WAL sync
	testutil.MaybeKill(testutil.KPWALSync0)

	if syncer, ok := w.dest.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return err
		}
	}

	// Kill point: crash after WAL sync (data is durable)
	testutil.MaybeKill(testutil.KPWALSync1)
	return nil
}
