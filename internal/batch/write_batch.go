// Package batch implements the WriteBatch format for atomic writes.
//
// WriteBatch Format:
//
//	Header (12 bytes):
//	  - 8 bytes: sequence number (little-endian uint64)
//	  - 4 bytes: count (little-endian uint32)
//	Records (repeated):
//	  - 1 byte: tag (record type)
//	  - For ColumnFamily variants: varint32 column_family_id
//	  - length-prefixed key
//	  - (for Put/Merge/RangeDeletion): length-prefixed value
//
// Reference: RocksDB v10.7.5
//   - db/write_batch.cc
//   - db/write_batch_internal.h
//   - db/dbformat.h (ValueType enum)
package batch

import (
	"encoding/binary"
	"errors"

	"github.com/aalhour/rockyardkv/internal/encoding"
)

// HeaderSize is the size in bytes of the WriteBatch header (8 bytes sequence + 4 bytes count).
const HeaderSize = 12

// Record types for WriteBatch entries.
// Reference: db/dbformat.h ValueType enum
const (
	TypeDeletion                        byte = 0x00
	TypeValue                           byte = 0x01
	TypeMerge                           byte = 0x02
	TypeLogData                         byte = 0x03
	TypeColumnFamilyDeletion            byte = 0x04
	TypeColumnFamilyValue               byte = 0x05
	TypeColumnFamilyMerge               byte = 0x06
	TypeSingleDeletion                  byte = 0x07
	TypeColumnFamilySingleDeletion      byte = 0x08
	TypeBeginPrepareXID                 byte = 0x09
	TypeEndPrepareXID                   byte = 0x0A
	TypeCommitXID                       byte = 0x0B
	TypeRollbackXID                     byte = 0x0C
	TypeNoop                            byte = 0x0D
	TypeColumnFamilyRangeDeletion       byte = 0x0E
	TypeRangeDeletion                   byte = 0x0F
	TypeColumnFamilyBlobIndex           byte = 0x10
	TypeBlobIndex                       byte = 0x11
	TypeBeginPersistedPrepareXID        byte = 0x12
	TypeBeginUnprepareXID               byte = 0x13
	TypeDeletionWithTimestamp           byte = 0x14
	TypeCommitXIDAndTimestamp           byte = 0x15
	TypeWideColumnEntity                byte = 0x16
	TypeColumnFamilyWideColumnEntity    byte = 0x17
	TypeValuePreferredSeqno             byte = 0x18
	TypeColumnFamilyValuePreferredSeqno byte = 0x19
)

var (
	// ErrCorrupted indicates a malformed WriteBatch.
	ErrCorrupted = errors.New("batch: corrupted write batch")

	// ErrTooSmall indicates the batch is smaller than the header.
	ErrTooSmall = errors.New("batch: too small")
)

// WriteBatch represents a collection of writes to be applied atomically.
type WriteBatch struct {
	data []byte // The raw batch data including header
}

// New creates a new empty WriteBatch.
func New() *WriteBatch {
	wb := &WriteBatch{
		data: make([]byte, HeaderSize),
	}
	// Initialize count to 0 (sequence will be set when written to WAL)
	return wb
}

// NewFromData creates a WriteBatch from existing data.
func NewFromData(data []byte) (*WriteBatch, error) {
	if len(data) < HeaderSize {
		return nil, ErrTooSmall
	}
	return &WriteBatch{data: data}, nil
}

// Clear resets the batch to empty state.
func (wb *WriteBatch) Clear() {
	wb.data = wb.data[:HeaderSize]
	// Reset count to 0
	binary.LittleEndian.PutUint32(wb.data[8:12], 0)
}

// Data returns the raw batch data.
func (wb *WriteBatch) Data() []byte {
	return wb.data
}

// Clone creates a deep copy of the WriteBatch.
func (wb *WriteBatch) Clone() *WriteBatch {
	clone := &WriteBatch{
		data: make([]byte, len(wb.data)),
	}
	copy(clone.data, wb.data)
	return clone
}

// Size returns the size of the batch data in bytes.
func (wb *WriteBatch) Size() int {
	return len(wb.data)
}

// Count returns the number of records in the batch.
func (wb *WriteBatch) Count() uint32 {
	return binary.LittleEndian.Uint32(wb.data[8:12])
}

// SetCount sets the count field.
func (wb *WriteBatch) SetCount(count uint32) {
	binary.LittleEndian.PutUint32(wb.data[8:12], count)
}

// Sequence returns the sequence number of the batch.
func (wb *WriteBatch) Sequence() uint64 {
	return binary.LittleEndian.Uint64(wb.data[0:8])
}

// SetSequence sets the sequence number of the batch.
func (wb *WriteBatch) SetSequence(seq uint64) {
	binary.LittleEndian.PutUint64(wb.data[0:8], seq)
}

// Put adds a Put record to the batch.
func (wb *WriteBatch) Put(key, value []byte) {
	wb.putRecord(TypeValue, 0, key, value)
}

// PutCF adds a Put record with column family to the batch.
func (wb *WriteBatch) PutCF(cfID uint32, key, value []byte) {
	if cfID == 0 {
		wb.Put(key, value)
		return
	}
	wb.putRecord(TypeColumnFamilyValue, cfID, key, value)
}

// Delete adds a Delete record to the batch.
func (wb *WriteBatch) Delete(key []byte) {
	wb.deleteRecord(TypeDeletion, 0, key)
}

// DeleteCF adds a Delete record with column family to the batch.
func (wb *WriteBatch) DeleteCF(cfID uint32, key []byte) {
	if cfID == 0 {
		wb.Delete(key)
		return
	}
	wb.deleteRecord(TypeColumnFamilyDeletion, cfID, key)
}

// SingleDelete adds a SingleDelete record to the batch.
func (wb *WriteBatch) SingleDelete(key []byte) {
	wb.deleteRecord(TypeSingleDeletion, 0, key)
}

// Merge adds a Merge record to the batch.
func (wb *WriteBatch) Merge(key, value []byte) {
	wb.putRecord(TypeMerge, 0, key, value)
}

// MergeCF adds a Merge record with column family to the batch.
func (wb *WriteBatch) MergeCF(cfID uint32, key, value []byte) {
	if cfID == 0 {
		wb.Merge(key, value)
		return
	}
	wb.putRecord(TypeColumnFamilyMerge, cfID, key, value)
}

// DeleteRange adds a DeleteRange record to the batch.
func (wb *WriteBatch) DeleteRange(startKey, endKey []byte) {
	wb.putRecord(TypeRangeDeletion, 0, startKey, endKey)
}

// DeleteRangeCF adds a DeleteRange record with column family to the batch.
func (wb *WriteBatch) DeleteRangeCF(cfID uint32, startKey, endKey []byte) {
	if cfID == 0 {
		wb.DeleteRange(startKey, endKey)
		return
	}
	wb.putRecord(TypeColumnFamilyRangeDeletion, cfID, startKey, endKey)
}

// PutLogData adds a log data record to the batch.
// LogData is not counted as a regular operation.
func (wb *WriteBatch) PutLogData(blob []byte) {
	wb.data = append(wb.data, TypeLogData)
	wb.data = encoding.AppendLengthPrefixedSlice(wb.data, blob)
	// LogData does NOT increment count
}

// Append appends the contents of another batch to this batch.
// The sequence number of the source batch is ignored.
func (wb *WriteBatch) Append(src *WriteBatch) {
	if src.Count() == 0 {
		return
	}
	// Append everything after the header from the source
	wb.data = append(wb.data, src.data[HeaderSize:]...)
	// Add the counts
	wb.SetCount(wb.Count() + src.Count())
}

// HasPut returns true if the batch contains at least one Put operation.
func (wb *WriteBatch) HasPut() bool {
	return wb.hasTag(TypeValue) || wb.hasTag(TypeColumnFamilyValue)
}

// HasDelete returns true if the batch contains at least one Delete operation.
func (wb *WriteBatch) HasDelete() bool {
	return wb.hasTag(TypeDeletion) || wb.hasTag(TypeColumnFamilyDeletion)
}

// HasSingleDelete returns true if the batch contains at least one SingleDelete operation.
func (wb *WriteBatch) HasSingleDelete() bool {
	return wb.hasTag(TypeSingleDeletion) || wb.hasTag(TypeColumnFamilySingleDeletion)
}

// HasMerge returns true if the batch contains at least one Merge operation.
func (wb *WriteBatch) HasMerge() bool {
	return wb.hasTag(TypeMerge) || wb.hasTag(TypeColumnFamilyMerge)
}

// HasDeleteRange returns true if the batch contains at least one DeleteRange operation.
func (wb *WriteBatch) HasDeleteRange() bool {
	return wb.hasTag(TypeRangeDeletion) || wb.hasTag(TypeColumnFamilyRangeDeletion)
}

// hasTag checks if the batch contains a specific tag.
// This is a simple scan - for production use, you'd cache this.
func (wb *WriteBatch) hasTag(tag byte) bool {
	data := wb.data[HeaderSize:]
	for len(data) > 0 {
		if data[0] == tag {
			return true
		}
		// Skip this record - simplified, just look for any matching tag
		data = data[1:]
	}
	return false
}

// putRecord adds a key-value record to the batch.
func (wb *WriteBatch) putRecord(tag byte, cfID uint32, key, value []byte) {
	// Append tag
	wb.data = append(wb.data, tag)

	// Append column family ID if needed
	if tag == TypeColumnFamilyValue || tag == TypeColumnFamilyMerge ||
		tag == TypeColumnFamilyRangeDeletion || tag == TypeColumnFamilyBlobIndex {
		wb.data = encoding.AppendVarint32(wb.data, cfID)
	}

	// Append length-prefixed key
	wb.data = encoding.AppendLengthPrefixedSlice(wb.data, key)

	// Append length-prefixed value
	wb.data = encoding.AppendLengthPrefixedSlice(wb.data, value)

	// Increment count
	count := wb.Count()
	wb.SetCount(count + 1)
}

// deleteRecord adds a delete record to the batch.
func (wb *WriteBatch) deleteRecord(tag byte, cfID uint32, key []byte) {
	// Append tag
	wb.data = append(wb.data, tag)

	// Append column family ID if needed
	if tag == TypeColumnFamilyDeletion || tag == TypeColumnFamilySingleDeletion {
		wb.data = encoding.AppendVarint32(wb.data, cfID)
	}

	// Append length-prefixed key
	wb.data = encoding.AppendLengthPrefixedSlice(wb.data, key)

	// Increment count
	count := wb.Count()
	wb.SetCount(count + 1)
}

// MarkBeginPrepare adds a begin-prepare marker to the batch.
// This is the start of a two-phase commit prepared transaction.
func (wb *WriteBatch) MarkBeginPrepare() {
	wb.data = append(wb.data, TypeBeginPrepareXID)
	// Note: BeginPrepare doesn't increment count - it's a marker
}

// MarkEndPrepare adds an end-prepare marker with the transaction ID.
// This completes the prepare phase of a two-phase commit.
func (wb *WriteBatch) MarkEndPrepare(xid []byte) {
	wb.data = append(wb.data, TypeEndPrepareXID)
	wb.data = encoding.AppendLengthPrefixedSlice(wb.data, xid)
	// Note: EndPrepare doesn't increment count - it's a marker
}

// MarkCommit adds a commit marker for a prepared transaction.
// This is the second phase of two-phase commit.
func (wb *WriteBatch) MarkCommit(xid []byte) {
	wb.data = append(wb.data, TypeCommitXID)
	wb.data = encoding.AppendLengthPrefixedSlice(wb.data, xid)
	// Note: Commit doesn't increment count - it's a marker
}

// MarkRollback adds a rollback marker for a prepared transaction.
// This aborts a prepared transaction.
func (wb *WriteBatch) MarkRollback(xid []byte) {
	wb.data = append(wb.data, TypeRollbackXID)
	wb.data = encoding.AppendLengthPrefixedSlice(wb.data, xid)
	// Note: Rollback doesn't increment count - it's a marker
}

// Handler is called for each record in the batch during iteration.
type Handler interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	SingleDelete(key []byte) error
	Merge(key, value []byte) error
	DeleteRange(startKey, endKey []byte) error
	LogData(blob []byte)

	// ColumnFamily variants
	PutCF(cfID uint32, key, value []byte) error
	DeleteCF(cfID uint32, key []byte) error
	SingleDeleteCF(cfID uint32, key []byte) error
	MergeCF(cfID uint32, key, value []byte) error
	DeleteRangeCF(cfID uint32, startKey, endKey []byte) error
}

// Handler2PC extends Handler with 2PC (two-phase commit) marker support.
// This is used during WAL recovery to restore prepared transactions.
type Handler2PC interface {
	Handler

	// MarkBeginPrepare indicates the start of a prepared transaction.
	// unprepared is true for TypeBeginUnprepareXID (transaction that was not persisted).
	MarkBeginPrepare(unprepared bool) error

	// MarkEndPrepare indicates the end of a prepared transaction.
	// xid is the transaction identifier (name).
	MarkEndPrepare(xid []byte) error

	// MarkCommit indicates a prepared transaction was committed.
	// xid is the transaction identifier.
	MarkCommit(xid []byte) error

	// MarkRollback indicates a prepared transaction was rolled back.
	// xid is the transaction identifier.
	MarkRollback(xid []byte) error
}

// Iterate calls the handler for each record in the batch.
func (wb *WriteBatch) Iterate(handler Handler) error {
	if len(wb.data) < HeaderSize {
		return ErrTooSmall
	}

	data := wb.data[HeaderSize:]

	for len(data) > 0 {
		tag := data[0]
		data = data[1:]

		var cfID uint32 = 0
		var key, value []byte
		var err error

		switch tag {
		case TypeColumnFamilyValue:
			cfID, data, err = decodeVarint32(data)
			if err != nil {
				return err
			}
			fallthrough
		case TypeValue:
			key, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			value, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			if cfID == 0 {
				if err := handler.Put(key, value); err != nil {
					return err
				}
			} else {
				if err := handler.PutCF(cfID, key, value); err != nil {
					return err
				}
			}

		case TypeColumnFamilyDeletion:
			cfID, data, err = decodeVarint32(data)
			if err != nil {
				return err
			}
			fallthrough
		case TypeDeletion:
			key, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			if cfID == 0 {
				if err := handler.Delete(key); err != nil {
					return err
				}
			} else {
				if err := handler.DeleteCF(cfID, key); err != nil {
					return err
				}
			}

		case TypeColumnFamilySingleDeletion:
			cfID, data, err = decodeVarint32(data)
			if err != nil {
				return err
			}
			fallthrough
		case TypeSingleDeletion:
			key, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			if cfID == 0 {
				if err := handler.SingleDelete(key); err != nil {
					return err
				}
			} else {
				if err := handler.SingleDeleteCF(cfID, key); err != nil {
					return err
				}
			}

		case TypeColumnFamilyMerge:
			cfID, data, err = decodeVarint32(data)
			if err != nil {
				return err
			}
			fallthrough
		case TypeMerge:
			key, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			value, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			if cfID == 0 {
				if err := handler.Merge(key, value); err != nil {
					return err
				}
			} else {
				if err := handler.MergeCF(cfID, key, value); err != nil {
					return err
				}
			}

		case TypeColumnFamilyRangeDeletion:
			cfID, data, err = decodeVarint32(data)
			if err != nil {
				return err
			}
			key, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			value, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			if err := handler.DeleteRangeCF(cfID, key, value); err != nil {
				return err
			}

		case TypeRangeDeletion:
			key, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			value, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			if err := handler.DeleteRange(key, value); err != nil {
				return err
			}

		case TypeLogData:
			var blob []byte
			blob, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			handler.LogData(blob)

		case TypeNoop:
			// No-op, just continue

		// 2PC (Two-Phase Commit) markers
		case TypeBeginPrepareXID, TypeBeginPersistedPrepareXID, TypeBeginUnprepareXID:
			// Check if handler supports 2PC
			if h2pc, ok := handler.(Handler2PC); ok {
				unprepared := (tag == TypeBeginUnprepareXID)
				if err := h2pc.MarkBeginPrepare(unprepared); err != nil {
					return err
				}
			}
			// If handler doesn't support 2PC, silently skip

		case TypeEndPrepareXID:
			// Read the XID (transaction name)
			var xid []byte
			xid, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			if h2pc, ok := handler.(Handler2PC); ok {
				if err := h2pc.MarkEndPrepare(xid); err != nil {
					return err
				}
			}

		case TypeCommitXID, TypeCommitXIDAndTimestamp:
			// Read the XID
			var xid []byte
			xid, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			// For TypeCommitXIDAndTimestamp, also read timestamp (skip it for now)
			if tag == TypeCommitXIDAndTimestamp {
				// Skip 8 bytes for timestamp
				if len(data) < 8 {
					return ErrCorrupted
				}
				data = data[8:]
			}
			if h2pc, ok := handler.(Handler2PC); ok {
				if err := h2pc.MarkCommit(xid); err != nil {
					return err
				}
			}

		case TypeRollbackXID:
			// Read the XID
			var xid []byte
			xid, data, err = decodeLengthPrefixed(data)
			if err != nil {
				return err
			}
			if h2pc, ok := handler.(Handler2PC); ok {
				if err := h2pc.MarkRollback(xid); err != nil {
					return err
				}
			}

		default:
			return ErrCorrupted
		}
	}

	return nil
}

func decodeVarint32(data []byte) (uint32, []byte, error) {
	v, n, err := encoding.DecodeVarint32(data)
	if err != nil {
		return 0, nil, ErrCorrupted
	}
	return v, data[n:], nil
}

func decodeLengthPrefixed(data []byte) ([]byte, []byte, error) {
	if len(data) == 0 {
		return nil, nil, ErrCorrupted
	}
	length, n, err := encoding.DecodeVarint32(data)
	if err != nil {
		return nil, nil, ErrCorrupted
	}
	data = data[n:]
	if len(data) < int(length) {
		return nil, nil, ErrCorrupted
	}
	value := data[:length]
	return value, data[length:], nil
}
