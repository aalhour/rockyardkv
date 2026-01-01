// wide_columns.go implements Wide Column support for entity-style storage.
//
// Wide Columns allow storing multiple named columns (attributes) for a single key,
// similar to a document store or wide-column database like Cassandra.
//
// Format (Value encoding):
//
//	[Number of Columns (varint)]
//	[Column 1: Name Length (varint) | Name | Value Length (varint) | Value]
//	[Column 2: ...]
//	...
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/wide_columns.h
//   - db/wide/wide_column_serialization.cc
package rockyardkv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sort"
)

// WideColumn represents a named column with a value.
type WideColumn struct {
	Name  []byte
	Value []byte
}

// WideColumns is a slice of WideColumn.
type WideColumns []WideColumn

// Len returns the number of columns.
func (wc WideColumns) Len() int { return len(wc) }

// Swap swaps two columns.
func (wc WideColumns) Swap(i, j int) { wc[i], wc[j] = wc[j], wc[i] }

// Less returns true if column i's name is less than column j's name.
func (wc WideColumns) Less(i, j int) bool {
	return bytes.Compare(wc[i].Name, wc[j].Name) < 0
}

// Sort sorts the columns by name.
func (wc WideColumns) Sort() {
	sort.Sort(wc)
}

// Get returns the value of a column by name, or nil if not found.
func (wc WideColumns) Get(name []byte) []byte {
	for _, col := range wc {
		if bytes.Equal(col.Name, name) {
			return col.Value
		}
	}
	return nil
}

// GetString returns the value of a column by name as a string.
func (wc WideColumns) GetString(name string) []byte {
	return wc.Get([]byte(name))
}

// PinnableWideColumns wraps WideColumns with a reference to underlying storage.
// Similar to RocksDB's PinnableWideColumns.
type PinnableWideColumns struct {
	Columns WideColumns
	pinned  bool // Indicates if columns are pinned to internal storage
}

// Release releases the pinned memory.
func (p *PinnableWideColumns) Release() {
	p.pinned = false
	p.Columns = nil
}

// Errors for wide column operations
var (
	ErrWideColumnTooShort = errors.New("wide_column: data too short")
	ErrWideColumnCorrupt  = errors.New("wide_column: corrupt data")
)

// EncodeWideColumns encodes wide columns to bytes.
func EncodeWideColumns(columns WideColumns) ([]byte, error) {
	// Sort columns by name for consistent encoding
	sortedColumns := make(WideColumns, len(columns))
	copy(sortedColumns, columns)
	sortedColumns.Sort()

	var buf bytes.Buffer

	// Write number of columns
	if err := writeVarInt(&buf, len(sortedColumns)); err != nil {
		return nil, err
	}

	// Write each column
	for _, col := range sortedColumns {
		// Write name length and name
		if err := writeVarInt(&buf, len(col.Name)); err != nil {
			return nil, err
		}
		buf.Write(col.Name)

		// Write value length and value
		if err := writeVarInt(&buf, len(col.Value)); err != nil {
			return nil, err
		}
		buf.Write(col.Value)
	}

	return buf.Bytes(), nil
}

// DecodeWideColumns decodes wide columns from bytes.
func DecodeWideColumns(data []byte) (WideColumns, error) {
	if len(data) == 0 {
		return nil, nil
	}

	r := bytes.NewReader(data)

	// Read number of columns
	numColumns, err := readVarInt(r)
	if err != nil {
		return nil, ErrWideColumnTooShort
	}

	columns := make(WideColumns, 0, numColumns)

	for range numColumns {
		// Read name
		nameLen, err := readVarInt(r)
		if err != nil {
			return nil, ErrWideColumnCorrupt
		}
		name := make([]byte, nameLen)
		if _, err := io.ReadFull(r, name); err != nil {
			return nil, ErrWideColumnCorrupt
		}

		// Read value
		valueLen, err := readVarInt(r)
		if err != nil {
			return nil, ErrWideColumnCorrupt
		}
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(r, value); err != nil {
			return nil, ErrWideColumnCorrupt
		}

		columns = append(columns, WideColumn{
			Name:  name,
			Value: value,
		})
	}

	return columns, nil
}

// Helper for varint encoding

func writeVarInt(w io.Writer, n int) error {
	buf := make([]byte, binary.MaxVarintLen64)
	size := binary.PutUvarint(buf, uint64(n))
	_, err := w.Write(buf[:size])
	return err
}

func readVarInt(r io.ByteReader) (int, error) {
	val, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, err
	}
	return int(val), nil
}

// WideColumnDBInterface defines the interface for wide column operations.
// This is an extension to the standard DB interface.
type WideColumnDBInterface interface {
	// PutEntity stores an entity (key with multiple named columns)
	PutEntity(key []byte, columns WideColumns) error

	// GetEntity retrieves an entity's columns
	GetEntity(key []byte) (*PinnableWideColumns, error)

	// DeleteEntity deletes an entity
	DeleteEntity(key []byte) error
}

// WideColumnDB wraps a DB to provide wide column operations.
type WideColumnDB struct {
	db *DBImpl
}

// NewWideColumnDB creates a new WideColumnDB wrapper.
func NewWideColumnDB(db *DBImpl) *WideColumnDB {
	return &WideColumnDB{db: db}
}

// PutEntity stores an entity (key with multiple named columns).
func (w *WideColumnDB) PutEntity(key []byte, columns WideColumns) error {
	encoded, err := EncodeWideColumns(columns)
	if err != nil {
		return err
	}
	return w.db.Put(nil, key, encoded)
}

// GetEntity retrieves an entity's columns.
func (w *WideColumnDB) GetEntity(key []byte) (*PinnableWideColumns, error) {
	value, err := w.db.Get(nil, key)
	if err != nil {
		return nil, err
	}

	columns, err := DecodeWideColumns(value)
	if err != nil {
		return nil, err
	}

	return &PinnableWideColumns{
		Columns: columns,
		pinned:  true,
	}, nil
}

// DeleteEntity deletes an entity.
func (w *WideColumnDB) DeleteEntity(key []byte) error {
	return w.db.Delete(nil, key)
}

// MergeEntity merges new columns into an existing entity.
// Existing columns with the same name are overwritten.
func (w *WideColumnDB) MergeEntity(key []byte, newColumns WideColumns) error {
	// Get existing entity
	existing, err := w.GetEntity(key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}

	var merged WideColumns
	if existing != nil && existing.Columns != nil {
		// Build a map for efficient lookup
		colMap := make(map[string][]byte)
		for _, col := range existing.Columns {
			colMap[string(col.Name)] = col.Value
		}

		// Overwrite with new columns
		for _, col := range newColumns {
			colMap[string(col.Name)] = col.Value
		}

		// Convert back to slice
		for name, value := range colMap {
			merged = append(merged, WideColumn{
				Name:  []byte(name),
				Value: value,
			})
		}
		existing.Release()
	} else {
		merged = newColumns
	}

	return w.PutEntity(key, merged)
}
