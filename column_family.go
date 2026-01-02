package rockyardkv

// column_family.go implements column family management.
//
// Column families allow logically partitioning data within a single database.
// Each column family has its own memtable and set of SST files.
//
// Reference: RocksDB v10.7.5
//   - db/column_family.h
//   - db/column_family.cc

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/aalhour/rockyardkv/internal/memtable"
)

// DefaultColumnFamilyName is the name of the default column family.
const DefaultColumnFamilyName = "default"

// DefaultColumnFamilyID is the ID of the default column family.
const DefaultColumnFamilyID uint32 = 0

var (
	// ErrColumnFamilyNotFound is returned when a column family is not found.
	ErrColumnFamilyNotFound = errors.New("db: column family not found")

	// ErrColumnFamilyExists is returned when a column family already exists.
	ErrColumnFamilyExists = errors.New("db: column family already exists")

	// ErrInvalidColumnFamilyHandle is returned when a column family handle is invalid.
	ErrInvalidColumnFamilyHandle = errors.New("db: invalid column family handle")

	// ErrCannotDropDefaultCF is returned when trying to drop the default column family.
	ErrCannotDropDefaultCF = errors.New("db: cannot drop default column family")
)

// ColumnFamilyHandle represents a reference to a column family.
// It can be passed to DB operations to specify which column family to use.
type ColumnFamilyHandle interface {
	// ID returns the column family ID.
	ID() uint32

	// Name returns the column family name.
	Name() string

	// IsValid returns true if the handle is still valid (not dropped).
	IsValid() bool
}

// ColumnFamilyOptions contains options for creating a column family.
type ColumnFamilyOptions struct {
	// Comparator for ordering keys within the column family.
	// If nil, uses the database's default comparator.
	Comparator Comparator

	// WriteBufferSize is the amount of data to build up in memory
	// before converting to a sorted on-disk file.
	WriteBufferSize int
}

// DefaultColumnFamilyOptions returns default options for a column family.
func DefaultColumnFamilyOptions() ColumnFamilyOptions {
	return ColumnFamilyOptions{
		Comparator:      nil,
		WriteBufferSize: 4 * 1024 * 1024, // 4MB
	}
}

// columnFamilyData holds the internal data for a column family.
type columnFamilyData struct {
	id      uint32
	name    string
	options ColumnFamilyOptions

	// Memtable for this column family
	mem   *memtable.MemTable
	imm   []*memtable.MemTable // Immutable memtables pending flush
	memMu sync.RWMutex

	// Reference counting
	refs int32

	// Set to true when the column family is dropped
	dropped atomic.Bool

	// The parent database
	db *dbImpl
}

// newColumnFamilyData creates a new column family data.
func newColumnFamilyData(id uint32, name string, opts ColumnFamilyOptions, db *dbImpl) *columnFamilyData {
	var cmp memtable.Comparator
	if opts.Comparator != nil {
		cmp = memtable.Comparator(opts.Comparator.Compare)
	}

	return &columnFamilyData{
		id:      id,
		name:    name,
		options: opts,
		mem:     memtable.NewMemTable(cmp),
		refs:    1,
		db:      db,
	}
}

// ref increments the reference count.
func (cfd *columnFamilyData) ref() {
	atomic.AddInt32(&cfd.refs, 1)
}

// unref decrements the reference count.
func (cfd *columnFamilyData) unref() {
	if atomic.AddInt32(&cfd.refs, -1) == 0 {
		// Clean up resources
		cfd.mem = nil
		cfd.imm = nil
	}
}

// columnFamilyHandle implements ColumnFamilyHandle.
type columnFamilyHandle struct {
	cfd *columnFamilyData
}

// ID returns the column family ID.
func (h *columnFamilyHandle) ID() uint32 {
	return h.cfd.id
}

// Name returns the column family name.
func (h *columnFamilyHandle) Name() string {
	return h.cfd.name
}

// IsValid returns true if the handle is still valid.
func (h *columnFamilyHandle) IsValid() bool {
	return h.cfd != nil && !h.cfd.dropped.Load()
}

// columnFamilySet manages all column families in a database.
type columnFamilySet struct {
	mu sync.RWMutex

	// Map from name to column family data
	byName map[string]*columnFamilyData

	// Map from ID to column family data
	byID map[uint32]*columnFamilyData

	// Next column family ID to assign
	nextCFID uint32

	// The default column family
	defaultCF *columnFamilyData

	// Parent database
	db *dbImpl
}

// newColumnFamilySet creates a new column family set.
func newColumnFamilySet(db *dbImpl) *columnFamilySet {
	cfs := &columnFamilySet{
		byName:   make(map[string]*columnFamilyData),
		byID:     make(map[uint32]*columnFamilyData),
		nextCFID: 1, // Default CF uses ID 0
		db:       db,
	}

	// Create the default column family
	defaultCF := newColumnFamilyData(DefaultColumnFamilyID, DefaultColumnFamilyName, DefaultColumnFamilyOptions(), db)
	cfs.byName[DefaultColumnFamilyName] = defaultCF
	cfs.byID[DefaultColumnFamilyID] = defaultCF
	cfs.defaultCF = defaultCF

	return cfs
}

// getDefault returns the default column family.
func (cfs *columnFamilySet) getDefault() *columnFamilyData {
	return cfs.defaultCF
}

// getByName returns the column family with the given name.
func (cfs *columnFamilySet) getByName(name string) *columnFamilyData {
	cfs.mu.RLock()
	defer cfs.mu.RUnlock()
	return cfs.byName[name]
}

// getByID returns the column family with the given ID.
func (cfs *columnFamilySet) getByID(id uint32) *columnFamilyData {
	cfs.mu.RLock()
	defer cfs.mu.RUnlock()
	return cfs.byID[id]
}

// create creates a new column family.
func (cfs *columnFamilySet) create(name string, opts ColumnFamilyOptions) (*columnFamilyData, error) {
	cfs.mu.Lock()
	defer cfs.mu.Unlock()

	if _, exists := cfs.byName[name]; exists {
		return nil, ErrColumnFamilyExists
	}

	id := cfs.nextCFID
	cfs.nextCFID++

	cfd := newColumnFamilyData(id, name, opts, cfs.db)
	cfs.byName[name] = cfd
	cfs.byID[id] = cfd

	return cfd, nil
}

// drop marks a column family as dropped.
func (cfs *columnFamilySet) drop(cfd *columnFamilyData) error {
	cfs.mu.Lock()
	defer cfs.mu.Unlock()

	if cfd.id == DefaultColumnFamilyID {
		return ErrCannotDropDefaultCF
	}

	if cfd.dropped.Load() {
		return ErrColumnFamilyNotFound
	}

	cfd.dropped.Store(true)
	delete(cfs.byName, cfd.name)
	delete(cfs.byID, cfd.id)
	cfd.unref()

	return nil
}

// listNames returns the names of all column families.
func (cfs *columnFamilySet) listNames() []string {
	cfs.mu.RLock()
	defer cfs.mu.RUnlock()

	names := make([]string, 0, len(cfs.byName))
	for name := range cfs.byName {
		names = append(names, name)
	}
	return names
}

// count returns the number of column families.
func (cfs *columnFamilySet) count() int {
	cfs.mu.RLock()
	defer cfs.mu.RUnlock()
	return len(cfs.byName)
}

// nextID returns the next column family ID that will be assigned.
func (cfs *columnFamilySet) nextID() uint32 {
	cfs.mu.RLock()
	defer cfs.mu.RUnlock()
	return cfs.nextCFID
}

// setNextID sets the next column family ID (used during recovery).
func (cfs *columnFamilySet) setNextID(id uint32) {
	cfs.mu.Lock()
	defer cfs.mu.Unlock()
	if id > cfs.nextCFID {
		cfs.nextCFID = id
	}
}

// createWithID creates a column family with a specific ID (used during recovery).
func (cfs *columnFamilySet) createWithID(id uint32, name string, opts ColumnFamilyOptions) (*columnFamilyData, error) {
	cfs.mu.Lock()
	defer cfs.mu.Unlock()

	if _, exists := cfs.byName[name]; exists {
		return nil, ErrColumnFamilyExists
	}

	cfd := newColumnFamilyData(id, name, opts, cfs.db)
	cfs.byName[name] = cfd
	cfs.byID[id] = cfd

	// Update nextID if necessary
	if id >= cfs.nextCFID {
		cfs.nextCFID = id + 1
	}

	return cfd, nil
}

// forEach calls the given function for each column family.
func (cfs *columnFamilySet) forEach(fn func(*columnFamilyData)) {
	cfs.mu.RLock()
	defer cfs.mu.RUnlock()

	for _, cfd := range cfs.byName {
		fn(cfd)
	}
}

// getColumnFamilyData resolves a ColumnFamilyHandle to its internal data.
// If cf is nil, returns the default column family.
func (db *dbImpl) getColumnFamilyData(cf ColumnFamilyHandle) (*columnFamilyData, error) {
	if cf == nil {
		return db.columnFamilies.getDefault(), nil
	}

	handle, ok := cf.(*columnFamilyHandle)
	if !ok || handle == nil || handle.cfd == nil {
		return nil, ErrInvalidColumnFamilyHandle
	}

	if !handle.IsValid() {
		return nil, ErrColumnFamilyNotFound
	}

	return handle.cfd, nil
}
