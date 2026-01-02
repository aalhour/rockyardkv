package rockyardkv

// prefix_extractor.go implements PrefixExtractor (SliceTransform in C++) for prefix seek optimization.
//
// Prefix seek allows efficient iteration over keys with a common prefix.
// When a prefix extractor is configured:
// 1. Bloom filters are built for prefixes instead of whole keys
// 2. During iteration, blocks without the target prefix can be skipped
// 3. Seek(prefix) + Next() efficiently iterates within a prefix
//
// Reference: RocksDB v10.7.5
//   - include/rocksdb/slice_transform.h
//   - db/db_iter.cc (prefix seeking logic)

// PrefixExtractor extracts prefixes from keys for prefix-based operations.
// This is equivalent to RocksDB's SliceTransform interface.
//
// IMPORTANT: Together PrefixExtractor and Comparator must satisfy:
//
//	If Compare(k1, k2) <= 0 and Compare(k2, k3) <= 0 and
//	   InDomain(k1) and InDomain(k3) and Transform(k1) == Transform(k3),
//	Then InDomain(k2) and Transform(k2) == Transform(k1)
//
// In other words, all keys with the same prefix must be contiguous by comparator order.
type PrefixExtractor interface {
	// Name returns a unique identifier for this prefix extractor.
	// The name is stored in SST files and used for compatibility checks.
	Name() string

	// Transform extracts the prefix from the given key.
	// The returned slice may reference the input key's memory.
	// REQUIRES: InDomain(key) == true
	Transform(key []byte) []byte

	// InDomain returns true if the key has a valid prefix.
	// If false, the key is considered "out of domain" and prefix bloom
	// filters will not be used for it.
	InDomain(key []byte) bool
}

// FixedPrefixExtractor uses the first n bytes of each key as the prefix.
// Keys shorter than n bytes are out of domain.
type FixedPrefixExtractor struct {
	prefixLen int
}

// NewFixedPrefixExtractor creates a prefix extractor that uses the first n bytes.
func NewFixedPrefixExtractor(prefixLen int) *FixedPrefixExtractor {
	if prefixLen <= 0 {
		prefixLen = 1
	}
	return &FixedPrefixExtractor{prefixLen: prefixLen}
}

// Name returns the extractor name.
func (e *FixedPrefixExtractor) Name() string {
	return "rocksdb.FixedPrefix"
}

// Transform extracts the prefix from the key.
func (e *FixedPrefixExtractor) Transform(key []byte) []byte {
	if len(key) < e.prefixLen {
		return key
	}
	return key[:e.prefixLen]
}

// InDomain returns true if the key has at least prefixLen bytes.
func (e *FixedPrefixExtractor) InDomain(key []byte) bool {
	return len(key) >= e.prefixLen
}

// CappedPrefixExtractor uses min(n, len(key)) bytes as the prefix.
// All keys are in domain.
type CappedPrefixExtractor struct {
	capLen int
}

// NewCappedPrefixExtractor creates a prefix extractor that uses up to n bytes.
func NewCappedPrefixExtractor(capLen int) *CappedPrefixExtractor {
	if capLen <= 0 {
		capLen = 1
	}
	return &CappedPrefixExtractor{capLen: capLen}
}

// Name returns the extractor name.
func (e *CappedPrefixExtractor) Name() string {
	return "rocksdb.CappedPrefix"
}

// Transform extracts the prefix from the key.
func (e *CappedPrefixExtractor) Transform(key []byte) []byte {
	if len(key) <= e.capLen {
		return key
	}
	return key[:e.capLen]
}

// InDomain always returns true for capped prefix extractor.
func (e *CappedPrefixExtractor) InDomain(key []byte) bool {
	return true
}

// NoopPrefixExtractor returns the entire key as the prefix.
// This effectively disables prefix optimization.
type NoopPrefixExtractor struct{}

// NewNoopPrefixExtractor creates a no-op prefix extractor.
func NewNoopPrefixExtractor() *NoopPrefixExtractor {
	return &NoopPrefixExtractor{}
}

// Name returns the extractor name.
func (e *NoopPrefixExtractor) Name() string {
	return "rocksdb.Noop"
}

// Transform returns the entire key.
func (e *NoopPrefixExtractor) Transform(key []byte) []byte {
	return key
}

// InDomain always returns true.
func (e *NoopPrefixExtractor) InDomain(key []byte) bool {
	return true
}
