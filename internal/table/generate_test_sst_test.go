package table

import (
	"os"
	"testing"

	"github.com/aalhour/rockyardkv/internal/dbformat"
)

// TestGenerateGoSST creates an SST file that can be tested with C++ RocksDB.
// Run with: go test -run TestGenerateGoSST -v
// Then test with: sst_dump --file=/tmp/go_generated.sst --command=scan
func TestGenerateGoSST(t *testing.T) {
	path := "/tmp/go_generated.sst"
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	opts := DefaultBuilderOptions()
	builder := NewTableBuilder(file, opts)

	// Add some entries
	key1 := dbformat.AppendInternalKey(nil, &dbformat.ParsedInternalKey{
		UserKey:  []byte("gokey1"),
		Sequence: 100,
		Type:     dbformat.TypeValue,
	})
	key2 := dbformat.AppendInternalKey(nil, &dbformat.ParsedInternalKey{
		UserKey:  []byte("gokey2"),
		Sequence: 101,
		Type:     dbformat.TypeValue,
	})

	builder.Add(key1, []byte("govalue1"))
	builder.Add(key2, []byte("govalue2"))

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}
	file.Close()

	t.Logf("Created Go SST file: %s", path)
	t.Log("Test with C++ RocksDB:")
	t.Log("  cd $ROCKSDB_PATH && DYLD_LIBRARY_PATH=. ./sst_dump --file=/tmp/go_generated.sst --command=scan")
}
