// wide_columns_test.go implements tests for wide columns.
package rockyardkv

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestEncodeDecodeWideColumns(t *testing.T) {
	tests := []struct {
		name    string
		columns WideColumns
	}{
		{
			name:    "empty",
			columns: WideColumns{},
		},
		{
			name: "single_column",
			columns: WideColumns{
				{Name: []byte("name"), Value: []byte("John")},
			},
		},
		{
			name: "multiple_columns",
			columns: WideColumns{
				{Name: []byte("name"), Value: []byte("John")},
				{Name: []byte("age"), Value: []byte("30")},
				{Name: []byte("city"), Value: []byte("NYC")},
			},
		},
		{
			name: "binary_values",
			columns: WideColumns{
				{Name: []byte("data"), Value: []byte{0x00, 0x01, 0x02, 0xFF}},
			},
		},
		{
			name: "empty_values",
			columns: WideColumns{
				{Name: []byte("empty"), Value: []byte{}},
				{Name: []byte("null"), Value: nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeWideColumns(tt.columns)
			if err != nil {
				t.Fatalf("EncodeWideColumns failed: %v", err)
			}

			decoded, err := DecodeWideColumns(encoded)
			if err != nil {
				t.Fatalf("DecodeWideColumns failed: %v", err)
			}

			if len(decoded) != len(tt.columns) {
				t.Fatalf("Column count mismatch: got %d, want %d", len(decoded), len(tt.columns))
			}

			// Note: decoded columns are sorted by name
			decoded.Sort()
			sortedOriginal := make(WideColumns, len(tt.columns))
			copy(sortedOriginal, tt.columns)
			sortedOriginal.Sort()

			for i := range decoded {
				if !bytes.Equal(decoded[i].Name, sortedOriginal[i].Name) {
					t.Errorf("Column %d name mismatch: got %q, want %q",
						i, decoded[i].Name, sortedOriginal[i].Name)
				}
				if !bytes.Equal(decoded[i].Value, sortedOriginal[i].Value) {
					t.Errorf("Column %d value mismatch", i)
				}
			}
		})
	}
}

func TestWideColumnsGet(t *testing.T) {
	columns := WideColumns{
		{Name: []byte("name"), Value: []byte("John")},
		{Name: []byte("age"), Value: []byte("30")},
		{Name: []byte("city"), Value: []byte("NYC")},
	}

	// Test Get by []byte name
	if v := columns.Get([]byte("name")); !bytes.Equal(v, []byte("John")) {
		t.Errorf("Get(name) = %q, want John", v)
	}

	// Test GetString
	if v := columns.GetString("age"); !bytes.Equal(v, []byte("30")) {
		t.Errorf("GetString(age) = %q, want 30", v)
	}

	// Test missing column
	if v := columns.Get([]byte("missing")); v != nil {
		t.Errorf("Get(missing) = %q, want nil", v)
	}
}

func TestWideColumnsSort(t *testing.T) {
	columns := WideColumns{
		{Name: []byte("zebra"), Value: []byte("1")},
		{Name: []byte("alpha"), Value: []byte("2")},
		{Name: []byte("beta"), Value: []byte("3")},
	}

	columns.Sort()

	expected := []string{"alpha", "beta", "zebra"}
	for i, col := range columns {
		if string(col.Name) != expected[i] {
			t.Errorf("Column %d: got %q, want %q", i, col.Name, expected[i])
		}
	}
}

func TestWideColumnDB(t *testing.T) {
	dir, err := os.MkdirTemp("", "wide-column-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	wcdb := NewWideColumnDB(database.(*DBImpl))

	// Test PutEntity
	user := WideColumns{
		{Name: []byte("name"), Value: []byte("Alice")},
		{Name: []byte("email"), Value: []byte("alice@example.com")},
		{Name: []byte("age"), Value: []byte("28")},
	}

	if err := wcdb.PutEntity([]byte("user:1"), user); err != nil {
		t.Fatalf("PutEntity failed: %v", err)
	}

	// Test GetEntity
	result, err := wcdb.GetEntity([]byte("user:1"))
	if err != nil {
		t.Fatalf("GetEntity failed: %v", err)
	}
	defer result.Release()

	if len(result.Columns) != 3 {
		t.Errorf("Column count = %d, want 3", len(result.Columns))
	}

	if v := result.Columns.GetString("name"); !bytes.Equal(v, []byte("Alice")) {
		t.Errorf("name = %q, want Alice", v)
	}
	if v := result.Columns.GetString("email"); !bytes.Equal(v, []byte("alice@example.com")) {
		t.Errorf("email = %q, want alice@example.com", v)
	}

	// Test entity not found
	_, err = wcdb.GetEntity([]byte("user:nonexistent"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}

	// Test DeleteEntity
	if err := wcdb.DeleteEntity([]byte("user:1")); err != nil {
		t.Fatalf("DeleteEntity failed: %v", err)
	}

	_, err = wcdb.GetEntity([]byte("user:1"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}
}

func TestWideColumnDBMerge(t *testing.T) {
	dir, err := os.MkdirTemp("", "wide-column-merge-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	wcdb := NewWideColumnDB(database.(*DBImpl))

	// Initial entity
	initial := WideColumns{
		{Name: []byte("name"), Value: []byte("Bob")},
		{Name: []byte("age"), Value: []byte("25")},
	}
	if err := wcdb.PutEntity([]byte("user:2"), initial); err != nil {
		t.Fatalf("PutEntity failed: %v", err)
	}

	// Merge new columns
	update := WideColumns{
		{Name: []byte("age"), Value: []byte("26")},  // Update existing
		{Name: []byte("city"), Value: []byte("LA")}, // Add new
	}
	if err := wcdb.MergeEntity([]byte("user:2"), update); err != nil {
		t.Fatalf("MergeEntity failed: %v", err)
	}

	// Verify merge
	result, err := wcdb.GetEntity([]byte("user:2"))
	if err != nil {
		t.Fatalf("GetEntity failed: %v", err)
	}
	defer result.Release()

	if len(result.Columns) != 3 {
		t.Errorf("Column count = %d, want 3", len(result.Columns))
	}

	// Check values
	if v := result.Columns.GetString("name"); !bytes.Equal(v, []byte("Bob")) {
		t.Errorf("name = %q, want Bob", v)
	}
	if v := result.Columns.GetString("age"); !bytes.Equal(v, []byte("26")) {
		t.Errorf("age = %q, want 26", v)
	}
	if v := result.Columns.GetString("city"); !bytes.Equal(v, []byte("LA")) {
		t.Errorf("city = %q, want LA", v)
	}
}

func TestWideColumnDBMergeNonExistent(t *testing.T) {
	dir, err := os.MkdirTemp("", "wide-column-merge-new-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := DefaultOptions()
	opts.CreateIfMissing = true

	database, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer database.Close()

	wcdb := NewWideColumnDB(database.(*DBImpl))

	// Merge into non-existent entity should create it
	cols := WideColumns{
		{Name: []byte("foo"), Value: []byte("bar")},
	}
	if err := wcdb.MergeEntity([]byte("new:entity"), cols); err != nil {
		t.Fatalf("MergeEntity failed: %v", err)
	}

	// Verify entity exists
	result, err := wcdb.GetEntity([]byte("new:entity"))
	if err != nil {
		t.Fatalf("GetEntity failed: %v", err)
	}
	defer result.Release()

	if v := result.Columns.GetString("foo"); !bytes.Equal(v, []byte("bar")) {
		t.Errorf("foo = %q, want bar", v)
	}
}

func TestDecodeWideColumnsErrors(t *testing.T) {
	// Empty data should return nil
	cols, err := DecodeWideColumns(nil)
	if err != nil || cols != nil {
		t.Errorf("DecodeWideColumns(nil) = %v, %v; want nil, nil", cols, err)
	}

	// Truncated data
	truncated := []byte{0x02} // Claims 2 columns but has no data
	_, err = DecodeWideColumns(truncated)
	if err == nil {
		t.Error("Expected error for truncated data")
	}
}

func BenchmarkEncodeWideColumns(b *testing.B) {
	columns := WideColumns{
		{Name: []byte("field1"), Value: []byte("value1")},
		{Name: []byte("field2"), Value: []byte("value2")},
		{Name: []byte("field3"), Value: []byte("value3")},
		{Name: []byte("field4"), Value: []byte("value4")},
		{Name: []byte("field5"), Value: []byte("value5")},
	}

	for b.Loop() {
		_, _ = EncodeWideColumns(columns)
	}
}

func BenchmarkDecodeWideColumns(b *testing.B) {
	columns := WideColumns{
		{Name: []byte("field1"), Value: []byte("value1")},
		{Name: []byte("field2"), Value: []byte("value2")},
		{Name: []byte("field3"), Value: []byte("value3")},
		{Name: []byte("field4"), Value: []byte("value4")},
		{Name: []byte("field5"), Value: []byte("value5")},
	}
	encoded, _ := EncodeWideColumns(columns)

	for b.Loop() {
		_, _ = DecodeWideColumns(encoded)
	}
}
