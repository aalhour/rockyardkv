package rockyardkv_test

import (
	"fmt"
	"os"

	"github.com/aalhour/rockyardkv"
)

func ExampleOpen() {
	dir, err := os.MkdirTemp("", "rockyardkv-example-*")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	opts := rockyardkv.DefaultOptions()
	opts.CreateIfMissing = true

	db, err := rockyardkv.Open(dir, opts)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Put(rockyardkv.DefaultWriteOptions(), []byte("k"), []byte("v")); err != nil {
		panic(err)
	}

	val, err := db.Get(rockyardkv.DefaultReadOptions(), []byte("k"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(val))
	// Output:
	// v
}
