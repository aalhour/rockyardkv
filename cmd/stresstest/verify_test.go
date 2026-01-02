package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aalhour/rockyardkv"
	"github.com/aalhour/rockyardkv/internal/testutil"
	"github.com/aalhour/rockyardkv/vfs"
)

type fakeDB struct {
	rockyardkv.DB
	getFn func(opts *rockyardkv.ReadOptions, key []byte) ([]byte, error)
}

func (f fakeDB) Get(opts *rockyardkv.ReadOptions, key []byte) ([]byte, error) {
	return f.getFn(opts, key)
}

func TestVerifyAll_DeletedKeyReadError_ClassifiedAsErrorNotFound(t *testing.T) {
	// Save and restore global flags (verifyAll uses them directly).
	oldNumKeys := *numKeys
	oldVerbose := *verbose
	oldAllowDBAhead := *allowDBAhead
	oldAllowDataLoss := *allowDataLoss
	t.Cleanup(func() {
		*numKeys = oldNumKeys
		*verbose = oldVerbose
		*allowDBAhead = oldAllowDBAhead
		*allowDataLoss = oldAllowDataLoss
	})

	*numKeys = 1
	*verbose = true
	*allowDBAhead = false
	*allowDataLoss = false

	expected := testutil.NewExpectedStateV2(1, 1, 2) // key0 deleted by default
	stats := &Stats{}

	database := fakeDB{
		getFn: func(_ *rockyardkv.ReadOptions, _ []byte) ([]byte, error) {
			return nil, vfs.ErrInjectedReadError
		},
	}

	// Capture stdout since verifyAll prints classification lines there.
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	outCh := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		outCh <- buf.String()
	}()

	verifyErr := verifyAll(database, expected, stats, nil)

	_ = w.Close()
	os.Stdout = oldStdout
	out := <-outCh
	_ = r.Close()

	if verifyErr == nil {
		t.Fatalf("expected verification to fail when Get returns a non-ErrNotFound error")
	}
	if strings.Contains(out, "expected deleted but found") {
		t.Fatalf("misclassified read error as found; output:\n%s", out)
	}
	if !strings.Contains(out, "expected deleted but got error") {
		t.Fatalf("missing expected classification message; output:\n%s", out)
	}
}
