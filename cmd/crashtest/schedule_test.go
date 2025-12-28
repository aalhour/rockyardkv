package main

import (
	"testing"
	"time"
)

func TestParseCrashSchedule(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		got, err := parseCrashSchedule("1s, 250ms,5s")
		if err != nil {
			t.Fatalf("expected nil err, got %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(got))
		}
		if got[0] != time.Second || got[1] != 250*time.Millisecond || got[2] != 5*time.Second {
			t.Fatalf("unexpected schedule: %#v", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		_, err := parseCrashSchedule("")
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("invalid_duration", func(t *testing.T) {
		_, err := parseCrashSchedule("wat")
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("non_positive", func(t *testing.T) {
		_, err := parseCrashSchedule("0s")
		if err == nil {
			t.Fatalf("expected error")
		}
	})
}
