// File: orchestrator/service/queries/truncate_internal_test.go
//
// White-box unit tests for the private byte-cap truncation helper, isolated
// from difflib's output formatting so the rune-boundary math is verified
// exactly rather than probabilistically.
package queries

import (
	"strings"
	"testing"
)

func TestTruncateToByteCap_WalksBackToRuneStart(t *testing.T) {
	// "é" is a 2-byte UTF-8 rune (0xC3 0xA9). Ten of them is a 20-byte string;
	// a 15-byte cap lands on the low byte of the 8th rune (mid-rune), so the
	// cut must walk back to 14 bytes — 7 complete runes.
	s := strings.Repeat("é", 10)
	got, truncated := truncateToByteCap(s, 15)
	if !truncated {
		t.Fatalf("expected truncated=true")
	}
	want := strings.Repeat("é", 7)
	if got != want {
		t.Fatalf("got %q (%d bytes), want %q (%d bytes)", got, len(got), want, len(want))
	}
}

func TestTruncateToByteCap_ExactRuneBoundaryNeedsNoWalkBack(t *testing.T) {
	// A 16-byte cap on the same 20-byte string already lands exactly on a
	// rune boundary (8 complete runes); no walk-back should occur beyond that.
	s := strings.Repeat("é", 10)
	got, truncated := truncateToByteCap(s, 16)
	if !truncated {
		t.Fatalf("expected truncated=true")
	}
	want := strings.Repeat("é", 8)
	if got != want {
		t.Fatalf("got %q (%d bytes), want %q (%d bytes)", got, len(got), want, len(want))
	}
}

func TestTruncateToByteCap_UnderCapReturnsUnchanged(t *testing.T) {
	s := "hello"
	got, truncated := truncateToByteCap(s, 100)
	if truncated {
		t.Fatalf("expected truncated=false")
	}
	if got != s {
		t.Fatalf("got %q, want unchanged %q", got, s)
	}
}

func TestTruncateToByteCap_EmptyStringUnaffected(t *testing.T) {
	got, truncated := truncateToByteCap("", 10)
	if truncated {
		t.Fatalf("expected truncated=false for empty input")
	}
	if got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}
