package redis

import (
	"testing"
	"time"
)

// TestEmittedAtFromMsgID verifies that the millisecond prefix of a Redis stream
// message ID ("<unixMillis>-<seq>") is parsed into the publish time, and that an
// ID with no parseable millis prefix reports failure so the caller leaves
// EmittedAt zero (which keeps the completeness barrier closed).
func TestEmittedAtFromMsgID(t *testing.T) {
	got, ok := emittedAtFromMsgID("1700000000000-0")
	if !ok {
		t.Fatalf("want ok for a well-formed message ID")
	}
	if want := time.UnixMilli(1700000000000); !got.Equal(want) {
		t.Errorf("emittedAtFromMsgID = %v, want %v", got, want)
	}

	if _, ok := emittedAtFromMsgID("not-a-millis-0"); ok {
		t.Errorf("want !ok for a non-numeric millis prefix")
	}
	if _, ok := emittedAtFromMsgID("no-dash"); ok {
		t.Errorf("want !ok for an ID without the seq separator")
	}
}
