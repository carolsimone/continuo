// Package trim decides how far a Redis stream can be trimmed: up to the oldest
// entry any consumer group declared in the contract still needs, and past the
// retention cutoff only after the entries a lagging group still needs are
// quarantined.
package trim

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// StreamID is a Redis stream entry id, "<ms>-<seq>".
type StreamID struct{ Ms, Seq uint64 }

// Zero is the id before every entry.
var Zero StreamID

// ParseID parses "<ms>-<seq>".
func ParseID(s string) (StreamID, error) {
	ms, seq, ok := strings.Cut(s, "-")
	if !ok || strings.Contains(seq, "-") {
		return Zero, fmt.Errorf("stream id %q: want <ms>-<seq>", s)
	}
	m, err := strconv.ParseUint(ms, 10, 64)
	if err != nil {
		return Zero, fmt.Errorf("stream id %q: %w", s, err)
	}
	q, err := strconv.ParseUint(seq, 10, 64)
	if err != nil {
		return Zero, fmt.Errorf("stream id %q: %w", s, err)
	}
	return StreamID{Ms: m, Seq: q}, nil
}

// IDAt is the first id at time t.
func IDAt(t time.Time) StreamID {
	return StreamID{Ms: uint64(t.UnixMilli())} //nolint:gosec // G115: stream-id milliseconds are non-negative and fit in uint64
}

func (a StreamID) String() string {
	return strconv.FormatUint(a.Ms, 10) + "-" + strconv.FormatUint(a.Seq, 10)
}

// Less reports whether a sorts before b.
func (a StreamID) Less(b StreamID) bool { return a.Ms < b.Ms || (a.Ms == b.Ms && a.Seq < b.Seq) }

// IsZero reports whether a is Zero.
func (a StreamID) IsZero() bool { return a == Zero }

// Next is the smallest id after a.
func (a StreamID) Next() StreamID {
	if a.Seq == math.MaxUint64 {
		return StreamID{Ms: a.Ms + 1}
	}
	return StreamID{Ms: a.Ms, Seq: a.Seq + 1}
}

// Time is when an entry with id a was added.
func (a StreamID) Time() time.Time {
	return time.UnixMilli(int64(a.Ms)).UTC() //nolint:gosec // G115: stream-id milliseconds fit in int64
}

// Entry is one stream entry.
type Entry struct {
	ID     StreamID
	Fields map[string]string
}
