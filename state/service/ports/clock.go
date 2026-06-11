// Package ports declares the interfaces the application layer depends on.
// Implementations live in state/adapters. This package depends only on the
// domain layer and the stdlib.
package ports

import "time"

// Clock abstracts time.Now for testability.
type Clock interface {
	Now() time.Time
}

// SystemClock returns time.Now and is the production implementation.
type SystemClock struct{}

// Now returns the current UTC time truncated to microsecond resolution.
//
// Every instant this clock mints is eventually persisted to a PostgreSQL
// timestamptz column, which stores microsecond precision and rounds away any
// finer nanoseconds. Truncating here means the value held in memory (and
// returned in a gRPC response) is byte-for-byte the value that round-trips
// from the row, so "response timestamp == persisted timestamp" holds for a
// single logical event instead of differing in the sub-microsecond digits.
func (SystemClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }
