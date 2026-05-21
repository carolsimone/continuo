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

// Now returns time.Now in UTC.
func (SystemClock) Now() time.Time { return time.Now().UTC() }
