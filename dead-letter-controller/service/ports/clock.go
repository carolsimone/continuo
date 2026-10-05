// Package ports declares the technical collaborators the application layer uses.
package ports

import "time"

// Clock returns the current time.
type Clock interface{ Now() time.Time }

// SystemClock is the wall clock in UTC.
type SystemClock struct{}

// Now returns the current UTC time.
func (SystemClock) Now() time.Time { return time.Now().UTC() }
