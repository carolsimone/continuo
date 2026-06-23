package ports

import "time"

// Clock abstracts the current time so handlers stay deterministic in tests.
type Clock interface {
	Now() time.Time
}

// SystemClock is the production Clock backed by the wall clock (UTC).
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }
