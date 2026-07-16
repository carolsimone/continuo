package ports

import "time"

// Clock reports the current time. The lease lifecycle stamps claim, heartbeat,
// and terminal instants, and compares them against lease deadlines, so its
// application services take time as a dependency rather than reading the wall
// clock directly.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the wall clock in UTC.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }
