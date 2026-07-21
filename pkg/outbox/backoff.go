package outbox

import "time"

// backoff returns the delay before the given 1-based attempt, as capped
// exponential growth: base * 2^(attempt-1), clamped to max. attempt is the
// number of failures recorded so far (retry_count after this failure). A
// non-positive attempt is treated as the first (returns base).
//
// The cap is checked BEFORE each doubling: once delay would reach or exceed max
// on the next step, max is returned immediately. This bounds the loop to
// ~log2(max/base) iterations and guarantees delay never doubles past max, so
// the int64 duration can never overflow (and wrap negative) regardless of how
// large attempt or max is.
func backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base >= max {
		return max
	}
	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= max/2 {
			return max
		}
		delay *= 2
	}
	return delay
}
