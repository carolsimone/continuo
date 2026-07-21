package outbox

import "time"

// backoff returns the delay before the given 1-based attempt, as capped
// exponential growth: base * 2^(attempt-1), clamped to max. attempt is the
// number of failures recorded so far (retry_count after this failure). A
// non-positive attempt is treated as the first (returns base). The doubling is
// computed in a loop with an early cap check so a large attempt can never
// overflow the shift.
func backoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= max {
			return max
		}
	}
	if delay > max {
		return max
	}
	return delay
}
