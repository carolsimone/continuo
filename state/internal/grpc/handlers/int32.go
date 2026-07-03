package handlers

import "math"

// toInt32 bounds-checks an int count/metric before narrowing it to the int32
// proto wire type. The values passed through here (row counts, retry counts,
// percentages, durations in seconds) are all small in practice, but this
// clamps defensively instead of trusting that invariant silently.
func toInt32(n int) int32 {
	switch {
	case n > math.MaxInt32:
		return math.MaxInt32
	case n < math.MinInt32:
		return math.MinInt32
	default:
		return int32(n)
	}
}
