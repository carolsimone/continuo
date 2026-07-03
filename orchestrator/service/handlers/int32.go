package handlers

import "math"

// toInt32 bounds-checks a task-count int before narrowing it to the proto
// wire type used by RunEntriesDispatched.TotalTaskCount. The count is a Go
// slice length (number of dispatched tasks in one run), which cannot
// realistically approach MaxInt32, but this clamps defensively instead of
// trusting that invariant silently.
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
