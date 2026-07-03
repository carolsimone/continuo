package run

import "math"

// toInt32 bounds-checks a slice-length count before narrowing it to the
// int32 aggregate field it is stored in. The count here is the number of
// tasks projected for one run, which cannot realistically approach
// math.MaxInt32, but this clamps defensively instead of trusting that
// invariant silently.
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
