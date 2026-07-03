// Package num provides safe narrowing conversions to fixed-width integer types.
package num

import (
	"fmt"
	"math"
)

// ClampInt32 narrows n to int32, saturating at the int32 bounds instead of
// silently wrapping. Use for local, bounded quantities (row/retry/node counts,
// durations) where an out-of-range value cannot legitimately occur, so clamping
// is safe and hard-failing would be overkill.
func ClampInt32[T ~int | ~int64](n T) int32 {
	switch {
	case n > math.MaxInt32:
		return math.MaxInt32
	case n < math.MinInt32:
		return math.MinInt32
	default:
		return int32(n) //nolint:gosec // G115: bounds-checked above
	}
}

// Int32 narrows n to int32, returning an error naming the field if n falls
// outside the int32 range. Use when the value crosses a trust boundary (e.g. an
// inbound event payload) where an out-of-range value signals corruption and must
// fail loudly rather than be silently clamped.
func Int32[T ~int | ~int64](n T, field string) (int32, error) {
	if n < math.MinInt32 || n > math.MaxInt32 {
		return 0, fmt.Errorf("%s value %d overflows int32", field, n)
	}
	return int32(n), nil //nolint:gosec // G115: bounds-checked above
}
