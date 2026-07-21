package outbox

import (
	"math"
	"testing"
	"time"
)

func TestBackoff_CappedExponential(t *testing.T) {
	base := 5 * time.Second
	max := 5 * time.Minute
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 5 * time.Second},
		{2, 10 * time.Second},
		{3, 20 * time.Second},
		{4, 40 * time.Second},
		{5, 80 * time.Second},
		{6, 160 * time.Second},
		{7, 5 * time.Minute},   // 320s capped to 300s
		{8, 5 * time.Minute},   // capped
		{50, 5 * time.Minute},  // large attempt never overflows past the cap
	}
	for _, c := range cases {
		if got := backoff(c.attempt, base, max); got != c.want {
			t.Fatalf("backoff(%d)=%s, want %s", c.attempt, got, c.want)
		}
	}
}

func TestBackoff_NonPositiveAttemptIsBase(t *testing.T) {
	if got := backoff(0, 5*time.Second, time.Minute); got != 5*time.Second {
		t.Fatalf("backoff(0)=%s, want base 5s", got)
	}
}

// TestBackoff_NoOverflowWithHugeMax guards the int64 duration against wrapping
// negative. With a max near the int64 ceiling, naive `delay *= 2` before the cap
// check would overflow to a negative duration for a large attempt; the
// check-before-double form must instead clamp to max and stay positive.
func TestBackoff_NoOverflowWithHugeMax(t *testing.T) {
	huge := time.Duration(math.MaxInt64)
	got := backoff(1000, time.Second, huge)
	if got != huge {
		t.Fatalf("backoff(1000, 1s, MaxInt64)=%s, want max (%s)", got, huge)
	}
	if got <= 0 {
		t.Fatalf("backoff overflowed to a non-positive duration: %d", got)
	}
}
