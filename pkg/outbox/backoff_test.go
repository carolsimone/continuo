package outbox

import (
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
