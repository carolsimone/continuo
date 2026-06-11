package ports_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/service/ports"
)

// TestSystemClock_NowTruncatedToMicrosecond guards the cancel/finalize
// "response timestamp == persisted timestamp" guarantee: PostgreSQL timestamptz
// stores microsecond precision, so a clock that minted nanoseconds would make
// the in-memory value differ from the stored row. SystemClock.Now must carry no
// sub-microsecond remainder.
func TestSystemClock_NowTruncatedToMicrosecond(t *testing.T) {
	for i := 0; i < 1000; i++ {
		now := ports.SystemClock{}.Now()
		if rem := now.Nanosecond() % int(time.Microsecond/time.Nanosecond); rem != 0 {
			t.Fatalf("SystemClock.Now() has sub-microsecond precision: %d ns remainder at %s", rem, now)
		}
		if now.Location() != time.UTC {
			t.Fatalf("SystemClock.Now() must be UTC, got %s", now.Location())
		}
	}
}
