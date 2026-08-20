package lifecycle

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestShutdownReceivesLiveContext is the core regression guard: the shutdown
// handlers must run against a context that is NOT already cancelled. The old
// code passed the just-cancelled root ctx, so http.Server.Shutdown returned
// instantly without draining.
func TestShutdownReceivesLiveContext(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())

	var sawLiveContext atomic.Bool
	al.RegisterShutdownHandler(func(ctx context.Context) error {
		if ctx.Err() == nil {
			sawLiveContext.Store(true)
		}
		return nil
	})

	_, cancel := context.WithCancel(context.Background())
	al.Shutdown(cancel, time.Second)

	if !sawLiveContext.Load() {
		t.Fatal("shutdown handler received an already-cancelled context")
	}
}

// TestShutdownCancelsRootContextBeforeDraining verifies the ordering: intake is
// stopped (root ctx cancelled) before handlers run, and Done() is closed.
func TestShutdownCancelsRootContextBeforeDraining(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())
	ctx, cancel := context.WithCancel(context.Background())

	al.RegisterShutdownHandler(func(c context.Context) error {
		if ctx.Err() == nil {
			t.Error("root context was not cancelled before shutdown handlers ran")
		}
		return nil
	})

	al.Shutdown(cancel, time.Second)

	select {
	case <-al.Done():
	case <-time.After(time.Second):
		t.Fatal("Done() was not closed after Shutdown")
	}
}

// TestShutdownWaitsForTrackedGoroutines verifies in-flight goroutines drain
// before infra teardown, and that the drain unblocks once they return.
func TestShutdownWaitsForTrackedGoroutines(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())
	ctx, cancel := context.WithCancel(context.Background())

	var finished atomic.Bool
	al.Go(func() {
		<-ctx.Done() // returns when intake stops
		time.Sleep(20 * time.Millisecond)
		finished.Store(true)
	})

	al.RegisterShutdownHandler(func(c context.Context) error {
		if !finished.Load() {
			t.Error("infra teardown ran before in-flight goroutine drained")
		}
		return nil
	})

	al.Shutdown(cancel, time.Second)
}

// TestDrainBoundedByGrace verifies a stuck goroutine does not hang shutdown
// past the grace period.
func TestDrainBoundedByGrace(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())
	_, cancel := context.WithCancel(context.Background())

	al.Go(func() {
		time.Sleep(2 * time.Second) // never returns within grace
	})

	start := time.Now()
	al.Shutdown(cancel, 50*time.Millisecond)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("shutdown blocked past grace period: %s", elapsed)
	}
}

// TestShutdownIdempotent verifies a second Shutdown is a no-op and does not
// panic on the already-closed Done channel.
func TestShutdownIdempotent(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())
	_, cancel := context.WithCancel(context.Background())
	al.Shutdown(cancel, time.Second)
	al.Shutdown(cancel, time.Second) // must not panic
}

// TestGoAfterShutdownIsRefused verifies that Go called once shutdown has
// begun does not start the goroutine and does not panic. Starting new
// tracked work after intake has stopped is wrong: it would either escape
// the drain that already ran, or race wg.Add against a wg.Wait already in
// flight, which is sync.WaitGroup misuse.
func TestGoAfterShutdownIsRefused(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())
	_, cancel := context.WithCancel(context.Background())
	al.Shutdown(cancel, time.Second)

	var ran atomic.Bool
	al.Go(func() {
		ran.Store(true)
	})

	time.Sleep(20 * time.Millisecond)
	if ran.Load() {
		t.Fatal("Go started a goroutine after shutdown had already begun")
	}
}

// TestConcurrentGoAndShutdownDoesNotMisuseWaitGroup drives Go and Shutdown
// concurrently. sync.WaitGroup requires that any Add with a positive delta
// happen-before the Wait it is meant to be counted by; violating that is
// documented misuse that can panic ("WaitGroup misuse: Add called
// concurrently with Wait") or let a goroutine escape the drain. This test
// only has teeth under the race detector: run with `-race -count=20`.
func TestConcurrentGoAndShutdownDoesNotMisuseWaitGroup(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())
	_, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			al.Go(func() {
				time.Sleep(time.Millisecond)
			})
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		al.Shutdown(cancel, 200*time.Millisecond)
	}()

	wg.Wait()

	select {
	case <-al.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done() was not closed after concurrent Go/Shutdown")
	}
}

// TestShutdownStaysWithinGraceBudgetWhenGoroutineHangs guards against
// spending the grace period twice (once draining, once tearing down infra).
// A tracked goroutine that ignores cancellation must not push total shutdown
// time anywhere near 2x the configured grace.
func TestShutdownStaysWithinGraceBudgetWhenGoroutineHangs(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())
	_, cancel := context.WithCancel(context.Background())

	release := make(chan struct{})
	defer close(release)
	al.Go(func() {
		<-release // ignores cancellation; only returns when the test releases it
	})

	// Simulates a real teardown handler (e.g. http.Server.Shutdown) that
	// respects its context and blocks until the context ends. This is what
	// makes the double-budget bug visible: a handler that returns instantly
	// wouldn't spend the second grace period even when one is offered.
	al.RegisterShutdownHandler(func(c context.Context) error {
		<-c.Done()
		return c.Err()
	})

	grace := 300 * time.Millisecond
	start := time.Now()
	al.Shutdown(cancel, grace)
	elapsed := time.Since(start)

	const tolerance = 150 * time.Millisecond
	if elapsed > grace+tolerance {
		t.Fatalf("shutdown exceeded the grace budget: elapsed=%s grace=%s", elapsed, grace)
	}
	if elapsed >= 2*grace {
		t.Fatalf("shutdown took ~2x grace (old double-budget bug): elapsed=%s grace=%s", elapsed, grace)
	}
}

// TestRegisterShutdownHandlerAfterShutdownIsRefused verifies that
// RegisterShutdownHandler called once shutdown has begun does not append to
// the handler slice and the handler never runs — symmetric with
// TestGoAfterShutdownIsRefused. Registering into a list Shutdown has already
// snapshotted (or will never read again) is worse than failing loudly: the
// caller would believe cleanup is wired up when it silently is not.
func TestRegisterShutdownHandlerAfterShutdownIsRefused(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())
	_, cancel := context.WithCancel(context.Background())
	al.Shutdown(cancel, time.Second)

	var ran atomic.Bool
	al.RegisterShutdownHandler(func(context.Context) error {
		ran.Store(true)
		return nil
	})

	if ran.Load() {
		t.Fatal("handler registered after shutdown ran")
	}
	al.mu.Lock()
	n := len(al.shutdownHandlers)
	al.mu.Unlock()
	if n != 0 {
		t.Fatalf("handler registered after shutdown was appended: len=%d", n)
	}
}

// TestConcurrentRegisterShutdownHandlerAndShutdownDoesNotRace drives
// RegisterShutdownHandler and Shutdown concurrently. Shutdown must read
// al.shutdownHandlers only under al.mu — a concurrent unsynchronized read
// racing RegisterShutdownHandler's synchronized append is a data race on the
// slice header (len/cap/ptr), which the race detector catches even though it
// rarely panics or misbehaves visibly. This test only has teeth under
// `-race -count=20`.
func TestConcurrentRegisterShutdownHandlerAndShutdownDoesNotRace(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())
	_, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			al.RegisterShutdownHandler(func(context.Context) error {
				return nil
			})
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		al.Shutdown(cancel, 200*time.Millisecond)
	}()

	wg.Wait()

	select {
	case <-al.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done() was not closed after concurrent RegisterShutdownHandler/Shutdown")
	}
}

// TestShutdownClampsNonPositiveGrace verifies that a zero or negative grace
// does not yield a deadline already in the past — the exact dead-context
// failure this package exists to prevent. Shutdown must substitute a sane
// default instead of handing shutdown handlers a context that is already
// done.
func TestShutdownClampsNonPositiveGrace(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())

	var sawLiveContext atomic.Bool
	al.RegisterShutdownHandler(func(ctx context.Context) error {
		if ctx.Err() == nil {
			sawLiveContext.Store(true)
		}
		return nil
	})

	_, cancel := context.WithCancel(context.Background())
	al.Shutdown(cancel, 0)

	if !sawLiveContext.Load() {
		t.Fatal("non-positive grace was not clamped: shutdown handler received an already-dead context")
	}
}

// TestTeardownGetsLiveContextWhenDrainTimesOut guards the starvation failure
// mode: when the drain step consumes its whole share of the budget, the
// shutdown handlers must still receive a live context with time left on it,
// not an already-expired one. A future "simplification" that derives the
// teardown context via time.Until(deadline) after an already-exhausted drain
// would silently reintroduce this.
func TestTeardownGetsLiveContextWhenDrainTimesOut(t *testing.T) {
	al := NewApplicationLifecycle(quietLogger())
	_, cancel := context.WithCancel(context.Background())

	release := make(chan struct{})
	defer close(release)
	al.Go(func() {
		<-release
	})

	var sawCancelled atomic.Bool
	var sawDeadline atomic.Bool
	var sawRemaining atomic.Int64
	al.RegisterShutdownHandler(func(c context.Context) error {
		sawCancelled.Store(c.Err() != nil)
		if dl, ok := c.Deadline(); ok {
			sawDeadline.Store(true)
			sawRemaining.Store(int64(time.Until(dl)))
		}
		return nil
	})

	al.Shutdown(cancel, 300*time.Millisecond)

	if sawCancelled.Load() {
		t.Fatal("shutdown handler received an already-cancelled context after the drain timed out")
	}
	if !sawDeadline.Load() {
		t.Fatal("shutdown handler context had no deadline")
	}
	if remaining := time.Duration(sawRemaining.Load()); remaining <= 0 {
		t.Fatalf("shutdown handler context deadline had already passed: remaining=%s", remaining)
	}
}
