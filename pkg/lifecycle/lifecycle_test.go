package lifecycle

import (
	"context"
	"io"
	"log/slog"
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
