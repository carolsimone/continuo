package liveness

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRegistry_ReadyUntilWorkerExitsWithError(t *testing.T) {
	r := NewRegistry()
	r.RegisterWorker("consumer-a")
	r.RegisterWorker("consumer-b")

	if !r.Ready(context.Background()) {
		t.Fatalf("expected ready with all workers live")
	}

	// A clean stop (nil error) must keep the process ready: this is the
	// graceful-shutdown path where the consumer returns on ctx cancel.
	r.WorkerExited("consumer-a", nil)
	if !r.Ready(context.Background()) {
		t.Fatalf("expected ready after clean worker stop")
	}

	// A genuine exit (non-nil error) flips readiness.
	r.WorkerExited("consumer-b", errors.New("redis dropped"))
	if r.Ready(context.Background()) {
		t.Fatalf("expected not ready after worker exited with error")
	}

	failures := r.Check(context.Background())
	if len(failures) != 1 || failures[0].Name != "consumer-b" {
		t.Fatalf("expected single failure for consumer-b, got %+v", failures)
	}
}

func TestRegistry_ProbeFlipsReadiness(t *testing.T) {
	r := NewRegistry()
	probeErr := errors.New("ping failed")
	r.AddProbe("postgres", time.Minute, func(ctx context.Context) error { return probeErr })

	if r.Ready(context.Background()) {
		t.Fatalf("expected not ready when probe fails")
	}
}

func TestRegistry_ProbeResultCachedWithinTTL(t *testing.T) {
	r := NewRegistry()
	calls := 0
	r.AddProbe("postgres", time.Hour, func(ctx context.Context) error {
		calls++
		return nil
	})

	for i := 0; i < 5; i++ {
		r.Ready(context.Background())
	}
	if calls != 1 {
		t.Fatalf("expected probe evaluated once within TTL, got %d calls", calls)
	}
}

func TestRegistry_ProbeReevaluatedAfterTTL(t *testing.T) {
	r := NewRegistry()
	calls := 0
	r.AddProbe("postgres", time.Nanosecond, func(ctx context.Context) error {
		calls++
		return nil
	})

	r.Ready(context.Background())
	time.Sleep(time.Millisecond)
	r.Ready(context.Background())
	if calls != 2 {
		t.Fatalf("expected probe re-evaluated after TTL, got %d calls", calls)
	}
}
