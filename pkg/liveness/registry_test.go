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

// TestRegistry_DependencyProbeFailsReadinessNotLiveness is the core of the
// readiness/liveness split: a failing DEPENDENCY probe (Redis/Postgres down)
// must pull the pod from Service endpoints (readiness) WITHOUT restarting it
// (liveness) — the consumers are already retrying, and a restart would only
// turn a recoverable outage into CrashLoopBackOff.
func TestRegistry_DependencyProbeFailsReadinessNotLiveness(t *testing.T) {
	r := NewRegistry()
	r.RegisterWorker("consumer-a")
	r.AddDependencyProbe("redis", time.Minute, func(context.Context) error {
		return errors.New("connection refused")
	})

	if r.Ready(context.Background()) {
		t.Fatalf("expected NOT ready while a dependency probe is failing")
	}
	if !r.Live(context.Background()) {
		t.Fatalf("expected still LIVE during a dependency outage — liveness must not include dependency probes")
	}

	// The dependency failure must appear in the readiness Check but not the
	// LivenessCheck.
	if got := len(r.Check(context.Background())); got != 1 {
		t.Fatalf("expected 1 readiness failure, got %d", got)
	}
	if got := len(r.LivenessCheck(context.Background())); got != 0 {
		t.Fatalf("expected 0 liveness failures during a dependency outage, got %d", got)
	}
}

// TestRegistry_WorkerHeartbeatFailsBoth: a stale consumer heartbeat (a wedged
// read loop) is a worker probe, so it must fail BOTH readiness and liveness —
// liveness is what actually restarts the pod.
func TestRegistry_WorkerHeartbeatFailsBoth(t *testing.T) {
	r := NewRegistry()
	r.AddWorkerProbe("consumer-a_heartbeat", time.Minute, func(context.Context) error {
		return errors.New("read loop stalled")
	})

	if r.Ready(context.Background()) {
		t.Fatalf("expected NOT ready when a worker heartbeat is stale")
	}
	if r.Live(context.Background()) {
		t.Fatalf("expected NOT live when a worker heartbeat is stale — a wedged consumer must restart the pod")
	}
}

// TestRegistry_WorkerExitFailsBoth: a worker that exited with an error (e.g. a
// permanent consumer-group bootstrap failure) must fail BOTH checks so it
// surfaces in readiness AND ultimately restarts the pod via liveness.
func TestRegistry_WorkerExitFailsBoth(t *testing.T) {
	r := NewRegistry()
	r.RegisterWorker("consumer-a")
	r.WorkerExited("consumer-a", errors.New("permanent bootstrap failure: WRONGTYPE"))

	if r.Ready(context.Background()) {
		t.Fatalf("expected NOT ready after a worker exited with error")
	}
	if r.Live(context.Background()) {
		t.Fatalf("expected NOT live after a worker exited with error")
	}
}

// TestRegistry_LiveWithHealthyMixIgnoresDeps: with a live worker, a fresh
// heartbeat, and a HEALTHY dependency probe, both checks pass — and liveness
// still evaluates only worker-side probes.
func TestRegistry_LiveWithHealthyMixIgnoresDeps(t *testing.T) {
	r := NewRegistry()
	r.RegisterWorker("consumer-a")
	r.AddWorkerProbe("consumer-a_heartbeat", time.Minute, func(context.Context) error { return nil })
	depCalls := 0
	r.AddDependencyProbe("redis", time.Minute, func(context.Context) error {
		depCalls++
		return nil
	})

	if !r.Ready(context.Background()) {
		t.Fatalf("expected ready with everything healthy")
	}
	if !r.Live(context.Background()) {
		t.Fatalf("expected live with worker + heartbeat healthy")
	}
	// LivenessCheck must not evaluate the dependency probe at all.
	depCalls = 0
	r.LivenessCheck(context.Background())
	if depCalls != 0 {
		t.Fatalf("LivenessCheck must not evaluate dependency probes, but it made %d dep calls", depCalls)
	}
}
