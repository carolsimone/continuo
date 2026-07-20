package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/carolsimone/continuo/pkg/liveness"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// readiness and liveness build the two health handlers exactly as main wires
// them, so these tests exercise the real registry-check plumbing.
func readiness(reg *liveness.Registry) http.HandlerFunc {
	return healthHandler("readiness", reg.Check, discardLogger())
}
func liveHandler(reg *liveness.Registry) http.HandlerFunc {
	return healthHandler("liveness", reg.LivenessCheck, discardLogger())
}

func code(h http.HandlerFunc) int {
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	return rec.Code
}

// TestHealthyRegistryPassesBothProbes: a live worker + fresh heartbeat → both
// readiness and liveness answer 200.
func TestHealthyRegistryPassesBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	reg.AddWorkerProbe("remediation_requested_heartbeat", 0, func(context.Context) error { return nil })

	if c := code(readiness(reg)); c != http.StatusOK {
		t.Fatalf("readiness: expected 200, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusOK {
		t.Fatalf("liveness: expected 200, got %d", c)
	}
}

// TestDependencyOutageFailsReadinessNotLiveness is the [P1a] regression: a
// failing dependency probe fails readiness but NOT liveness.
func TestDependencyOutageFailsReadinessNotLiveness(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	reg.AddDependencyProbe("postgres", 0, func(context.Context) error { return errors.New("down") })

	if c := code(readiness(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("readiness during dependency outage: expected 503, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusOK {
		t.Fatalf("liveness during dependency outage: expected 200 (must NOT restart), got %d", c)
	}
}

// TestConsumerExitFailsBothProbes: a consumer that exited with an error fails
// both readiness and liveness.
func TestConsumerExitFailsBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	reg.WorkerExited("remediation_requested", errors.New("permanent bootstrap failure: WRONGTYPE"))

	if c := code(readiness(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("readiness after consumer exit: expected 503, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("liveness after consumer exit: expected 503, got %d", c)
	}
}

// TestStaleHeartbeatFailsBothProbes: a wedged consumer (stale heartbeat) fails
// both probes.
func TestStaleHeartbeatFailsBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	reg.AddWorkerProbe("remediation_requested_heartbeat", 0, func(context.Context) error {
		return errors.New("read loop stalled — no activity for 5m0s")
	})

	if c := code(readiness(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("readiness with stale heartbeat: expected 503, got %d", c)
	}
	if c := code(liveHandler(reg)); c != http.StatusServiceUnavailable {
		t.Fatalf("liveness with stale heartbeat: expected 503, got %d", c)
	}
}
