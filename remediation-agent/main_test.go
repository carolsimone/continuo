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

// TestHealthzReturns200WhenRegistryHealthy is the base case: with no
// registered failures, /healthz answers 200. Before this change, /healthz was
// hardcoded to 200 regardless of consumer state (dead code as far as the
// registry goes), so this test alone would have passed either way — the
// failing-registry tests below are the ones that pin the fix.
func TestHealthzReturns200WhenRegistryHealthy(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	handler := healthzHandler(reg, discardLogger())

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// TestHealthzReturns503WhenConsumerExited is the readiness-flip regression: a
// consumer goroutine returning an error must drive /healthz to 503 — deploy
// config points BOTH the readiness and liveness Kubernetes probes at
// /healthz for this service, so this is what actually restarts a pod stuck
// with a dead consumer instead of leaving it at 1/1 Running forever.
func TestHealthzReturns503WhenConsumerExited(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	reg.WorkerExited("remediation_requested", errors.New("redis dropped"))
	handler := healthzHandler(reg, discardLogger())

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after consumer exit, got %d", rec.Code)
	}
}

// TestHealthzReturns503WhenHeartbeatStale proves the specific gap this
// incident exposed: a consumer whose goroutine never returned (wedged, not
// exited) is invisible to WorkerExited alone. The heartbeat probe catches it.
func TestHealthzReturns503WhenHeartbeatStale(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("remediation_requested")
	reg.AddProbe("remediation_requested_heartbeat", 0, func(context.Context) error {
		return errors.New("read loop stalled — no activity for 5m0s")
	})
	handler := healthzHandler(reg, discardLogger())

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when the read-loop heartbeat is stale, got %d", rec.Code)
	}
}

// TestHealthzReturns503WhenDependencyProbeFails: a failed Redis/Postgres ping
// must also flip readiness.
func TestHealthzReturns503WhenDependencyProbeFails(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.AddProbe("postgres", 0, func(context.Context) error { return errors.New("down") })
	handler := healthzHandler(reg, discardLogger())

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when a dependency probe fails, got %d", rec.Code)
	}
}
