package http

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

// TestReadyReturns200WhenRegistryHealthy is the base case: with only healthy
// workers/probes registered, /ready must answer 200. Before this change,
// k8s-controller's /ready was hardcoded to 200 regardless of the registry
// (dead code, unwired) — this test would have passed either way, so the
// failing-registry tests below are the ones that actually pin the fix.
func TestReadyReturns200WhenRegistryHealthy(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("check_k8s")
	srv := NewHealthServer(0, reg, discardLogger())

	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// TestReadyReturns503WhenConsumerExited is the readiness-flip regression: a
// consumer goroutine returning an error must drive /ready to 503, so the
// Kubernetes probe (which deploy config now points at /ready for BOTH
// readiness and liveness — see deploy/continuo/values.yaml) restarts the pod
// instead of leaving a dead consumer running forever inside a 1/1 pod.
func TestReadyReturns503WhenConsumerExited(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("check_k8s")
	reg.WorkerExited("check_k8s", errors.New("redis dropped"))
	srv := NewHealthServer(0, reg, discardLogger())

	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after consumer exit, got %d", rec.Code)
	}
}

// TestReadyReturns503WhenHeartbeatStale proves the specific gap this incident
// exposed: a consumer whose goroutine never returned (wedged, not exited) is
// invisible to WorkerExited alone. The heartbeat probe must catch it too.
func TestReadyReturns503WhenHeartbeatStale(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("check_k8s")
	reg.AddProbe("check_k8s_heartbeat", 0, func(context.Context) error {
		return errors.New("read loop stalled — no activity for 5m0s")
	})
	srv := NewHealthServer(0, reg, discardLogger())

	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when the read-loop heartbeat is stale, got %d", rec.Code)
	}
}

// TestReadyReturns503WhenDependencyProbeFails: a failed Redis/Postgres ping
// must also flip readiness.
func TestReadyReturns503WhenDependencyProbeFails(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.AddProbe("postgres", 0, func(context.Context) error { return errors.New("down") })
	srv := NewHealthServer(0, reg, discardLogger())

	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when a dependency probe fails, got %d", rec.Code)
	}
}

// TestHealthAlwaysHealthy: /health stays a pure "process is up" liveness
// check, independent of the registry.
func TestHealthAlwaysHealthy(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("check_k8s")
	reg.WorkerExited("check_k8s", errors.New("dead"))
	srv := NewHealthServer(0, reg, discardLogger())

	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /health regardless of registry state, got %d", rec.Code)
	}
}
