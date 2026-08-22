package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/carolsimone/continuo/pkg/liveness"
)

func TestReadinessReturns200WhenWorkersLive(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("session_store")
	handler := newReadinessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// TestReadinessReturns503WhenWorkerExited is the readiness-flip regression: a
// worker returning an error must drive /ready to 503.
func TestReadinessReturns503WhenWorkerExited(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("session_store")
	reg.WorkerExited("session_store", errors.New("redis dropped"))
	handler := newReadinessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after worker exit, got %d", rec.Code)
	}
}

func TestReadinessReturns503WhenDependencyProbeFails(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.AddDependencyProbe("postgres", 0, func(context.Context) error { return errors.New("down") })
	handler := newReadinessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when dependency probe fails, got %d", rec.Code)
	}
}

func TestHealthAlwaysReturns200(t *testing.T) {
	rec := httptest.NewRecorder()
	healthHandler(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from health, got %d", rec.Code)
	}
}

// TestLivenessReturns200OnFreshRegistry covers agent-chat's actual shape: it
// runs no stream consumers, so a registry with nothing registered must still
// report live.
func TestLivenessReturns200OnFreshRegistry(t *testing.T) {
	reg := liveness.NewRegistry()
	handler := newLivenessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on a fresh registry, got %d", rec.Code)
	}
}

// TestLivenessReturns503WhenWorkerExited is the liveness-flip regression: a
// worker returning an error must drive /livez to 503, the same as /ready.
func TestLivenessReturns503WhenWorkerExited(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("session_store")
	reg.WorkerExited("session_store", errors.New("boom"))
	handler := newLivenessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("livez after worker exit = %d, want 503", rec.Code)
	}
}

// TestLivenessDoesNotFlipOnDependencyFailure asserts the split between the two
// probes: a dependency outage must stop traffic (readiness) without
// restarting a pod whose workers are already retrying through it.
func TestLivenessDoesNotFlipOnDependencyFailure(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.AddDependencyProbe("postgres", 0, func(context.Context) error { return errors.New("down") })
	handler := newLivenessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("livez during dependency outage = %d, want 200 (deps must not restart the pod)", rec.Code)
	}
}
