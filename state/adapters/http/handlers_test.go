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
	reg.RegisterWorker("schedule_catalog")
	handler := NewReadinessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// TestReadinessReturns503WhenConsumerExited is the readiness-flip regression: a
// consumer returning an error must drive /ready to 503.
func TestReadinessReturns503WhenConsumerExited(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("schedule_catalog")
	reg.WorkerExited("schedule_catalog", errors.New("redis dropped"))
	handler := NewReadinessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after consumer exit, got %d", rec.Code)
	}
}

func TestReadinessReturns503WhenProbeFails(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.AddProbe("postgres", 0, func(ctx context.Context) error { return errors.New("down") })
	handler := NewReadinessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when probe fails, got %d", rec.Code)
	}
}

func TestHealthAlwaysReturns200(t *testing.T) {
	rec := httptest.NewRecorder()
	HealthHandler(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from health, got %d", rec.Code)
	}
}

// TestLivenessHandler_WorkerExitFlipsLivez is the liveness-flip regression: a
// consumer returning an error must drive /livez to 503, the same as /ready.
func TestLivenessHandler_WorkerExitFlipsLivez(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("consumer_a")
	reg.WorkerExited("consumer_a", errors.New("boom"))
	handler := NewLivenessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("livez after worker exit = %d, want 503", rec.Code)
	}
}

// TestLivenessHandler_DependencyFailureDoesNotFlipLivez asserts the split
// between the two probes: a dependency outage must stop traffic (readiness)
// without restarting a pod whose consumers are already retrying through it.
func TestLivenessHandler_DependencyFailureDoesNotFlipLivez(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.AddDependencyProbe("redis", 0, func(context.Context) error { return errors.New("down") })
	handler := NewLivenessHandler(reg)

	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/livez", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("livez during dependency outage = %d, want 200 (deps must not restart the pod)", rec.Code)
	}
}
