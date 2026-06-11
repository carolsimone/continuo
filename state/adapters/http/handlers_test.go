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

func TestLivenessAlwaysHealthy(t *testing.T) {
	rec := httptest.NewRecorder()
	HealthHandler(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from liveness, got %d", rec.Code)
	}
}
