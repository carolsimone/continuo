package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/carolsimone/continuo/pkg/liveness"
	"github.com/carolsimone/continuo/run-controller/config"
)

func TestReadinessReturns200WhenWorkersLive(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("task_status_updated")
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
	reg.RegisterWorker("task_status_updated")
	reg.WorkerExited("task_status_updated", errors.New("redis dropped"))
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

// Every body the health, readiness and liveness handlers answer, ready or not,
// names the service as config.ServiceName, the name the service's dead letters
// and envelopes carry.
func TestProbeBodiesNameTheService(t *testing.T) {
	live := liveness.NewRegistry()
	live.RegisterWorker("consumer_a")

	failing := liveness.NewRegistry()
	failing.RegisterWorker("consumer_a")
	failing.WorkerExited("consumer_a", errors.New("boom"))

	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantStatus string
	}{
		{"health", HealthHandler, "healthy"},
		{"ready", NewReadinessHandler(live), "ready"},
		{"not ready", NewReadinessHandler(failing), "not_ready"},
		{"live", NewLivenessHandler(live), "live"},
		{"not live", NewLivenessHandler(failing), "not_live"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.handler(rec, httptest.NewRequest(http.MethodGet, "/probe", nil))

			var body ReadinessResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode %q: %v", rec.Body.String(), err)
			}
			if body.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", body.Status, tc.wantStatus)
			}
			if body.Service != config.ServiceName {
				t.Errorf("service = %q, want config.ServiceName %q", body.Service, config.ServiceName)
			}
		})
	}
}
