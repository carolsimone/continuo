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

func newTestServer(reg *liveness.Registry) *HealthServer {
	return NewHealthServer(0, reg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestReadyReturns200WhenAllWorkersLive(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("node_updated")
	h := newTestServer(reg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	h.server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

// TestReadyReturns503WhenConsumerExited is the readiness-flip regression: a
// consumer that returns an error must make /ready report 503 so Kubernetes
// stops routing traffic to the degraded pod.
func TestReadyReturns503WhenConsumerExited(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("node_updated")
	reg.WorkerExited("node_updated", errors.New("redis dropped"))
	h := newTestServer(reg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	h.server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after consumer exit, got %d", rec.Code)
	}
}

func TestReadyReturns503WhenDependencyProbeFails(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.AddProbe("postgres", 0, func(ctx context.Context) error { return errors.New("down") })
	h := newTestServer(reg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	h.server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when dependency probe fails, got %d", rec.Code)
	}
}

func TestHealthAlwaysReturns200(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("node_updated")
	reg.WorkerExited("node_updated", errors.New("redis dropped"))
	h := newTestServer(reg)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	h.server.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("liveness must stay 200 even when readiness is degraded, got %d", rec.Code)
	}
}
