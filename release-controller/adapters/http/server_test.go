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

func testServer(reg *liveness.Registry) *Server {
	return &Server{registry: reg, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func probe(t *testing.T, srv *Server, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

// TestHealthzAndLivezHealthy: with a live worker and fresh heartbeat, both
// /healthz (readiness) and /livez (liveness) answer 200.
func TestHealthzAndLivezHealthy(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("manifest_loaded_candidate")
	reg.AddWorkerProbe("manifest_loaded_candidate_heartbeat", 0, func(context.Context) error { return nil })
	srv := testServer(reg)

	if code := probe(t, srv, "/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz: expected 200, got %d", code)
	}
	if code := probe(t, srv, "/livez"); code != http.StatusOK {
		t.Fatalf("/livez: expected 200, got %d", code)
	}
}

// TestDependencyOutageFailsReadinessNotLiveness is the [P1a] regression: a
// failing DEPENDENCY probe fails /healthz (readiness) but NOT /livez (liveness).
func TestDependencyOutageFailsReadinessNotLiveness(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("manifest_loaded_candidate")
	reg.AddDependencyProbe("postgres", 0, func(context.Context) error { return errors.New("down") })
	srv := testServer(reg)

	if code := probe(t, srv, "/healthz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/healthz during dependency outage: expected 503, got %d", code)
	}
	if code := probe(t, srv, "/livez"); code != http.StatusOK {
		t.Fatalf("/livez during dependency outage: expected 200 (must NOT restart), got %d", code)
	}
}

// TestConsumerExitFailsBothProbes: a consumer that exited with an error fails
// BOTH /healthz and /livez — liveness restarts the pod.
func TestConsumerExitFailsBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("manifest_loaded_candidate")
	reg.WorkerExited("manifest_loaded_candidate", errors.New("permanent bootstrap failure: WRONGTYPE"))
	srv := testServer(reg)

	if code := probe(t, srv, "/healthz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/healthz after consumer exit: expected 503, got %d", code)
	}
	if code := probe(t, srv, "/livez"); code != http.StatusServiceUnavailable {
		t.Fatalf("/livez after consumer exit: expected 503, got %d", code)
	}
}

// TestStaleHeartbeatFailsBothProbes: a wedged consumer surfaces as a stale
// heartbeat and must fail BOTH probes.
func TestStaleHeartbeatFailsBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("manifest_loaded_candidate")
	reg.AddWorkerProbe("manifest_loaded_candidate_heartbeat", 0, func(context.Context) error {
		return errors.New("read loop stalled — no activity for 5m0s")
	})
	srv := testServer(reg)

	if code := probe(t, srv, "/healthz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/healthz with stale heartbeat: expected 503, got %d", code)
	}
	if code := probe(t, srv, "/livez"); code != http.StatusServiceUnavailable {
		t.Fatalf("/livez with stale heartbeat: expected 503, got %d", code)
	}
}
