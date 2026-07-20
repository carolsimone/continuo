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

// probe issues a GET against the given path on the health server and returns
// the status code.
func probe(t *testing.T, srv *HealthServer, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

// TestReadyAndLivezHealthyWhenRegistryHealthy: with a live worker and a fresh
// heartbeat, both /ready and /livez answer 200.
func TestReadyAndLivezHealthyWhenRegistryHealthy(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("query_model")
	reg.AddWorkerProbe("query_model_heartbeat", 0, func(context.Context) error { return nil })
	srv := NewHealthServer(0, reg, discardLogger())

	if code := probe(t, srv, "/ready"); code != http.StatusOK {
		t.Fatalf("/ready: expected 200, got %d", code)
	}
	if code := probe(t, srv, "/livez"); code != http.StatusOK {
		t.Fatalf("/livez: expected 200, got %d", code)
	}
}

// TestDependencyOutageFailsReadinessNotLiveness is the [P1a] regression: a
// failing DEPENDENCY probe (Redis/Postgres down) must fail /ready (pull the pod
// from Service endpoints) but NOT /livez (restarting a pod whose consumers are
// already retrying would just turn a recoverable outage into CrashLoopBackOff).
func TestDependencyOutageFailsReadinessNotLiveness(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("query_model")
	reg.AddDependencyProbe("redis", 0, func(context.Context) error { return errors.New("connection refused") })
	srv := NewHealthServer(0, reg, discardLogger())

	if code := probe(t, srv, "/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/ready during dependency outage: expected 503, got %d", code)
	}
	if code := probe(t, srv, "/livez"); code != http.StatusOK {
		t.Fatalf("/livez during dependency outage: expected 200 (must NOT restart), got %d", code)
	}
}

// TestConsumerExitFailsBothProbes: a consumer goroutine that exited with an
// error (e.g. a permanent bootstrap failure) must fail BOTH /ready and /livez —
// liveness is what restarts the pod.
func TestConsumerExitFailsBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("query_model")
	reg.WorkerExited("query_model", errors.New("permanent bootstrap failure: WRONGTYPE"))
	srv := NewHealthServer(0, reg, discardLogger())

	if code := probe(t, srv, "/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/ready after consumer exit: expected 503, got %d", code)
	}
	if code := probe(t, srv, "/livez"); code != http.StatusServiceUnavailable {
		t.Fatalf("/livez after consumer exit: expected 503, got %d", code)
	}
}

// TestStaleHeartbeatFailsBothProbes: a wedged (not exited) consumer surfaces as
// a stale worker heartbeat and must fail BOTH probes.
func TestStaleHeartbeatFailsBothProbes(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("query_model")
	reg.AddWorkerProbe("query_model_heartbeat", 0, func(context.Context) error {
		return errors.New("read loop stalled — no activity for 5m0s")
	})
	srv := NewHealthServer(0, reg, discardLogger())

	if code := probe(t, srv, "/ready"); code != http.StatusServiceUnavailable {
		t.Fatalf("/ready with stale heartbeat: expected 503, got %d", code)
	}
	if code := probe(t, srv, "/livez"); code != http.StatusServiceUnavailable {
		t.Fatalf("/livez with stale heartbeat: expected 503, got %d", code)
	}
}

// TestHealthAlwaysHealthy: /health stays a pure "process is up" probe.
func TestHealthAlwaysHealthy(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.RegisterWorker("query_model")
	reg.WorkerExited("query_model", errors.New("dead"))
	srv := NewHealthServer(0, reg, discardLogger())

	if code := probe(t, srv, "/health"); code != http.StatusOK {
		t.Fatalf("expected 200 from /health regardless of registry state, got %d", code)
	}
}
