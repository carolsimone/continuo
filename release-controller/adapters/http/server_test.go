package http

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/liveness"
)

func testServer(reg *liveness.Registry) *Server {
	return NewServer(reg, "0", slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func probe(t *testing.T, srv *Server, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

// TestHealthRoutesAnswerBeforeTheAPIIsServed: the server answers its probes while
// the process waits for its dependencies, and the API appears once ServeAPI has
// run. /livez must answer here, or the liveness probe restarts a pod that is only
// waiting for Postgres.
func TestHealthRoutesAnswerBeforeTheAPIIsServed(t *testing.T) {
	reg := liveness.NewRegistry()
	reg.AddStartupGate()
	srv := testServer(reg)

	if code := probe(t, srv, "/livez"); code != http.StatusOK {
		t.Fatalf("/livez during the wait: expected 200, got %d", code)
	}
	if code := probe(t, srv, "/healthz"); code != http.StatusServiceUnavailable {
		t.Fatalf("/healthz during the wait: expected 503, got %d", code)
	}
	submit := func() int {
		rec := httptest.NewRecorder()
		srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/releases", strings.NewReader("not json")))
		return rec.Code
	}
	if code := submit(); code != http.StatusNotFound {
		t.Fatalf("POST /releases before ServeAPI: expected 404, got %d", code)
	}

	deps, _ := newRetryRemediationDeps(time.Unix(100, 0).UTC())
	srv.ServeAPI(deps)
	if code := submit(); code != http.StatusBadRequest {
		t.Fatalf("POST /releases of a bad body after ServeAPI: expected 400, got %d", code)
	}
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
