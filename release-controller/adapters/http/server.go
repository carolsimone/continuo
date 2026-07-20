package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/pkg/liveness"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

// Server is the HTTP server for the release-controller. It exposes a REST API
// for CI to submit release candidates and for operators to inspect release state.
type Server struct {
	deps     *handlers.Deps
	registry *liveness.Registry
	port     string
	srv      *http.Server
	log      *slog.Logger
}

// NewServer creates a new Server. Call Start to begin listening. Health is
// backed by registry across two paths with different semantics: /healthz
// (readiness) reflects workers + heartbeats + dependency probes, /livez
// (liveness) reflects workers + heartbeats ONLY. Deploy config points the
// Kubernetes readinessProbe at /healthz and the livenessProbe at /livez (see
// deploy/continuo/values.yaml), so a dependency outage pulls the pod from
// Service endpoints without restarting it, while a dead/wedged consumer both
// fails readiness AND restarts the pod.
func NewServer(deps *handlers.Deps, registry *liveness.Registry, port string, log *slog.Logger) *Server {
	return &Server{deps: deps, registry: registry, port: port, log: log}
}

// Routes registers all HTTP routes and returns the handler tree.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthHandler("readiness", s.registry.Check))
	mux.HandleFunc("GET /livez", s.healthHandler("liveness", s.registry.LivenessCheck))
	mux.HandleFunc("POST /releases", s.handleReceiveCandidate)
	mux.HandleFunc("GET /releases/{id}", s.handleGetRelease)
	mux.HandleFunc("GET /releases", s.handleListReleases)
	mux.HandleFunc("GET /current-prod", s.handleGetCurrentProd)
	return mux
}

// healthHandler answers 200 when check reports no failures and 503 (logging
// each failing component) otherwise. kind names the probe ("readiness" or
// "liveness") in logs and the response body.
func (s *Server) healthHandler(kind string, check func(context.Context) []liveness.Failure) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		failures := check(r.Context())
		if len(failures) == 0 {
			w.WriteHeader(http.StatusOK)
			return
		}
		for _, f := range failures {
			s.log.Warn("Health check failed", "kind", kind, "component", f.Name, "error", f.Err)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "NOT %s: %d component(s) unhealthy", kind, len(failures))
	}
}

// Start registers routes, begins accepting connections, and blocks until ctx is
// cancelled, at which point it performs a graceful 5-second shutdown.
func (s *Server) Start(ctx context.Context) error {
	s.srv = &http.Server{Addr: ":" + s.port, Handler: s.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() { //nolint:gosec // G118: ctx is already Done() by the time we get here, so it cannot supply a working deadline for Shutdown; a fresh context.Background() is required, not a request-scoped one
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(shutdownCtx)
	}()
	s.log.Info("release-controller HTTP listening", "port", s.port)
	if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}
