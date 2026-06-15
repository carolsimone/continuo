package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/carolsimone/continuo/pkg/liveness"
)

// Server wraps the HTTP server exposing liveness (/health) and readiness
// (/ready) probes.
type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
}

// NewServer creates a new HTTP health server. Readiness is answered from the
// supplied liveness registry; /health returns 200 as long as the process can
// serve HTTP.
func NewServer(port int, registry *liveness.Registry, logger *slog.Logger) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/ready", newReadinessHandler(registry))

	return &Server{
		httpServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		},
		logger: logger,
	}
}

// Start starts the HTTP server. It returns nil on clean close (ErrServerClosed).
func (s *Server) Start() error {
	s.logger.Info("Starting HTTP health server", "addr", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP health server")
	return s.httpServer.Shutdown(ctx)
}

// healthResponse is the JSON body for /health.
type healthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

// readinessResponse is the JSON body for /ready.
type readinessResponse struct {
	Status    string   `json:"status"`
	Service   string   `json:"service"`
	Unhealthy []string `json:"unhealthy,omitempty"`
}

// healthHandler handles liveness check requests. It returns 200 as long as the
// process is running and able to serve HTTP.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(healthResponse{Status: "healthy", Service: "agent-runner"}) //nolint:errcheck
}

// newReadinessHandler returns a readiness handler backed by the liveness
// registry. It responds 503 when any registered worker has exited with an
// error or any cached dependency probe fails, so Kubernetes stops routing
// traffic to an unhealthy pod.
func newReadinessHandler(registry *liveness.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		failures := registry.Check(r.Context())
		if len(failures) == 0 {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(readinessResponse{Status: "ready", Service: "agent-runner"}) //nolint:errcheck
			return
		}
		names := make([]string, 0, len(failures))
		for _, f := range failures {
			names = append(names, f.Name)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(readinessResponse{Status: "not_ready", Service: "agent-runner", Unhealthy: names}) //nolint:errcheck
	}
}
