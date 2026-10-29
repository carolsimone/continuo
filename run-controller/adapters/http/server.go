package http

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/pkg/liveness"
)

// Server wraps the HTTP server exposing a process-up probe (/health),
// readiness (/ready), and liveness (/livez) endpoints.
type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
}

// NewServer creates a new HTTP server. Readiness and liveness are answered
// from the supplied liveness registry: /ready also fails on a dependency
// outage (stops traffic), /livez fails only on a dead or wedged worker
// (restarts the pod).
func NewServer(port string, registry *liveness.Registry, logger *slog.Logger) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", HealthHandler)
	mux.HandleFunc("/ready", NewReadinessHandler(registry))
	mux.HandleFunc("/livez", NewLivenessHandler(registry))

	return &Server{
		httpServer: &http.Server{
			Addr:              ":" + port,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		logger: logger,
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	s.logger.Info("Starting HTTP health server", "addr", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP health server")
	return s.httpServer.Shutdown(ctx)
}
