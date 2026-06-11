package http

import (
	"context"
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

// NewServer creates a new HTTP server. Readiness is answered from the supplied
// liveness registry.
func NewServer(port string, registry *liveness.Registry, logger *slog.Logger) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", HealthHandler)
	mux.HandleFunc("/ready", NewReadinessHandler(registry))

	return &Server{
		httpServer: &http.Server{
			Addr:    ":" + port,
			Handler: mux,
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
