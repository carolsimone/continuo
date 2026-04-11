package http

import (
	"context"
	"log/slog"
	"net/http"
)

// Server wraps HTTP server for health checks
type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
}

// NewServer creates a new HTTP server
func NewServer(port string, logger *slog.Logger) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", HealthHandler)

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
