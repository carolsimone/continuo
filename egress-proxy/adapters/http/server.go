package http

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/pkg/liveness"
)

// Server wraps the HTTP server exposing a process-up probe (/health),
// readiness (/ready), and liveness (/livez) endpoints.
type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
	listener   net.Listener
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

// Listen binds the health port, so a taken port stops the service at startup
// instead of leaving a process whose probes cannot connect.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("listen on health port %s: %w", s.httpServer.Addr, err)
	}
	s.listener = ln
	return nil
}

// Start serves on the port Listen bound until Shutdown, then returns nil.
func (s *Server) Start() error {
	s.logger.Info("Starting HTTP health server", "addr", s.listener.Addr().String())
	if err := s.httpServer.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP health server")
	return s.httpServer.Shutdown(ctx)
}
