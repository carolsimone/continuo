package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server serves GET /metrics on a listener of its own.
type Server struct {
	ln     net.Listener
	srv    *http.Server
	logger *slog.Logger
}

// Listen binds the metrics port at once, so a service whose METRICS_PORT is
// taken refuses to start instead of running unobservable. Port 0 picks a free
// port.
func Listen(port int, reg *Registry, logger *slog.Logger) (*Server, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen on metrics port %d: %w", port, err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", reg.Handler())
	return &Server{ln: ln, srv: &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}, logger: logger}, nil
}

// Serve blocks serving /metrics until Shutdown; it returns nil after Shutdown.
func (s *Server) Serve() error {
	s.logger.Info("Metrics listening", "port", s.Port())
	if err := s.srv.Serve(s.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown stops the listener, letting in-flight scrapes finish within ctx.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// Port is the bound port.
func (s *Server) Port() int { return s.ln.Addr().(*net.TCPAddr).Port }
