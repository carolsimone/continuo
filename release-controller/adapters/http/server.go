package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

// Server is the HTTP server for the release-controller. It exposes a REST API
// for CI to submit release candidates and for operators to inspect release state.
type Server struct {
	deps *handlers.Deps
	port string
	srv  *http.Server
	log  *slog.Logger
}

// NewServer creates a new Server. Call Start to begin listening.
func NewServer(deps *handlers.Deps, port string, log *slog.Logger) *Server {
	return &Server{deps: deps, port: port, log: log}
}

// Routes registers all HTTP routes and returns the handler tree.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /releases", s.handleReceiveCandidate)
	mux.HandleFunc("GET /releases/{id}", s.handleGetRelease)
	mux.HandleFunc("GET /releases", s.handleListReleases)
	mux.HandleFunc("GET /current-prod", s.handleGetCurrentProd)
	return mux
}

// Start registers routes, begins accepting connections, and blocks until ctx is
// cancelled, at which point it performs a graceful 5-second shutdown.
func (s *Server) Start(ctx context.Context) error {
	s.srv = &http.Server{Addr: ":" + s.port, Handler: s.Routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
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
