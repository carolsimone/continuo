package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/carolsimone/continuo/pkg/liveness"
)

// HealthServer provides HTTP liveness and readiness endpoints.
//
//   - /health is a liveness probe: it returns 200 as long as the process is
//     running and able to serve HTTP.
//   - /ready is a readiness probe backed by the liveness registry: it returns
//     503 when any registered consumer has exited with an error or any cached
//     dependency probe fails, so Kubernetes stops routing traffic to a pod
//     whose background workers or backing stores are unhealthy.
type HealthServer struct {
	server   *http.Server
	logger   *slog.Logger
	registry *liveness.Registry
}

// NewHealthServer creates a new health check HTTP server reading readiness from
// the supplied liveness registry.
func NewHealthServer(port int, registry *liveness.Registry, logger *slog.Logger) *HealthServer {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		failures := registry.Check(r.Context())
		if len(failures) == 0 {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("READY"))
			return
		}
		for _, f := range failures {
			logger.Warn("Readiness check failed", "component", f.Name, "error", f.Err)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "NOT READY: %d component(s) unhealthy", len(failures))
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return &HealthServer{
		server:   server,
		logger:   logger,
		registry: registry,
	}
}

// Start starts the HTTP server
func (h *HealthServer) Start() error {
	h.logger.Info("Starting health check server", "addr", h.server.Addr)
	if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start health server: %w", err)
	}
	return nil
}

// Shutdown gracefully shuts down the HTTP server
func (h *HealthServer) Shutdown(ctx context.Context) error {
	h.logger.Info("Shutting down health check server")
	return h.server.Shutdown(ctx)
}
