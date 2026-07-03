package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// HealthServer provides HTTP health check endpoint
type HealthServer struct {
	server *http.Server
	logger *slog.Logger
}

// NewHealthServer creates a new health check HTTP server
func NewHealthServer(port int, logger *slog.Logger) *HealthServer {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			logger.Warn("failed to write health response", "error", err)
		}
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("READY")); err != nil {
			logger.Warn("failed to write ready response", "error", err)
		}
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return &HealthServer{
		server: server,
		logger: logger,
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
