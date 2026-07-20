package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/pkg/liveness"
)

// HealthServer provides HTTP liveness and readiness endpoints with deliberately
// different semantics, both backed by the liveness registry:
//
//   - /ready (readiness) returns 503 when any registered worker has exited with
//     an error, any consumer's read-loop heartbeat has gone stale, OR any
//     external dependency probe (Redis/Postgres) fails. A dependency outage
//     pulls the pod out of Service endpoints so no traffic is routed to it.
//   - /livez (liveness) returns 503 ONLY for worker/heartbeat failures — never
//     for a dependency outage. Restarting a pod whose backing store is briefly
//     down just turns a recoverable outage into CrashLoopBackOff; the consumers
//     are already retrying through it. A dead/wedged consumer goroutine, though,
//     SHOULD restart the pod, and does.
//   - /health is a plain process-up probe (always 200); retained for manual use.
//
// Deploy config points the Kubernetes readinessProbe at /ready and the
// livenessProbe at /livez (see deploy/continuo/values.yaml), so the two probes
// hit DIFFERENT paths with the semantics above.
type HealthServer struct {
	server   *http.Server
	logger   *slog.Logger
	registry *liveness.Registry
}

// NewHealthServer creates a new health check HTTP server reading health from
// the supplied liveness registry.
func NewHealthServer(port int, registry *liveness.Registry, logger *slog.Logger) *HealthServer {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("OK")); err != nil {
			logger.Warn("failed to write health response", "error", err)
		}
	})

	mux.HandleFunc("/ready", writeHealth(logger, "readiness", registry.Check))
	mux.HandleFunc("/livez", writeHealth(logger, "liveness", registry.LivenessCheck))

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return &HealthServer{
		server:   server,
		logger:   logger,
		registry: registry,
	}
}

// writeHealth returns a handler that answers 200 when check reports no failures
// and 503 (logging each failing component) otherwise. kind names the probe in
// logs and the response body ("readiness" or "liveness").
func writeHealth(logger *slog.Logger, kind string, check func(context.Context) []liveness.Failure) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		failures := check(r.Context())
		if len(failures) == 0 {
			w.WriteHeader(http.StatusOK)
			if _, err := fmt.Fprintf(w, "%s OK", kind); err != nil {
				logger.Warn("failed to write health response", "kind", kind, "error", err)
			}
			return
		}
		for _, f := range failures {
			logger.Warn("Health check failed", "kind", kind, "component", f.Name, "error", f.Err)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := fmt.Fprintf(w, "NOT %s: %d component(s) unhealthy", kind, len(failures)); err != nil {
			logger.Warn("failed to write health response", "kind", kind, "error", err)
		}
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
