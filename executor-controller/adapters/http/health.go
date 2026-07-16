package http

import (
	"log/slog"
	"net/http"
)

// registerHealth installs the liveness and readiness probes. They carry no
// credential: Kubernetes reads them, not worker pods, and a probe that could
// fail authentication would take the pod down.
func registerHealth(mux *http.ServeMux, logger *slog.Logger) {
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
}
