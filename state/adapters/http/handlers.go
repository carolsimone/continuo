package http

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/carolsimone/continuo/pkg/liveness"
)

// HealthResponse represents the health check response
type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
}

// HealthHandler handles liveness check requests. It returns 200 as long as the
// process is running and able to serve HTTP.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := HealthResponse{
		Status:  "healthy",
		Service: "state",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("HealthHandler: failed to encode response", "error", err)
	}
}

// ReadinessResponse represents the readiness check response.
type ReadinessResponse struct {
	Status    string   `json:"status"`
	Service   string   `json:"service"`
	Unhealthy []string `json:"unhealthy,omitempty"`
}

// NewReadinessHandler returns a readiness handler backed by the liveness
// registry. It responds 503 when any registered consumer has exited with an
// error or any cached dependency probe fails, so Kubernetes stops routing
// traffic to an unhealthy pod.
func NewReadinessHandler(registry *liveness.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		failures := registry.Check(r.Context())
		if len(failures) == 0 {
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(ReadinessResponse{Status: "ready", Service: "state"}); err != nil {
				slog.Error("ReadinessHandler: failed to encode response", "error", err)
			}
			return
		}
		names := make([]string, 0, len(failures))
		for _, f := range failures {
			names = append(names, f.Name)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		if err := json.NewEncoder(w).Encode(ReadinessResponse{Status: "not_ready", Service: "state", Unhealthy: names}); err != nil {
			slog.Error("ReadinessHandler: failed to encode response", "error", err)
		}
	}
}
