package liveness

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
)

// Handler maps a registry check to an HTTP health response, hoisting the glue
// every service repeats: 200 with body "<kind> OK" when the check reports no
// failures, or 503 with body "NOT <kind>: N component(s) unhealthy" (logging
// each failing component at Warn) otherwise. kind names the probe ("readiness"
// or "liveness") in the body and logs.
//
// Wire a readiness endpoint to (*Registry).Check and a liveness endpoint to
// (*Registry).LivenessCheck, so a dependency outage flips readiness only while a
// dead/wedged worker flips both.
func Handler(kind string, check func(context.Context) []Failure, logger *slog.Logger) http.HandlerFunc {
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
