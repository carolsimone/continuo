package http

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/carolsimone/continuo/pkg/maintenance"
)

// writeMaintenance answers a request refused because maintenance mode is on:
// 503 with a Retry-After and the shared maintenance message and code.
func writeMaintenance(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", strconv.Itoa(maintenance.RetryAfterSeconds))
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": maintenance.Message, "code": maintenance.Code})
}
