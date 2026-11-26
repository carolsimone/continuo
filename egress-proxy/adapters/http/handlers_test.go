package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/carolsimone/continuo/egress-proxy/config"
	"github.com/carolsimone/continuo/pkg/liveness"
)

// TestProbeBodiesNameTheComponent holds every probe body to the key component,
// naming config.Name, and never the key service.
func TestProbeBodiesNameTheComponent(t *testing.T) {
	live := liveness.NewRegistry()
	live.RegisterWorker("proxy")

	failing := liveness.NewRegistry()
	failing.RegisterWorker("proxy")
	failing.WorkerExited("proxy", errors.New("boom"))

	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantStatus string
	}{
		{"health", HealthHandler, "healthy"},
		{"ready", NewReadinessHandler(live), "ready"},
		{"not ready", NewReadinessHandler(failing), "not_ready"},
		{"live", NewLivenessHandler(live), "live"},
		{"not live", NewLivenessHandler(failing), "not_live"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.handler(rec, httptest.NewRequest(http.MethodGet, "/probe", nil))

			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode %q: %v", rec.Body.String(), err)
			}
			if body["status"] != tc.wantStatus {
				t.Errorf("status = %v, want %q", body["status"], tc.wantStatus)
			}
			if body["component"] != config.Name {
				t.Errorf("component = %v, want config.Name %q", body["component"], config.Name)
			}
			if _, ok := body["service"]; ok {
				t.Errorf("body %s names the component under the key service, want component", rec.Body.String())
			}
		})
	}
}
