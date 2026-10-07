package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/maintenance"
	"github.com/stretchr/testify/require"
)

func TestMaintenance_ReleaseIntakeAndRetryAnswer503(t *testing.T) {
	deps, _ := newRetryRemediationDeps(time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC))
	deps.Maintenance = true
	srv := newTestServer(deps)
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/releases", `{"service":"svc","release_id":"r1","image_tag":"t","repo":"o/r","commit_sha":"s"}`},
		{http.MethodPost, "/releases/r1/retry-remediation", ``},
	} {
		t.Run(tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)
			require.Equal(t, "300", rec.Header().Get("Retry-After"))
			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			require.Equal(t, maintenance.Code, body["code"])
			require.Equal(t, maintenance.Message, body["error"])
		})
	}
}
