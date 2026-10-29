package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
)

// Each outcome of a command is counted under its command name.
func TestRunMetrics_CountsCommandRequestOutcomes(t *testing.T) {
	reg := pkgmetrics.New("run-controller")
	m := NewRunMetrics(reg)
	m.CommandRequest("start_run", "created")
	m.CommandRequest("start_run", "created")
	m.CommandRequest("start_run", "replayed")
	m.CommandRequest("cancel_active_run", "no_run")
	m.CommandRequest("start_run", "conflict")

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`continuo_command_requests_total{command="start_run",outcome="created",service="run-controller"} 2`,
		`continuo_command_requests_total{command="start_run",outcome="replayed",service="run-controller"} 1`,
		`continuo_command_requests_total{command="start_run",outcome="conflict",service="run-controller"} 1`,
		`continuo_command_requests_total{command="cancel_active_run",outcome="no_run",service="run-controller"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %q\n%s", want, body)
		}
	}
}
