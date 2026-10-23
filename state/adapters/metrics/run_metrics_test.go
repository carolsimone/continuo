package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRunMetrics_CountsTheStalledRuns(t *testing.T) {
	m := NewRunMetrics(pkgmetrics.New("state"))
	m.RunStalled()
	m.RunStalled()
	if got := testutil.ToFloat64(m.stalled); got != 2 {
		t.Fatalf("continuo_runs_stalled_total = %v, want 2", got)
	}
}

// The cancelling gauges report the last pass's counts; the histogram observes
// the seconds between a run's cancel and its end.
func TestRunMetrics_CancellingGaugesAndCancelDuration(t *testing.T) {
	reg := pkgmetrics.New("state")
	m := NewRunMetrics(reg)
	m.RunsCancelling(3, 1)
	m.RunsCancelling(2, 0)
	m.RunCancelled(90 * time.Second)
	m.RunCancelled(10 * time.Minute)

	if got := testutil.ToFloat64(m.cancelling); got != 2 {
		t.Fatalf("continuo_runs_cancelling = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.cancellingOverdue); got != 0 {
		t.Fatalf("continuo_runs_cancelling_overdue = %v, want 0", got)
	}
	m.RunsCancelling(5, 4)
	if got := testutil.ToFloat64(m.cancellingOverdue); got != 4 {
		t.Fatalf("continuo_runs_cancelling_overdue = %v, want 4", got)
	}

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`continuo_runs_cancelling{service="state"} 5`,
		`continuo_runs_cancelling_overdue{service="state"} 4`,
		`continuo_run_cancel_seconds_count{service="state"} 2`,
		`continuo_run_cancel_seconds_sum{service="state"} 690`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %q", want)
		}
	}
}
