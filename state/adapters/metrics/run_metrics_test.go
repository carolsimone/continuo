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

// The waiter gauge reports the last pass's count, the wait histogram observes
// the seconds each granted task waited, and the repair counter counts every
// lease deleted because its holder had ended.
func TestRunMetrics_LeaseSeries(t *testing.T) {
	reg := pkgmetrics.New("state")
	m := NewRunMetrics(reg)
	m.LeaseWaiters(4)
	m.LeaseWaiters(3)
	m.LeaseGranted(30 * time.Second)
	m.LeaseGranted(2 * time.Minute)
	m.LeaseRepaired()

	if got := testutil.ToFloat64(m.leaseWaiters); got != 3 {
		t.Fatalf("continuo_node_lease_waiters = %v, want 3", got)
	}
	if got := testutil.ToFloat64(m.leasesRepaired); got != 1 {
		t.Fatalf("continuo_node_leases_repaired_total = %v, want 1", got)
	}

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`continuo_node_lease_waiters{service="state"} 3`,
		`continuo_node_lease_wait_seconds_count{service="state"} 2`,
		`continuo_node_lease_wait_seconds_sum{service="state"} 150`,
		`continuo_node_leases_repaired_total{service="state"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %q", want)
		}
	}
}
