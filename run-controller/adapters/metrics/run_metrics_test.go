package metrics

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/run-controller/domain/schedule"
	"github.com/carolsimone/continuo/run-controller/service/cronloop"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRunMetrics_CountsTheStalledRuns(t *testing.T) {
	m := NewRunMetrics(pkgmetrics.New("run-controller"))
	m.RunStalled()
	m.RunStalled()
	if got := testutil.ToFloat64(m.stalled); got != 2 {
		t.Fatalf("continuo_runs_stalled_total = %v, want 2", got)
	}
}

// The cancelling gauges report the last pass's counts; the histogram observes
// the seconds between a run's cancel and its end.
func TestRunMetrics_CancellingGaugesAndCancelDuration(t *testing.T) {
	reg := pkgmetrics.New("run-controller")
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
		`continuo_runs_cancelling{service="run-controller"} 5`,
		`continuo_runs_cancelling_overdue{service="run-controller"} 4`,
		`continuo_run_cancel_seconds_count{service="run-controller"} 2`,
		`continuo_run_cancel_seconds_sum{service="run-controller"} 690`,
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
	reg := pkgmetrics.New("run-controller")
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
		`continuo_node_lease_waiters{service="run-controller"} 3`,
		`continuo_node_lease_wait_seconds_count{service="run-controller"} 2`,
		`continuo_node_lease_wait_seconds_sum{service="run-controller"} 150`,
		`continuo_node_leases_repaired_total{service="run-controller"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %q", want)
		}
	}
}

// The cron fire counter counts each outcome, failed included, and the missed
// slots of a fire all at once, and its help names failed; the schedules gauge
// reports the last pass's counts by state.
func TestRunMetrics_CronFiresAndSchedules(t *testing.T) {
	reg := pkgmetrics.New("run-controller")
	m := NewRunMetrics(reg)
	m.CronFire("started")
	m.CronFires("missed", 2)
	m.CronFires("missed", 1000)
	m.CronFire("skipped_active")
	m.CronFire("invalid_node_type")
	m.CronFire("failed")
	m.CronFire("failed")
	m.Schedules(4, 2, 1)
	m.Schedules(3, 1, 2)

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`continuo_cron_fires_total{outcome="started",service="run-controller"} 1`,
		`continuo_cron_fires_total{outcome="missed",service="run-controller"} 1002`,
		`continuo_cron_fires_total{outcome="skipped_active",service="run-controller"} 1`,
		`continuo_cron_fires_total{outcome="invalid_node_type",service="run-controller"} 1`,
		`continuo_cron_fires_total{outcome="failed",service="run-controller"} 2`,
		"or failed (a fire that returned an error or did not end within 30 seconds",
		`continuo_schedules{service="run-controller",state="scheduled"} 3`,
		`continuo_schedules{service="run-controller",state="paused"} 1`,
		`continuo_schedules{service="run-controller",state="unparsable"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %q", want)
		}
	}
}

// The cron fire counter's help states two tuning values, the fire timeout and
// the cap on the missed slots counted per fire; it names them as the code
// holds them, so changing either constant fails here until the help follows.
func TestRunMetrics_CronFiresHelpStatesTheTuningConstants(t *testing.T) {
	reg := pkgmetrics.New("run-controller")
	NewRunMetrics(reg).CronFire("started") // a counter vector exposes its help once it has a series

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	for _, want := range []string{
		fmt.Sprintf("did not end within %d seconds", int(cronloop.FireTimeout/time.Second)),
		fmt.Sprintf("at most %d per fire", schedule.MaxCountedSkips),
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the continuo_cron_fires_total help lacks %q", want)
		}
	}
}
