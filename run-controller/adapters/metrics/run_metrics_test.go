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

// Every action and outcome of a seed load, and every reason that stops one,
// is exported at 0 from boot; a seed load that ends raises its own sample.
func TestRunMetrics_SeedReloadSeriesStartAtZero(t *testing.T) {
	reg := pkgmetrics.New("run-controller")
	m := NewRunMetrics(reg)

	body := scrape(reg)
	for _, action := range []string{"load", "full_refresh"} {
		for _, outcome := range []string{"loaded", "stopped", "refused", "failed"} {
			want := fmt.Sprintf(`continuo_seed_reloads_total{action=%q,outcome=%q,service="run-controller"} 0`, action, outcome)
			if !strings.Contains(body, want) {
				t.Errorf("metrics output lacks %q", want)
			}
		}
	}
	for _, reason := range []string{"unowned_dependents", "engine_mismatch", "seed_full_refresh_disabled", "seed_full_refresh_undefined", "full_refresh_undefined"} {
		want := fmt.Sprintf(`continuo_seed_reload_stops_total{reason=%q,service="run-controller"} 0`, reason)
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %q", want)
		}
	}

	m.SeedReloadEnded("load", "loaded")
	m.SeedReloadStopped("unowned_dependents")
	body = scrape(reg)
	for _, want := range []string{
		`continuo_seed_reloads_total{action="load",outcome="loaded",service="run-controller"} 1`,
		`continuo_seed_reloads_total{action="full_refresh",outcome="loaded",service="run-controller"} 0`,
		`continuo_seed_reload_stops_total{reason="unowned_dependents",service="run-controller"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %q", want)
		}
	}
}

// The outage histogram observes the seconds a view was missing or stale until
// its rebuild succeeded, in the buckets from 5 s to a day.
func TestRunMetrics_ViewRebuiltObservesTheOutage(t *testing.T) {
	reg := pkgmetrics.New("run-controller")
	m := NewRunMetrics(reg)
	m.ViewRebuilt(90 * time.Second)

	body := scrape(reg)
	for _, want := range []string{
		`continuo_view_outage_seconds_bucket{service="run-controller",le="60"} 0`,
		`continuo_view_outage_seconds_bucket{service="run-controller",le="120"} 1`,
		`continuo_view_outage_seconds_bucket{service="run-controller",le="86400"} 1`,
		`continuo_view_outage_seconds_count{service="run-controller"} 1`,
		`continuo_view_outage_seconds_sum{service="run-controller"} 90`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output lacks %q", want)
		}
	}
}

// scrape returns what reg's /metrics handler serves.
func scrape(reg *pkgmetrics.Registry) string {
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}
