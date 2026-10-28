// Package metrics holds state's own Prometheus series.
package metrics

import (
	"time"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/prometheus/client_golang/prometheus"
)

// RunMetrics counts the runs state's reconcile loop failed as stalled, gauges
// the runs that are cancelling, times how long a cancel takes, and records
// the node write leases state grants: the tasks waiting, how long each
// granted task waited, the leases the lease waker repaired, and how each call
// of a command that creates or cancels a run ended. It counts the outcomes of
// the cron loop's fires and gauges the schedules scheduled and paused.
type RunMetrics struct {
	stalled           prometheus.Counter
	cancelling        prometheus.Gauge
	cancellingOverdue prometheus.Gauge
	cancelSeconds     prometheus.Histogram
	leaseWaiters      prometheus.Gauge
	leaseWaitSeconds  prometheus.Histogram
	leasesRepaired    prometheus.Counter
	commandRequests   *prometheus.CounterVec
	cronFires         *prometheus.CounterVec
	schedules         *prometheus.GaugeVec
}

var _ ports.RunMetrics = (*RunMetrics)(nil)

// NewRunMetrics registers continuo_runs_stalled_total, continuo_runs_cancelling,
// continuo_runs_cancelling_overdue, continuo_run_cancel_seconds,
// continuo_node_lease_waiters, continuo_node_lease_wait_seconds,
// continuo_node_leases_repaired_total, continuo_command_requests_total,
// continuo_cron_fires_total and continuo_schedules on reg.
func NewRunMetrics(reg *pkgmetrics.Registry) *RunMetrics {
	m := &RunMetrics{
		stalled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "continuo_runs_stalled_total",
			Help: "Runs state failed as stalled: no attempt in flight and no progress for five minutes, which only a defect in state causes.",
		}),
		cancelling: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "continuo_runs_cancelling",
			Help: "Runs that are cancelling: cancelled, with attempts execution-controller has not yet reported stopped.",
		}),
		cancellingOverdue: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "continuo_runs_cancelling_overdue",
			Help: "Cancelling runs whose cancel is older than the overdue limit and still waits for attempts to stop.",
		}),
		cancelSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "continuo_run_cancel_seconds",
			Help:    "Seconds from a run's cancel to its end as cancelled.",
			Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 900, 1800, 3600},
		}),
		leaseWaiters: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "continuo_node_lease_waiters",
			Help: "Tasks waiting for the write lease on their node, which another run's task holds while it writes the node.",
		}),
		leaseWaitSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "continuo_node_lease_wait_seconds",
			Help:    "Seconds a task waited for the write lease on its node before state granted it.",
			Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400},
		}),
		leasesRepaired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "continuo_node_leases_repaired_total",
			Help: "Node write leases the lease waker deleted because their task had ended without releasing them, which only a defect in state causes.",
		}),
		commandRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_command_requests_total",
			Help: "Calls of the commands that create or cancel a run, by command and outcome: created, replayed (a repeat of a request id answered from the first call), conflict (a request id reused with other parameters) or no_run (the command acted on no run).",
		}, []string{"command", "outcome"}),
		cronFires: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_cron_fires_total",
			Help: "Fires of the schedules' crons, by outcome: started (a run started), skipped_active (the schedule's previous run is still active), skipped_maintenance (maintenance mode is on), empty_selection (a failed run recorded, the live release giving the schedule nothing to run), or missed (one older slot left unfired by the fire of the latest one, counted per slot).",
		}, []string{"outcome"}),
		schedules: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "continuo_schedules",
			Help: "Schedules with a cron, of every tenant, by state: scheduled or paused.",
		}, []string{"state"}),
	}
	reg.Register(m.stalled, m.cancelling, m.cancellingOverdue, m.cancelSeconds, m.leaseWaiters, m.leaseWaitSeconds, m.leasesRepaired,
		m.commandRequests, m.cronFires, m.schedules)
	return m
}

// RunStalled counts one run failed as stalled.
func (m *RunMetrics) RunStalled() { m.stalled.Inc() }

// RunsCancelling sets the cancelling gauges.
func (m *RunMetrics) RunsCancelling(total, overdue int) {
	m.cancelling.Set(float64(total))
	m.cancellingOverdue.Set(float64(overdue))
}

// RunCancelled observes the seconds a run took from its cancel to its end.
func (m *RunMetrics) RunCancelled(after time.Duration) { m.cancelSeconds.Observe(after.Seconds()) }

// LeaseWaiters sets the gauge of the tasks waiting for a node lease.
func (m *RunMetrics) LeaseWaiters(n int) { m.leaseWaiters.Set(float64(n)) }

// LeaseGranted observes the seconds a granted task waited for its lease.
func (m *RunMetrics) LeaseGranted(waited time.Duration) { m.leaseWaitSeconds.Observe(waited.Seconds()) }

// LeaseRepaired counts one repaired lease.
func (m *RunMetrics) LeaseRepaired() { m.leasesRepaired.Inc() }

// CommandRequest counts one call of command under its outcome.
func (m *RunMetrics) CommandRequest(command, outcome string) {
	m.commandRequests.WithLabelValues(command, outcome).Inc()
}

// CronFire counts one fire outcome of a schedule's cron.
func (m *RunMetrics) CronFire(outcome string) { m.cronFires.WithLabelValues(outcome).Inc() }

// Schedules sets the gauges of the scheduled and the paused schedules.
func (m *RunMetrics) Schedules(scheduled, paused int) {
	m.schedules.WithLabelValues("scheduled").Set(float64(scheduled))
	m.schedules.WithLabelValues("paused").Set(float64(paused))
}
