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
// the node write leases the lease waker serves: the tasks waiting, how long
// each granted task waited, and the leases it repaired.
type RunMetrics struct {
	stalled           prometheus.Counter
	cancelling        prometheus.Gauge
	cancellingOverdue prometheus.Gauge
	cancelSeconds     prometheus.Histogram
	leaseWaiters      prometheus.Gauge
	leaseWaitSeconds  prometheus.Histogram
	leasesRepaired    prometheus.Counter
}

var _ ports.RunMetrics = (*RunMetrics)(nil)

// NewRunMetrics registers continuo_runs_stalled_total, continuo_runs_cancelling,
// continuo_runs_cancelling_overdue, continuo_run_cancel_seconds,
// continuo_node_lease_waiters, continuo_node_lease_wait_seconds and
// continuo_node_leases_repaired_total on reg.
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
			Help:    "Seconds a task waited for the write lease on its node before the lease waker granted it.",
			Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400},
		}),
		leasesRepaired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "continuo_node_leases_repaired_total",
			Help: "Node write leases the lease waker deleted because their task had ended without releasing them, which only a defect in state causes.",
		}),
	}
	reg.Register(m.stalled, m.cancelling, m.cancellingOverdue, m.cancelSeconds, m.leaseWaiters, m.leaseWaitSeconds, m.leasesRepaired)
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
