// Package metrics holds state's own Prometheus series.
package metrics

import (
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/prometheus/client_golang/prometheus"
)

// RunMetrics counts the runs state's reconcile loop failed as stalled.
type RunMetrics struct {
	stalled prometheus.Counter
}

var _ ports.RunMetrics = (*RunMetrics)(nil)

// NewRunMetrics registers continuo_runs_stalled_total on reg.
func NewRunMetrics(reg *pkgmetrics.Registry) *RunMetrics {
	m := &RunMetrics{stalled: prometheus.NewCounter(prometheus.CounterOpts{
		Name: "continuo_runs_stalled_total",
		Help: "Runs state failed as stalled: no attempt in flight and no progress for five minutes, which only a defect in state causes.",
	})}
	reg.Register(m.stalled)
	return m
}

// RunStalled counts one run failed as stalled.
func (m *RunMetrics) RunStalled() { m.stalled.Inc() }
