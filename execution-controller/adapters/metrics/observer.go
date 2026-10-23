// Package metrics holds execution-controller's own Prometheus series.
package metrics

import (
	"sync"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// Observer counts the attempts that have waited past the alert threshold for
// a pod to stop (a pod being deleted, or on a node that stopped reporting),
// the attempts reported stopped, and the Jobs deleted because no row deploys
// them.
type Observer struct {
	mu       sync.Mutex
	waiting  map[string]bool
	awaiting prometheus.Gauge
	stopped  prometheus.Counter
	reaped   *prometheus.CounterVec
}

var (
	_ ports.ObserverMetrics = (*Observer)(nil)
	_ ports.StopMetrics     = (*Observer)(nil)
)

// NewObserver registers continuo_attempts_awaiting_pod_stop,
// continuo_attempts_stopped_total and continuo_fenced_jobs_deleted_total on
// reg.
func NewObserver(reg *pkgmetrics.Registry) *Observer {
	o := &Observer{waiting: map[string]bool{},
		awaiting: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "continuo_attempts_awaiting_pod_stop",
			Help: "Attempts waiting past the alert threshold for a pod to stop; force-delete the pod to end the attempt.",
		}),
		stopped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "continuo_attempts_stopped_total",
			Help: "Attempts reported stopped because their run was cancelled.",
		}),
		reaped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_fenced_jobs_deleted_total",
			Help: "Jobs deleted because no row deploys them: their run is fenced (fenced_run), or their row ended before it was deployed (ended_undeployed).",
		}, []string{"reason"}),
	}
	reg.Register(o.awaiting)
	reg.Register(o.stopped)
	reg.Register(o.reaped)
	return o
}

// AttemptStopped counts an attempt reported stopped.
func (o *Observer) AttemptStopped() { o.stopped.Inc() }

// JobReaped counts a Job deleted for reason.
func (o *Observer) JobReaped(reason string) { o.reaped.WithLabelValues(reason).Inc() }

// AwaitingPodStop marks or clears jobName's attempt and sets the gauge to the
// number marked.
func (o *Observer) AwaitingPodStop(jobName string, waiting bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if waiting {
		o.waiting[jobName] = true
	} else {
		delete(o.waiting, jobName)
	}
	o.awaiting.Set(float64(len(o.waiting)))
}
