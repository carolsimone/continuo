// Package metrics holds execution-controller's own Prometheus series.
package metrics

import (
	"sync"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// Observer counts the attempts that have waited past the alert threshold for
// a pod to stop: a pod being deleted, or on a node that stopped reporting.
type Observer struct {
	mu       sync.Mutex
	waiting  map[string]bool
	awaiting prometheus.Gauge
}

var _ ports.ObserverMetrics = (*Observer)(nil)

// NewObserver registers continuo_attempts_awaiting_pod_stop on reg.
func NewObserver(reg *pkgmetrics.Registry) *Observer {
	o := &Observer{waiting: map[string]bool{}, awaiting: prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "continuo_attempts_awaiting_pod_stop",
		Help: "Attempts waiting past the alert threshold for a pod to stop; force-delete the pod to end the attempt.",
	})}
	reg.Register(o.awaiting)
	return o
}

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
