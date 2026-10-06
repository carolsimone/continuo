package metrics

import (
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// Trim counts what the trim loop quarantines and removes, and when it last
// completed a run without error.
type Trim struct {
	dl          *Metrics
	quarantined *prometheus.CounterVec
	trimmed     *prometheus.CounterVec
	lastSuccess prometheus.Gauge
}

var _ ports.TrimObserver = (*Trim)(nil)

func NewTrim(reg *pkgmetrics.Registry, dl *Metrics) *Trim {
	t := &Trim{
		dl: dl,
		quarantined: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "continuo_stream_quarantined_total",
			Help: "Stream entries stored as trimmed dead letters before the retention cap removed them."}, []string{"stream", "group"}),
		trimmed: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "continuo_stream_trimmed_entries_total",
			Help: "Stream entries removed by the trim loop."}, []string{"stream"}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{Name: "continuo_stream_trim_last_success_timestamp_seconds",
			Help: "Unix time of the last trim run that completed without error."}),
	}
	reg.Register(t.quarantined, t.trimmed, t.lastSuccess)
	return t
}

func (t *Trim) Quarantined(stream, group string, n int) {
	t.quarantined.WithLabelValues(stream, group).Add(float64(n))
	t.dl.recorded.WithLabelValues(string(deadletter.SourceQuarantine), stream, group, string(model.DeadLetterKindTrimmed)).Add(float64(n))
}
func (t *Trim) Trimmed(stream string, n int64) { t.trimmed.WithLabelValues(stream).Add(float64(n)) }
func (t *Trim) TrimSucceeded(at time.Time)     { t.lastSuccess.Set(float64(at.Unix())) }
