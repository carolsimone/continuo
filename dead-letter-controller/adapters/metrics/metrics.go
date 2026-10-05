// Package metrics exports dead-letter-controller's Prometheus series.
package metrics

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// BacklogSource reads the open dead letters at scrape time.
type BacklogSource interface {
	Backlog(ctx context.Context) ([]deadletter.BacklogRow, error)
}

// Metrics counts stored, redriven and expired dead letters and reports the open
// backlog when scraped.
type Metrics struct {
	recorded *prometheus.CounterVec
	redriven *prometheus.CounterVec
	expired  *prometheus.CounterVec
	backlog  BacklogSource
	open     *prometheus.Desc
	oldest   *prometheus.Desc
}

var _ ports.Observer = (*Metrics)(nil)

func New(reg *pkgmetrics.Registry, backlog BacklogSource) *Metrics {
	m := &Metrics{
		recorded: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "continuo_dead_letters_total",
			Help: "Dead letters stored, by source, stream, consumer group and failure kind."}, []string{"source", "stream", "group", "kind"}),
		redriven: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "continuo_dead_letters_redriven_total",
			Help: "Dead letters redriven to their original stream."}, []string{"source", "stream"}),
		expired: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "continuo_dead_letters_expired_total",
			Help: "Dead letters deleted past the 30-day replay horizon."}, []string{"source"}),
		backlog: backlog,
		open: prometheus.NewDesc("continuo_dead_letter_backlog", "Open dead letters, read when scraped.",
			[]string{"source", "stream", "kind"}, nil),
		oldest: prometheus.NewDesc("continuo_dead_letter_oldest_open_age_seconds",
			"Age of the oldest open dead letter per source, read when scraped.", []string{"source"}, nil),
	}
	reg.Register(m.recorded, m.redriven, m.expired, m)
	return m
}

func (m *Metrics) Recorded(dl deadletter.DeadLetter) {
	m.recorded.WithLabelValues(string(dl.Source), dl.Stream, dl.Group, string(dl.FailureKind)).Inc()
}
func (m *Metrics) Redriven(dl deadletter.DeadLetter) {
	m.redriven.WithLabelValues(string(dl.Source), dl.Stream).Inc()
}
func (m *Metrics) Expired(dl deadletter.DeadLetter) {
	m.expired.WithLabelValues(string(dl.Source)).Inc()
}

func (m *Metrics) Describe(ch chan<- *prometheus.Desc) { ch <- m.open; ch <- m.oldest }

func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := m.backlog.Backlog(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(m.open, err)
		return
	}
	oldest := map[deadletter.Source]time.Time{}
	for _, r := range rows {
		ch <- prometheus.MustNewConstMetric(m.open, prometheus.GaugeValue, float64(r.Open), string(r.Source), r.Stream, string(r.Kind))
		if o, ok := oldest[r.Source]; !ok || r.OldestRecordedAt.Before(o) {
			oldest[r.Source] = r.OldestRecordedAt
		}
	}
	for src, t := range oldest {
		ch <- prometheus.MustNewConstMetric(m.oldest, prometheus.GaugeValue, time.Since(t).Seconds(), string(src))
	}
}
