package metrics

import (
	"context"
	"sync"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/prometheus/client_golang/prometheus"
)

// BacklogSource is an outbox processor whose table is read at scrape time.
type BacklogSource interface {
	Table() string
	Backlog(ctx context.Context) (pkgoutbox.Backlog, error)
}

// Outbox returns the observer every outbox processor of this process reports to.
func (r *Registry) Outbox() pkgoutbox.Observer {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outbox == nil {
		r.outbox = newOutboxMetrics()
		r.labelled.MustRegister(r.outbox)
	}
	return r.outbox
}

// WatchOutbox reports src's backlog gauges.
func (r *Registry) WatchOutbox(src BacklogSource) {
	r.Outbox()
	r.outbox.add(src)
}

type outboxMetrics struct {
	published    *prometheus.CounterVec
	failures     *prometheus.CounterVec
	backlog      *prometheus.Desc
	oldest       *prometheus.Desc
	deadLettered *prometheus.Desc

	mu      sync.Mutex
	sources []BacklogSource
}

func newOutboxMetrics() *outboxMetrics {
	return &outboxMetrics{
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_outbox_published_total", Help: "Outbox rows published.",
		}, []string{"table"}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_outbox_publish_failures_total",
			Help: "Failed outbox publishes: retry for a rescheduled row, otherwise the dead-letter kind.",
		}, []string{"table", "kind"}),
		backlog:      prometheus.NewDesc("continuo_outbox_backlog", "Pending and scheduled outbox rows.", []string{"table"}, nil),
		oldest:       prometheus.NewDesc("continuo_outbox_oldest_open_age_seconds", "Age of the oldest pending or scheduled row.", []string{"table"}, nil),
		deadLettered: prometheus.NewDesc("continuo_outbox_dead_letters", "Outbox rows parked as failed.", []string{"table"}, nil),
	}
}

func (m *outboxMetrics) add(src BacklogSource) {
	m.mu.Lock()
	m.sources = append(m.sources, src)
	m.mu.Unlock()
}

func (m *outboxMetrics) Published(table string, rows int) {
	m.published.WithLabelValues(table).Add(float64(rows))
}
func (m *outboxMetrics) Failed(table, kind string) { m.failures.WithLabelValues(table, kind).Inc() }

func (m *outboxMetrics) Describe(ch chan<- *prometheus.Desc) {
	m.published.Describe(ch)
	m.failures.Describe(ch)
	ch <- m.backlog
	ch <- m.oldest
	ch <- m.deadLettered
}

func (m *outboxMetrics) Collect(ch chan<- prometheus.Metric) {
	m.published.Collect(ch)
	m.failures.Collect(ch)
	m.mu.Lock()
	sources := append([]BacklogSource(nil), m.sources...)
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
	defer cancel()
	for _, src := range sources {
		b, err := src.Backlog(ctx)
		if err != nil {
			continue
		}
		ch <- prometheus.MustNewConstMetric(m.backlog, prometheus.GaugeValue, float64(b.Open), src.Table())
		ch <- prometheus.MustNewConstMetric(m.oldest, prometheus.GaugeValue, b.OldestOpenAge.Seconds(), src.Table())
		ch <- prometheus.MustNewConstMetric(m.deadLettered, prometheus.GaugeValue, float64(b.DeadLettered), src.Table())
	}
}
