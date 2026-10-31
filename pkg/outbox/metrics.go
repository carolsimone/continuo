package outbox

import (
	"context"
	"sync"
	"time"

	"github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// metricsScrapeTimeout bounds the backlog reads one scrape makes.
const metricsScrapeTimeout = 2 * time.Second

// BacklogSource is an outbox processor whose table is read at scrape time.
type BacklogSource interface {
	Table() string
	Backlog(ctx context.Context) (Backlog, error)
}

// Metrics is the Observer every outbox processor of a process reports to,
// and the collector of the outbox series: the rows each table published, the
// publishes that failed, and each watched table's backlog, read from the
// database inside each scrape. A process builds one, on its registry, and
// only a process with an outbox does.
type Metrics struct {
	published    *prometheus.CounterVec
	failures     *prometheus.CounterVec
	backlog      *prometheus.Desc
	oldest       *prometheus.Desc
	deadLettered *prometheus.Desc

	mu      sync.Mutex
	sources []BacklogSource
}

var _ Observer = (*Metrics)(nil)

// NewMetrics registers continuo_outbox_published_total,
// continuo_outbox_publish_failures_total, continuo_outbox_backlog,
// continuo_outbox_oldest_open_age_seconds and continuo_outbox_dead_letters
// on reg, each labelled by table.
func NewMetrics(reg *metrics.Registry) *Metrics {
	m := &Metrics{
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
	reg.Register(m)
	return m
}

// Watch reports src's backlog gauges.
func (m *Metrics) Watch(src BacklogSource) {
	m.mu.Lock()
	m.sources = append(m.sources, src)
	m.mu.Unlock()
}

// Published implements Observer.
func (m *Metrics) Published(table string, rows int) {
	m.published.WithLabelValues(table).Add(float64(rows))
}

// Failed implements Observer.
func (m *Metrics) Failed(table, kind string) { m.failures.WithLabelValues(table, kind).Inc() }

// Describe implements prometheus.Collector.
func (m *Metrics) Describe(ch chan<- *prometheus.Desc) {
	m.published.Describe(ch)
	m.failures.Describe(ch)
	ch <- m.backlog
	ch <- m.oldest
	ch <- m.deadLettered
}

// Collect implements prometheus.Collector. A table whose backlog cannot be
// read within the scrape's bound omits its gauges.
func (m *Metrics) Collect(ch chan<- prometheus.Metric) {
	m.published.Collect(ch)
	m.failures.Collect(ch)
	m.mu.Lock()
	sources := append([]BacklogSource(nil), m.sources...)
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), metricsScrapeTimeout)
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
