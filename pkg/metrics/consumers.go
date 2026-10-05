package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"
)

// scrapeTimeout bounds the Redis and Postgres reads one scrape makes.
const scrapeTimeout = 2 * time.Second

// Consumers returns the observer every stream consumer of this process reports to.
func (r *Registry) Consumers() pkgredis.Observer {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.consumers == nil {
		r.consumers = newConsumerMetrics()
		r.labelled.MustRegister(r.consumers)
	}
	return r.consumers
}

// WatchRedis gives the consumer metrics the client that answers XINFO GROUPS
// for the lag and pending gauges. Until it is called those gauges are absent.
func (r *Registry) WatchRedis(client *goredis.Client) {
	r.Consumers()
	r.consumers.setClient(client)
}

type consumerMetrics struct {
	handled     *prometheus.HistogramVec
	deadLetters *prometheus.CounterVec
	pauses      *prometheus.CounterVec
	lag         *prometheus.Desc
	pending     *prometheus.Desc

	mu     sync.Mutex
	client *goredis.Client
	groups map[string]map[string]bool // stream -> watched groups
}

func newConsumerMetrics() *consumerMetrics {
	return &consumerMetrics{
		handled: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "continuo_consumer_handler_duration_seconds",
			Help:    "Handler invocations by result (ok, permanent, infrastructure, transient) and their duration.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300},
		}, []string{"stream", "group", "result"}),
		deadLetters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_consumer_dead_letters_total",
			Help: "Messages written to the consumer dead-letter stream, by kind.",
		}, []string{"stream", "group", "kind"}),
		pauses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_consumer_infra_pauses_total",
			Help: "Pauses after an infrastructure error.",
		}, []string{"stream", "group"}),
		lag:     prometheus.NewDesc("continuo_consumer_lag", "Entries of the stream not yet delivered to the group.", []string{"stream", "group"}, nil),
		pending: prometheus.NewDesc("continuo_consumer_pending", "Entries delivered to the group and not yet acknowledged.", []string{"stream", "group"}, nil),
		groups:  map[string]map[string]bool{},
	}
}

func (m *consumerMetrics) setClient(c *goredis.Client) { m.mu.Lock(); m.client = c; m.mu.Unlock() }

func (m *consumerMetrics) Watch(stream, group string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.groups[stream] == nil {
		m.groups[stream] = map[string]bool{}
	}
	m.groups[stream][group] = true
}

func (m *consumerMetrics) Handled(stream, group, result string, d time.Duration) {
	m.handled.WithLabelValues(stream, group, result).Observe(d.Seconds())
}

func (m *consumerMetrics) DeadLettered(stream, group string, kind model.DeadLetterKind) {
	m.deadLetters.WithLabelValues(stream, group, string(kind)).Inc()
}

func (m *consumerMetrics) Paused(stream, group string) { m.pauses.WithLabelValues(stream, group).Inc() }

func (m *consumerMetrics) Describe(ch chan<- *prometheus.Desc) {
	m.handled.Describe(ch)
	m.deadLetters.Describe(ch)
	m.pauses.Describe(ch)
	ch <- m.lag
	ch <- m.pending
}

// Collect reads XINFO GROUPS once per watched stream. A stream Redis cannot
// answer for is skipped; Redis reports lag as unknown (negative) after some
// trims, and then the lag gauge is omitted.
func (m *consumerMetrics) Collect(ch chan<- prometheus.Metric) {
	m.handled.Collect(ch)
	m.deadLetters.Collect(ch)
	m.pauses.Collect(ch)
	client, watched := m.snapshot()
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), scrapeTimeout)
	defer cancel()
	for stream, groups := range watched {
		infos, err := client.XInfoGroups(ctx, stream).Result()
		if err != nil {
			continue
		}
		for _, g := range infos {
			if !groups[g.Name] {
				continue
			}
			ch <- prometheus.MustNewConstMetric(m.pending, prometheus.GaugeValue, float64(g.Pending), stream, g.Name)
			if g.Lag >= 0 {
				ch <- prometheus.MustNewConstMetric(m.lag, prometheus.GaugeValue, float64(g.Lag), stream, g.Name)
			}
		}
	}
}

func (m *consumerMetrics) snapshot() (*goredis.Client, map[string]map[string]bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]map[string]bool, len(m.groups))
	for s, gs := range m.groups {
		out[s] = make(map[string]bool, len(gs))
		for g := range gs {
			out[s][g] = true
		}
	}
	return m.client, out
}
