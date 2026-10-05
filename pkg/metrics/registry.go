// Package metrics serves a process's Prometheus metrics on a listener of its
// own. Every series carries the label service. Consumer lag and outbox backlog
// are read from Redis and Postgres inside each scrape, so a process nobody
// scrapes does no metrics I/O.
package metrics

import (
	"database/sql"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds one process's metrics.
type Registry struct {
	reg       *prometheus.Registry
	labelled  prometheus.Registerer
	mu        sync.Mutex
	consumers *consumerMetrics
	outbox    *outboxMetrics
}

// New returns a registry whose series carry service=<service>, with Go
// runtime and process metrics registered.
func New(service string) *Registry {
	reg := prometheus.NewRegistry()
	r := &Registry{reg: reg, labelled: prometheus.WrapRegistererWith(prometheus.Labels{"service": service}, reg)}
	r.labelled.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return r
}

// Handler serves the registry in the Prometheus text format. A collector
// failing mid-scrape omits its series instead of failing the scrape.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})
}

// WatchDB reports db's connection-pool statistics as go_sql_* series
// labelled db_name=<name>.
func (r *Registry) WatchDB(db *sql.DB, name string) {
	r.labelled.MustRegister(collectors.NewDBStatsCollector(db, name))
}

// Register adds service-specific collectors; their series carry the service label.
func (r *Registry) Register(cs ...prometheus.Collector) { r.labelled.MustRegister(cs...) }
