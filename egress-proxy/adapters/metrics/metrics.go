// Package metrics records egress-proxy's decisions and traffic as Prometheus
// series.
package metrics

import (
	"github.com/carolsimone/continuo/egress-proxy/service/ports"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	decisionAllowed = "allowed"
	decisionDenied  = "denied"
)

// Metrics is the Prometheus Observer. No series is labelled by host: refused
// hosts are unbounded, and the denial log names them.
type Metrics struct {
	requests *prometheus.CounterVec
	bytes    *prometheus.CounterVec
	tunnels  prometheus.Gauge
}

var _ ports.Observer = (*Metrics)(nil)

// New registers egress-proxy's series on reg. reasons is the closed set of
// reasons the caller passes to Request. Every decision/reason pair and both
// directions are exported at 0 from the start, so rate() sees the first
// increment and a scrape before any traffic lists the series.
func New(reg *pkgmetrics.Registry, reasons []string) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "continuo_egress_requests_total",
			Help: "Proxied requests and tunnels, by decision (allowed or denied) and reason."}, []string{"decision", "reason"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "continuo_egress_bytes_total",
			Help: "Bytes relayed, by direction: up to the destination, down to the agent."}, []string{"direction"}),
		tunnels: prometheus.NewGauge(prometheus.GaugeOpts{Name: "continuo_egress_active_tunnels",
			Help: "CONNECT tunnels open now."}),
	}
	for _, decision := range []string{decisionAllowed, decisionDenied} {
		for _, reason := range reasons {
			m.requests.WithLabelValues(decision, reason)
		}
	}
	for _, direction := range []string{"up", "down"} {
		m.bytes.WithLabelValues(direction)
	}
	reg.Register(m.requests, m.bytes, m.tunnels)
	return m
}

// Request counts one request or tunnel. reason must come from the closed set
// given to New: it is a label value, so callers keep it bounded.
func (m *Metrics) Request(allowed bool, reason string) {
	decision := decisionDenied
	if allowed {
		decision = decisionAllowed
	}
	m.requests.WithLabelValues(decision, reason).Inc()
}

// Transferred adds n relayed bytes in direction.
func (m *Metrics) Transferred(direction string, n int64) {
	m.bytes.WithLabelValues(direction).Add(float64(n))
}

// TunnelOpened counts an open tunnel.
func (m *Metrics) TunnelOpened() { m.tunnels.Inc() }

// TunnelClosed uncounts a closed tunnel.
func (m *Metrics) TunnelClosed() { m.tunnels.Dec() }
