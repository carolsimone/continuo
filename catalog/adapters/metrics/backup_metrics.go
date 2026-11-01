// Package metrics holds catalog's own Prometheus series: those of the
// nightly export of each tenant's graph.
package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// lastSuccessCacheFor is how long a read of the stored exports serves the
	// last-success gauge, so frequent scrapes list the store at most once a
	// minute.
	lastSuccessCacheFor = time.Minute
	// lastSuccessReadTimeout bounds one read of the tenants and their
	// exports, well under Prometheus's default scrape timeout of 10 s.
	lastSuccessReadTimeout = 5 * time.Second
)

// BackupMetrics counts the exports this process runs, by outcome, and
// reports the duration, size and records of the last stored one. Its
// last-success gauge is read from the store when scraped, so it holds every
// export, whichever process took it, and survives a restart.
type BackupMetrics struct {
	exports     *prometheus.CounterVec
	duration    prometheus.Histogram
	bytes       prometheus.Gauge
	records     *prometheus.GaugeVec
	lastSuccess *lastSuccess
}

var _ ports.BackupMetrics = (*BackupMetrics)(nil)

// NewBackupMetrics registers continuo_catalog_exports_total,
// continuo_catalog_export_duration_seconds, continuo_catalog_export_bytes,
// continuo_catalog_export_records and
// continuo_catalog_export_last_success_timestamp_seconds on reg. The last of
// those reads the tenants' stored exports through freshness; clock times its
// cache.
func NewBackupMetrics(reg *pkgmetrics.Registry, freshness ports.ExportFreshness, clock func() time.Time, logger *slog.Logger) *BackupMetrics {
	m := &BackupMetrics{
		exports: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_catalog_exports_total",
			Help: "Exports of a tenant's catalog graph this process ran, by outcome: success (the export was stored), failure (nothing was stored) or skipped (another export or a restore held the tenant's lease).",
		}, []string{"outcome"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "continuo_catalog_export_duration_seconds",
			Help:    "Seconds each stored export held its tenant's export lease, during which the tenant's catalog writes were paused.",
			Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200},
		}),
		bytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "continuo_catalog_export_bytes",
			Help: "Size in bytes of the last export this process stored.",
		}),
		records: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "continuo_catalog_export_records",
			Help: "Records of the last export this process stored, by kind: node or relationship.",
		}, []string{"kind"}),
		lastSuccess: &lastSuccess{
			desc: prometheus.NewDesc("continuo_catalog_export_last_success_timestamp_seconds",
				"Unix time of the newest stored export of the tenant whose newest export is the oldest, 0 when a tenant has none; read from object storage at most once a minute.",
				nil, nil),
			freshness: freshness, clock: clock, logger: logger,
		},
	}
	for _, outcome := range []string{ports.ExportSuccess, ports.ExportFailure, ports.ExportSkipped} {
		m.exports.WithLabelValues(outcome)
	}
	reg.Register(m.exports, m.duration, m.bytes, m.records, m.lastSuccess)
	return m
}

// ExportObserved counts one export under its outcome; a success also sets
// the duration, size and record series.
func (m *BackupMetrics) ExportObserved(outcome string, d time.Duration, bytes int64, nodes, rels int64) {
	m.exports.WithLabelValues(outcome).Inc()
	if outcome != ports.ExportSuccess {
		return
	}
	m.duration.Observe(d.Seconds())
	m.bytes.Set(float64(bytes))
	m.records.WithLabelValues("node").Set(float64(nodes))
	m.records.WithLabelValues("relationship").Set(float64(rels))
}

// lastSuccess is the collector of the last-success gauge. A scrape reads the
// exports' freshness at most once per lastSuccessCacheFor; a
// read that fails is logged and the last value stays. The read holds no lock,
// so a scrape that comes while another one reads serves the last value at
// once instead of waiting. With no tenant, or no read that succeeded yet, the
// gauge has no sample.
type lastSuccess struct {
	desc      *prometheus.Desc
	freshness ports.ExportFreshness
	clock     func() time.Time
	logger    *slog.Logger

	mu     sync.Mutex
	readAt time.Time
	value  float64
	known  bool
}

func (c *lastSuccess) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *lastSuccess) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	now := c.clock()
	due := c.readAt.IsZero() || now.Sub(c.readAt) >= lastSuccessCacheFor
	if due {
		// Taking the read's turn before the lock is let go keeps every other
		// scrape of the next minute from reading too.
		c.readAt = now
	}
	c.mu.Unlock()

	if due {
		value, known, err := c.read()
		c.mu.Lock()
		if err != nil {
			c.logger.Warn("the last catalog export could not be read from object storage; its gauge keeps the last value", "error", err)
		} else {
			c.value, c.known = value, known
		}
		c.mu.Unlock()
	}

	c.mu.Lock()
	value, known := c.value, c.known
	c.mu.Unlock()
	if known {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, value)
	}
}

// read returns the oldest of the tenants' newest exports, in Unix seconds
// (0 when a tenant has none), and false when there is no tenant.
func (c *lastSuccess) read() (float64, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), lastSuccessReadTimeout)
	defer cancel()
	at, known, err := c.freshness.OldestNewestExport(ctx)
	if err != nil || !known {
		return 0, false, err
	}
	if at.IsZero() {
		return 0, true, nil
	}
	return float64(at.UnixNano()) / 1e9, true, nil
}
