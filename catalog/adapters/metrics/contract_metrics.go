package metrics

import (
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// ContractMetrics counts the contract versions this process records and the
// writes of the live contracts folder, and times each step of a promotion on
// the contracts group.
type ContractMetrics struct {
	versions   prometheus.Counter
	liveWrites *prometheus.CounterVec
	duration   *prometheus.HistogramVec
}

var _ ports.ContractMetrics = (*ContractMetrics)(nil)

// NewContractMetrics registers continuo_catalog_contract_versions_written_total,
// continuo_catalog_live_contract_writes_total{op,outcome} and
// continuo_catalog_contracts_duration_seconds{step} on reg, each label pair
// at 0.
func NewContractMetrics(reg *pkgmetrics.Registry) *ContractMetrics {
	m := &ContractMetrics{
		versions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "continuo_catalog_contract_versions_written_total",
			Help: "Stored contract versions the contracts group created in the graph, counted once their transaction committed.",
		}),
		liveWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "continuo_catalog_live_contract_writes_total",
			Help: "Writes of the live contracts folder, by op (put: a node's file, delete: the file of a node no longer live, index: the folder's index) and outcome (ok or failed).",
		}, []string{"op", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "continuo_catalog_contracts_duration_seconds",
			Help:    "Seconds each step of a promotion on the contracts group took, by step: ingest (read the contract bundle and record its versions) or live_rewrite (rewrite the live contracts folder).",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		}, []string{"step"}),
	}
	for _, op := range []string{ports.LiveWritePut, ports.LiveWriteDelete, ports.LiveWriteIndex} {
		for _, outcome := range []string{ports.LiveWriteOK, ports.LiveWriteFailed} {
			m.liveWrites.WithLabelValues(op, outcome)
		}
	}
	for _, step := range []string{ports.ContractStepIngest, ports.ContractStepLiveRewrite} {
		m.duration.WithLabelValues(step)
	}
	reg.Register(m.versions, m.liveWrites, m.duration)
	return m
}

// VersionsWritten adds n to the versions counter.
func (m *ContractMetrics) VersionsWritten(n int) { m.versions.Add(float64(n)) }

// LiveWrite counts one write of the live folder under op and outcome.
func (m *ContractMetrics) LiveWrite(op, outcome string) {
	m.liveWrites.WithLabelValues(op, outcome).Inc()
}

// ObserveStep records d under step.
func (m *ContractMetrics) ObserveStep(step string, d time.Duration) {
	m.duration.WithLabelValues(step).Observe(d.Seconds())
}
