package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/catalog/service/ports"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
)

// scrape returns the text exposition of reg.
func scrape(reg *pkgmetrics.Registry) string {
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}

// Every label pair of the three contract series is backed up at 0 from boot,
// so a rate or an absent() alert works before the first promotion.
func TestContractMetrics_EveryLabelPairStartsAtZero(t *testing.T) {
	reg := pkgmetrics.New("catalog")
	NewContractMetrics(reg)

	body := scrape(reg)
	for _, want := range []string{
		`continuo_catalog_contract_versions_written_total{component="catalog"} 0`,
		`continuo_catalog_live_contract_writes_total{component="catalog",op="put",outcome="ok"} 0`,
		`continuo_catalog_live_contract_writes_total{component="catalog",op="put",outcome="failed"} 0`,
		`continuo_catalog_live_contract_writes_total{component="catalog",op="delete",outcome="ok"} 0`,
		`continuo_catalog_live_contract_writes_total{component="catalog",op="delete",outcome="failed"} 0`,
		`continuo_catalog_live_contract_writes_total{component="catalog",op="index",outcome="ok"} 0`,
		`continuo_catalog_live_contract_writes_total{component="catalog",op="index",outcome="failed"} 0`,
		`continuo_catalog_contracts_duration_seconds_count{component="catalog",step="ingest"} 0`,
		`continuo_catalog_contracts_duration_seconds_count{component="catalog",step="live_rewrite"} 0`,
		"# TYPE continuo_catalog_contract_versions_written_total counter",
		"# TYPE continuo_catalog_live_contract_writes_total counter",
		"# TYPE continuo_catalog_contracts_duration_seconds histogram",
	} {
		assert.Contains(t, body, want)
	}
}

// Each observation moves its own series.
func TestContractMetrics_CountsWhatItIsTold(t *testing.T) {
	reg := pkgmetrics.New("catalog")
	m := NewContractMetrics(reg)

	m.VersionsWritten(3)
	m.VersionsWritten(0)
	m.VersionsWritten(2)
	m.LiveWrite(ports.LiveWritePut, ports.LiveWriteOK)
	m.LiveWrite(ports.LiveWritePut, ports.LiveWriteOK)
	m.LiveWrite(ports.LiveWriteDelete, ports.LiveWriteFailed)
	m.LiveWrite(ports.LiveWriteIndex, ports.LiveWriteOK)
	m.ObserveStep(ports.ContractStepIngest, 1500*time.Millisecond)
	m.ObserveStep(ports.ContractStepLiveRewrite, 250*time.Millisecond)
	m.ObserveStep(ports.ContractStepLiveRewrite, 750*time.Millisecond)

	body := scrape(reg)
	for _, want := range []string{
		`continuo_catalog_contract_versions_written_total{component="catalog"} 5`,
		`continuo_catalog_live_contract_writes_total{component="catalog",op="put",outcome="ok"} 2`,
		`continuo_catalog_live_contract_writes_total{component="catalog",op="delete",outcome="failed"} 1`,
		`continuo_catalog_live_contract_writes_total{component="catalog",op="delete",outcome="ok"} 0`,
		`continuo_catalog_live_contract_writes_total{component="catalog",op="index",outcome="ok"} 1`,
		`continuo_catalog_contracts_duration_seconds_count{component="catalog",step="ingest"} 1`,
		`continuo_catalog_contracts_duration_seconds_sum{component="catalog",step="ingest"} 1.5`,
		`continuo_catalog_contracts_duration_seconds_count{component="catalog",step="live_rewrite"} 2`,
		`continuo_catalog_contracts_duration_seconds_sum{component="catalog",step="live_rewrite"} 1`,
	} {
		assert.Contains(t, body, want)
	}
}

// The port's label values are the ones the series are documented with.
func TestContractMetrics_LabelValues(t *testing.T) {
	assert.Equal(t, []string{"put", "delete", "index"}, []string{ports.LiveWritePut, ports.LiveWriteDelete, ports.LiveWriteIndex})
	assert.Equal(t, []string{"ok", "failed"}, []string{ports.LiveWriteOK, ports.LiveWriteFailed})
	assert.Equal(t, []string{"ingest", "live_rewrite"}, []string{ports.ContractStepIngest, ports.ContractStepLiveRewrite})
}
