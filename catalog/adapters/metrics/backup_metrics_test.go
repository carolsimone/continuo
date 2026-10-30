package metrics

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3infra "github.com/carolsimone/continuo/catalog/adapters/s3"
	"github.com/carolsimone/continuo/catalog/domain/export"
	"github.com/carolsimone/continuo/catalog/service/ports"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// fakeClock is a clock the test moves by hand.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeTenants lists the tenants the test sets, or fails with err.
type fakeTenants struct {
	mu      sync.Mutex
	tenants []string
	err     error
}

func (f *fakeTenants) Tenants(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.tenants), f.err
}

func (f *fakeTenants) set(tenants []string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tenants, f.err = tenants, err
}

// countingStore counts the List calls that reach a BackupStore.
type countingStore struct {
	ports.BackupStore
	mu    sync.Mutex
	lists int
}

func (s *countingStore) List(ctx context.Context, tenantID string) ([]ports.BackupObject, error) {
	s.mu.Lock()
	s.lists++
	s.mu.Unlock()
	return s.BackupStore.List(ctx, tenantID)
}

func (s *countingStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists
}

func storeExport(t *testing.T, store ports.BackupStore, tenantID string, at time.Time) ports.BackupObject {
	t.Helper()
	w, key, err := store.Open(context.Background(), tenantID, at)
	require.NoError(t, err)
	_, err = io.WriteString(w, "export")
	require.NoError(t, err)
	require.NoError(t, w.Close())
	listed, err := store.List(context.Background(), tenantID)
	require.NoError(t, err)
	for _, o := range listed {
		if o.Key == key {
			return o
		}
	}
	t.Fatalf("%s is not listed", key)
	return ports.BackupObject{}
}

// lastSuccessSamples returns the values the last-success gauge reports.
func lastSuccessSamples(t *testing.T, m *BackupMetrics) []float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 4)
	m.lastSuccess.Collect(ch)
	close(ch)
	var out []float64
	for metric := range ch {
		out = append(out, testutil.ToFloat64(constCollector{metric}))
	}
	return out
}

// constCollector collects one metric, so testutil can read its value.
type constCollector struct{ m prometheus.Metric }

func (c constCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.m.Desc() }
func (c constCollector) Collect(ch chan<- prometheus.Metric) { ch <- c.m }

func unix(t time.Time) float64 { return float64(t.UnixNano()) / 1e9 }

// The last-success gauge is read from the store when scraped: the newest
// export of each tenant, the oldest of those across tenants, 0 when a tenant
// has none. It lists the store at most once a minute, a new process reads
// the same value, and a store or tenant-list failure keeps the last value and
// is logged.
func TestBackupMetrics_LastSuccessComesFromTheStore(t *testing.T) {
	mem := objectstoretest.NewMemory("continuo")
	store := &countingStore{BackupStore: s3infra.NewBackupStore(mem)}
	tenants := &fakeTenants{tenants: []string{"a", "b"}}
	clock := &fakeClock{now: time.Date(2026, 10, 30, 8, 0, 0, 0, time.UTC)}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	newMetrics := func() *BackupMetrics {
		return NewBackupMetrics(pkgmetrics.New("catalog"), store, tenants, clock.Now, logger)
	}
	at := time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)

	storeExport(t, store, "a", at.Add(-24*time.Hour))
	time.Sleep(2 * time.Millisecond)
	aNewest := storeExport(t, store, "a", at)
	time.Sleep(2 * time.Millisecond)
	bNewest := storeExport(t, store, "b", at.Add(time.Minute))
	require.True(t, bNewest.LastModified.After(aNewest.LastModified))

	m := newMetrics()
	assert.Equal(t, []float64{unix(aNewest.LastModified)}, lastSuccessSamples(t, m),
		"the oldest tenant's newest export")

	before := store.count()
	clock.Advance(59 * time.Second)
	assert.Equal(t, []float64{unix(aNewest.LastModified)}, lastSuccessSamples(t, m))
	assert.Equal(t, before, store.count(), "a second scrape inside a minute lists nothing")

	time.Sleep(2 * time.Millisecond)
	aNewer := storeExport(t, store, "a", at.Add(2*time.Minute))
	clock.Advance(2 * time.Second)
	assert.Equal(t, []float64{unix(bNewest.LastModified)}, lastSuccessSamples(t, m),
		"after a minute the store is read again; b is now the oldest")
	assert.Greater(t, store.count(), before)

	again := newMetrics()
	assert.Equal(t, []float64{unix(bNewest.LastModified)}, lastSuccessSamples(t, again),
		"a new process reads the same value from the store")
	require.True(t, aNewer.LastModified.After(bNewest.LastModified))

	tenants.set([]string{"a", "b", "c"}, nil)
	clock.Advance(time.Minute)
	assert.Equal(t, []float64{0}, lastSuccessSamples(t, m), "a tenant with no export reads 0")

	tenants.set([]string{"a", "b"}, nil)
	clock.Advance(time.Minute)
	require.Equal(t, []float64{unix(bNewest.LastModified)}, lastSuccessSamples(t, m))
	prefix, _ := export.ObjectPrefix("b")
	mem.FailList(prefix, errors.New("SlowDown"))
	clock.Advance(time.Minute)
	assert.Equal(t, []float64{unix(bNewest.LastModified)}, lastSuccessSamples(t, m), "a store failure keeps the last value")
	assert.Contains(t, logs.String(), "SlowDown")

	tenants.set(nil, errors.New("neo4j: connection refused"))
	clock.Advance(time.Minute)
	assert.Equal(t, []float64{unix(bNewest.LastModified)}, lastSuccessSamples(t, m), "a tenant-list failure keeps the last value")
	assert.Contains(t, logs.String(), "connection refused")
}

// With no tenant at all there is no export to be late, and no sample; the
// first read failing reports nothing either.
func TestBackupMetrics_LastSuccessHasNoSampleWithoutTenants(t *testing.T) {
	store := s3infra.NewBackupStore(objectstoretest.NewMemory("continuo"))
	clock := &fakeClock{now: time.Now()}
	m := NewBackupMetrics(pkgmetrics.New("catalog"), store, &fakeTenants{}, clock.Now, slog.New(slog.NewTextHandler(io.Discard, nil)))
	assert.Empty(t, lastSuccessSamples(t, m))

	failing := NewBackupMetrics(pkgmetrics.New("catalog"), store, &fakeTenants{err: errors.New("down")}, clock.Now, slog.New(slog.NewTextHandler(io.Discard, nil)))
	assert.Empty(t, lastSuccessSamples(t, failing))
}

// Each export counts under its outcome; a success also sets the duration,
// size and record series, which describe the last stored export.
func TestBackupMetrics_CountsEachExportByOutcome(t *testing.T) {
	reg := pkgmetrics.New("catalog")
	store := s3infra.NewBackupStore(objectstoretest.NewMemory("continuo"))
	m := NewBackupMetrics(reg, store, &fakeTenants{}, time.Now, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, outcome := range []string{ports.ExportSuccess, ports.ExportFailure, ports.ExportSkipped} {
		assert.Zero(t, testutil.ToFloat64(m.exports.WithLabelValues(outcome)), "%s starts at 0", outcome)
	}

	m.ExportObserved(ports.ExportSuccess, 90*time.Second, 2048, 10, 9)
	m.ExportObserved(ports.ExportFailure, 0, 0, 0, 0)
	m.ExportObserved(ports.ExportSkipped, 0, 0, 0, 0)
	m.ExportObserved(ports.ExportSuccess, 30*time.Second, 4096, 12, 11)
	m.ExportObserved(ports.ExportFailure, 0, 0, 0, 0)

	assert.Equal(t, 2.0, testutil.ToFloat64(m.exports.WithLabelValues(ports.ExportSuccess)))
	assert.Equal(t, 2.0, testutil.ToFloat64(m.exports.WithLabelValues(ports.ExportFailure)))
	assert.Equal(t, 1.0, testutil.ToFloat64(m.exports.WithLabelValues(ports.ExportSkipped)))
	assert.Equal(t, 4096.0, testutil.ToFloat64(m.bytes), "the last stored export's size")
	assert.Equal(t, 12.0, testutil.ToFloat64(m.records.WithLabelValues("node")))
	assert.Equal(t, 11.0, testutil.ToFloat64(m.records.WithLabelValues("relationship")))

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`continuo_catalog_exports_total{outcome="success",service="catalog"} 2`,
		`continuo_catalog_exports_total{outcome="failure",service="catalog"} 2`,
		`continuo_catalog_exports_total{outcome="skipped",service="catalog"} 1`,
		`continuo_catalog_export_duration_seconds_count{service="catalog"} 2`,
		`continuo_catalog_export_duration_seconds_sum{service="catalog"} 120`,
		`continuo_catalog_export_bytes{service="catalog"} 4096`,
		`continuo_catalog_export_records{kind="node",service="catalog"} 12`,
		`continuo_catalog_export_records{kind="relationship",service="catalog"} 11`,
	} {
		assert.True(t, strings.Contains(body, want), "the scrape lacks %q", want)
	}
}

// The last-success gauge is served under its name with the service label.
func TestBackupMetrics_LastSuccessIsScraped(t *testing.T) {
	reg := pkgmetrics.New("catalog")
	store := s3infra.NewBackupStore(objectstoretest.NewMemory("continuo"))
	o := storeExport(t, store, "default", time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC))
	NewBackupMetrics(reg, store, &fakeTenants{tenants: []string{"default"}}, time.Now, slog.New(slog.NewTextHandler(io.Discard, nil)))

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Contains(t, rec.Body.String(), `continuo_catalog_export_last_success_timestamp_seconds{service="catalog"} `)
	assert.Contains(t, rec.Body.String(), "# TYPE continuo_catalog_export_last_success_timestamp_seconds gauge")
	assert.NotZero(t, o.LastModified)
}
