package metrics

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/service/ports"
	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
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

// fakeFreshness answers with the freshness the test sets, or fails with err,
// and counts the reads. When block is set, a read waits until it is closed
// (entered closes when the first read arrives, and deadline records how long
// that read's context gave it).
type fakeFreshness struct {
	mu    sync.Mutex
	at    time.Time
	known bool
	err   error
	reads int

	block    chan struct{}
	entered  chan struct{}
	once     sync.Once
	deadline time.Duration
}

var _ ports.ExportFreshness = (*fakeFreshness)(nil)

func (f *fakeFreshness) OldestNewestExport(ctx context.Context) (time.Time, bool, error) {
	f.mu.Lock()
	f.reads++
	f.mu.Unlock()
	if f.block != nil {
		f.once.Do(func() {
			if d, ok := ctx.Deadline(); ok {
				f.deadline = time.Until(d)
			}
			close(f.entered)
		})
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.at, f.known, f.err
}

func (f *fakeFreshness) set(at time.Time, known bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.at, f.known, f.err = at, known, err
}

func (f *fakeFreshness) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

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

// The last-success gauge reports the freshness in Unix seconds, fractional,
// and 0 when a tenant has no export. It reads the freshness at most once a
// minute, and a failed read keeps the last value and is logged.
func TestBackupMetrics_LastSuccessReportsTheFreshness(t *testing.T) {
	freshness := &fakeFreshness{}
	clock := &fakeClock{now: time.Date(2026, 10, 30, 8, 0, 0, 0, time.UTC)}
	var logs bytes.Buffer
	m := NewBackupMetrics(pkgmetrics.New("catalog"), freshness, clock.Now, slog.New(slog.NewTextHandler(&logs, nil)))

	newest := time.Date(2026, 10, 30, 2, 0, 0, 500_000_000, time.UTC)
	freshness.set(newest, true, nil)
	assert.Equal(t, []float64{unix(newest)}, lastSuccessSamples(t, m), "the freshness, with its fraction of a second")
	assert.InDelta(t, 0.5, unix(newest)-float64(newest.Unix()), 1e-6)

	before := freshness.count()
	clock.Advance(59 * time.Second)
	freshness.set(newest.Add(time.Hour), true, nil)
	assert.Equal(t, []float64{unix(newest)}, lastSuccessSamples(t, m), "inside a minute the cached value serves")
	assert.Equal(t, before, freshness.count(), "a second scrape inside a minute reads no freshness")

	clock.Advance(2 * time.Second)
	assert.Equal(t, []float64{unix(newest.Add(time.Hour))}, lastSuccessSamples(t, m), "after a minute it reads again")

	clock.Advance(time.Minute)
	freshness.set(time.Time{}, true, nil)
	assert.Equal(t, []float64{0}, lastSuccessSamples(t, m), "a tenant with no export reads 0")

	clock.Advance(time.Minute)
	freshness.set(newest, true, nil)
	require.Equal(t, []float64{unix(newest)}, lastSuccessSamples(t, m))

	clock.Advance(time.Minute)
	freshness.set(time.Time{}, false, errors.New("SlowDown"))
	assert.Equal(t, []float64{unix(newest)}, lastSuccessSamples(t, m), "a failed read keeps the last value")
	assert.Contains(t, logs.String(), "SlowDown")
}

// A scrape that comes while another one reads serves the last value at once
// instead of waiting on the read, and the read is bounded well under
// Prometheus's 10 s scrape timeout.
func TestBackupMetrics_AScrapeDoesNotWaitOnAnotherOnesRead(t *testing.T) {
	freshness := &fakeFreshness{block: make(chan struct{}), entered: make(chan struct{}), known: true}
	clock := &fakeClock{now: time.Date(2026, 10, 30, 8, 0, 0, 0, time.UTC)}
	m := NewBackupMetrics(pkgmetrics.New("catalog"), freshness, clock.Now, quietLogger())

	first := make(chan []float64, 1)
	go func() { first <- lastSuccessSamples(t, m) }()
	select {
	case <-freshness.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the first scrape did not read the freshness")
	}

	second := make(chan []float64, 1)
	go func() { second <- lastSuccessSamples(t, m) }()
	select {
	case got := <-second:
		assert.Empty(t, got, "nothing was read yet, so the second scrape has no sample")
	case <-time.After(2 * time.Second):
		t.Fatal("a second scrape waited on the first one's read")
	}

	close(freshness.block)
	assert.Equal(t, []float64{0}, <-first, "the first scrape reports what it read: a tenant with no export")
	assert.Positive(t, freshness.deadline)
	assert.LessOrEqual(t, freshness.deadline, 5*time.Second, "the read is bounded at 5 s")
	assert.Equal(t, 5*time.Second, lastSuccessReadTimeout)
}

// With no tenant at all there is no export to be late, and no sample; the
// first read failing reports nothing either.
func TestBackupMetrics_LastSuccessHasNoSampleWithoutTenants(t *testing.T) {
	clock := &fakeClock{now: time.Now()}
	m := NewBackupMetrics(pkgmetrics.New("catalog"), &fakeFreshness{}, clock.Now, quietLogger())
	assert.Empty(t, lastSuccessSamples(t, m))

	failing := NewBackupMetrics(pkgmetrics.New("catalog"), &fakeFreshness{err: errors.New("down")}, clock.Now, quietLogger())
	assert.Empty(t, lastSuccessSamples(t, failing))
}

// Each export counts under its outcome; a success also sets the duration,
// size and record series, which describe the last stored export.
func TestBackupMetrics_CountsEachExportByOutcome(t *testing.T) {
	reg := pkgmetrics.New("catalog")
	m := NewBackupMetrics(reg, &fakeFreshness{}, time.Now, quietLogger())
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
	at := time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC)
	NewBackupMetrics(reg, &fakeFreshness{at: at, known: true}, time.Now, quietLogger())

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Contains(t, rec.Body.String(), `continuo_catalog_export_last_success_timestamp_seconds{service="catalog"} 1.7933256e+09`)
	assert.Contains(t, rec.Body.String(), "# TYPE continuo_catalog_export_last_success_timestamp_seconds gauge")
}
