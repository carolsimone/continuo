package outbox_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/stretchr/testify/assert"
)

type fakeBacklog struct {
	table string
	b     outbox.Backlog
	err   error
}

func (f fakeBacklog) Table() string                                   { return f.table }
func (f fakeBacklog) Backlog(context.Context) (outbox.Backlog, error) { return f.b, f.err }

// The outbox series are served on the process's registry, with its service
// label: what each table published and failed to, and each watched table's
// backlog, read at scrape time. A table whose backlog cannot be read omits
// its gauges.
func TestMetrics_ServesTheOutboxSeries(t *testing.T) {
	reg := metrics.New("test-svc")
	m := outbox.NewMetrics(reg)
	var o outbox.Observer = m
	o.Published("t", 3)
	o.Failed("t", "retry")
	m.Watch(fakeBacklog{table: "t", b: outbox.Backlog{Open: 4, OldestOpenAge: 90 * time.Second, DeadLettered: 1}})
	m.Watch(fakeBacklog{table: "down", err: errors.New("connection refused")})

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`continuo_outbox_published_total{service="test-svc",table="t"} 3`,
		`continuo_outbox_publish_failures_total{kind="retry",service="test-svc",table="t"} 1`,
		`continuo_outbox_backlog{service="test-svc",table="t"} 4`,
		`continuo_outbox_oldest_open_age_seconds{service="test-svc",table="t"} 90`,
		`continuo_outbox_dead_letters{service="test-svc",table="t"} 1`,
	} {
		assert.Contains(t, body, want)
	}
	assert.NotContains(t, body, `table="down"`)
}
