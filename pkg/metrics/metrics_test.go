package metrics_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/metrics"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeBacklog struct {
	table string
	b     outbox.Backlog
}

func (f fakeBacklog) Table() string                                   { return f.table }
func (f fakeBacklog) Backlog(context.Context) (outbox.Backlog, error) { return f.b, nil }

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	srv := httptest.NewServer(h)
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(body)
}

func TestRegistry_ServesConsumerOutboxAndRuntimeSeries(t *testing.T) {
	reg := metrics.New("test-svc")
	c := reg.Consumers()
	c.Watch("s", "g")
	c.Handled("s", "g", "ok", 15*time.Millisecond)
	c.DeadLettered("s", "g", model.DeadLetterKindPermanent)
	c.Paused("s", "g")
	o := reg.Outbox()
	o.Published("t", 3)
	o.Failed("t", "retry")
	reg.WatchOutbox(fakeBacklog{table: "t", b: outbox.Backlog{Open: 4, OldestOpenAge: 90 * time.Second, DeadLettered: 1}})

	body := scrape(t, reg.Handler())
	for _, want := range []string{
		`continuo_consumer_handler_duration_seconds_count{group="g",result="ok",service="test-svc",stream="s"} 1`,
		`continuo_consumer_dead_letters_total{group="g",kind="permanent",service="test-svc",stream="s"} 1`,
		`continuo_consumer_infra_pauses_total{group="g",service="test-svc",stream="s"} 1`,
		`continuo_outbox_published_total{service="test-svc",table="t"} 3`,
		`continuo_outbox_publish_failures_total{kind="retry",service="test-svc",table="t"} 1`,
		`continuo_outbox_backlog{service="test-svc",table="t"} 4`,
		`continuo_outbox_oldest_open_age_seconds{service="test-svc",table="t"} 90`,
		`continuo_outbox_dead_letters{service="test-svc",table="t"} 1`,
		`go_goroutines{service="test-svc"}`,
	} {
		assert.Contains(t, body, want)
	}
	assert.Same(t, reg.Consumers(), reg.Consumers(), "one consumer observer per process")
}

func TestListen_ServesMetricsAndRefusesATakenPort(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := metrics.New("test-svc")
	s, err := metrics.Listen(0, reg, logger)
	require.NoError(t, err)
	go func() { _ = s.Serve() }()
	defer s.Shutdown(context.Background())

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", s.Port()))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	_, err = metrics.Listen(s.Port(), metrics.New("other"), logger)
	require.ErrorContains(t, err, "metrics port")
}
