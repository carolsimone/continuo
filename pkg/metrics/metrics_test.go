package metrics_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegister_AddsServiceLabelledCollector(t *testing.T) {
	reg := metrics.New("svc")
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "continuo_test_gauge", Help: "h"})
	g.Set(3)
	reg.Register(g)
	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(rec.Body.String(), `continuo_test_gauge{service="svc"} 3`) {
		t.Fatalf("body:\n%s", rec.Body.String())
	}
}

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

func TestRegistry_ServesConsumerAndRuntimeSeries(t *testing.T) {
	reg := metrics.New("test-svc")
	c := reg.Consumers()
	c.Watch("s", "g")
	c.Handled("s", "g", "ok", 15*time.Millisecond)
	c.DeadLettered("s", "g", model.DeadLetterKindPermanent)
	c.Paused("s", "g")

	body := scrape(t, reg.Handler())
	for _, want := range []string{
		`continuo_consumer_handler_duration_seconds_count{group="g",result="ok",service="test-svc",stream="s"} 1`,
		`continuo_consumer_dead_letters_total{group="g",kind="permanent",service="test-svc",stream="s"} 1`,
		`continuo_consumer_infra_pauses_total{group="g",service="test-svc",stream="s"} 1`,
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
