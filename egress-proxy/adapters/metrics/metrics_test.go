package metrics

import (
	"io"
	"net/http/httptest"
	"testing"

	pkgmetrics "github.com/carolsimone/continuo/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics_RecordsDecisionsBytesAndTunnels(t *testing.T) {
	reg := pkgmetrics.New("egress-proxy")
	m := New(reg, []string{"ok", "not_allowed"})
	m.Request(true, "ok")
	m.Request(false, "not_allowed")
	m.Request(false, "not_allowed")
	m.Transferred("up", 10)
	m.Transferred("down", 32)
	m.TunnelOpened()
	m.TunnelOpened()
	m.TunnelClosed()

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	text := string(body)
	assert.Contains(t, text, `continuo_egress_requests_total{component="egress-proxy",decision="allowed",reason="ok"} 1`)
	assert.Contains(t, text, `continuo_egress_requests_total{component="egress-proxy",decision="denied",reason="not_allowed"} 2`)
	assert.Contains(t, text, `continuo_egress_bytes_total{component="egress-proxy",direction="up"} 10`)
	assert.Contains(t, text, `continuo_egress_bytes_total{component="egress-proxy",direction="down"} 32`)
	assert.Contains(t, text, `continuo_egress_active_tunnels{component="egress-proxy"} 1`)
}

// A fresh registry lists every decision/reason pair and both directions at 0
// before any traffic.
func TestMetrics_ExportsSeriesAtZeroBeforeTraffic(t *testing.T) {
	reg := pkgmetrics.New("egress-proxy")
	New(reg, []string{"ok", "capacity"})

	rec := httptest.NewRecorder()
	reg.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	text := rec.Body.String()
	for _, want := range []string{
		`continuo_egress_requests_total{component="egress-proxy",decision="allowed",reason="ok"} 0`,
		`continuo_egress_requests_total{component="egress-proxy",decision="allowed",reason="capacity"} 0`,
		`continuo_egress_requests_total{component="egress-proxy",decision="denied",reason="ok"} 0`,
		`continuo_egress_requests_total{component="egress-proxy",decision="denied",reason="capacity"} 0`,
		`continuo_egress_bytes_total{component="egress-proxy",direction="up"} 0`,
		`continuo_egress_bytes_total{component="egress-proxy",direction="down"} 0`,
		`continuo_egress_active_tunnels{component="egress-proxy"} 0`,
	} {
		assert.Contains(t, text, want)
	}
}
