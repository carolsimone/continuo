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
	assert.Contains(t, text, `continuo_egress_requests_total{decision="allowed",reason="ok",service="egress-proxy"} 1`)
	assert.Contains(t, text, `continuo_egress_requests_total{decision="denied",reason="not_allowed",service="egress-proxy"} 2`)
	assert.Contains(t, text, `continuo_egress_bytes_total{direction="up",service="egress-proxy"} 10`)
	assert.Contains(t, text, `continuo_egress_bytes_total{direction="down",service="egress-proxy"} 32`)
	assert.Contains(t, text, `continuo_egress_active_tunnels{service="egress-proxy"} 1`)
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
		`continuo_egress_requests_total{decision="allowed",reason="ok",service="egress-proxy"} 0`,
		`continuo_egress_requests_total{decision="allowed",reason="capacity",service="egress-proxy"} 0`,
		`continuo_egress_requests_total{decision="denied",reason="ok",service="egress-proxy"} 0`,
		`continuo_egress_requests_total{decision="denied",reason="capacity",service="egress-proxy"} 0`,
		`continuo_egress_bytes_total{direction="up",service="egress-proxy"} 0`,
		`continuo_egress_bytes_total{direction="down",service="egress-proxy"} 0`,
		`continuo_egress_active_tunnels{service="egress-proxy"} 0`,
	} {
		assert.Contains(t, text, want)
	}
}
