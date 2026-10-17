package config

import (
	"strings"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("EGRESS_ALLOWLIST_PATH", "/etc/egress-proxy/allowlist.yaml")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	require.NoError(t, cfg.Validate())
	assert.Equal(t, 3128, cfg.ListenPort)
	assert.Equal(t, "8093", cfg.HTTPPort)
	assert.Equal(t, pkgconfig.DefaultMetricsPort, cfg.MetricsPort)
	assert.Equal(t, "/etc/egress-proxy/allowlist.yaml", cfg.AllowlistPath)
	assert.Equal(t, 1024, cfg.MaxTunnels)
	assert.Equal(t, 10*time.Minute, cfg.IdleTimeout)
	assert.Equal(t, 15*time.Second, cfg.ShutdownGrace)
}

func TestLoad_RequiresAllowlistPath(t *testing.T) {
	t.Setenv("EGRESS_ALLOWLIST_PATH", "")
	v := &pkgconfig.Validator{}
	Load(v)
	assert.Contains(t, v.Missing(), "EGRESS_ALLOWLIST_PATH")
}

func TestLoad_ReportsInvalidValues(t *testing.T) {
	t.Setenv("EGRESS_ALLOWLIST_PATH", "/etc/egress-proxy/allowlist.yaml")
	t.Setenv("EGRESS_PROXY_LISTEN_PORT", "70000")
	t.Setenv("EGRESS_TUNNEL_IDLE_TIMEOUT", "ten minutes")
	t.Setenv("EGRESS_MAX_TUNNELS", "many")
	v := &pkgconfig.Validator{}
	Load(v)
	missing := strings.Join(v.Missing(), ",")
	assert.Contains(t, missing, "EGRESS_PROXY_LISTEN_PORT")
	assert.Contains(t, missing, "EGRESS_TUNNEL_IDLE_TIMEOUT")
	assert.Contains(t, missing, "EGRESS_MAX_TUNNELS")
}

func TestLoad_ReadsMaxTunnels(t *testing.T) {
	t.Setenv("EGRESS_ALLOWLIST_PATH", "/etc/egress-proxy/allowlist.yaml")
	t.Setenv("EGRESS_MAX_TUNNELS", "64")
	v := &pkgconfig.Validator{}
	cfg := Load(v)
	require.Empty(t, v.Missing())
	assert.Equal(t, 64, cfg.MaxTunnels)
}

func TestLoad_NonPositiveMaxTunnelsFailsValidate(t *testing.T) {
	for _, raw := range []string{"0", "-3"} {
		t.Run(raw, func(t *testing.T) {
			t.Setenv("EGRESS_ALLOWLIST_PATH", "/etc/egress-proxy/allowlist.yaml")
			t.Setenv("EGRESS_MAX_TUNNELS", raw)
			v := &pkgconfig.Validator{}
			cfg := Load(v)
			require.Empty(t, v.Missing())
			require.Error(t, cfg.Validate())
		})
	}
}

func TestValidate_RejectsNonPositiveLimits(t *testing.T) {
	require.Error(t, Config{MaxTunnels: 0, IdleTimeout: time.Minute}.Validate())
	require.Error(t, Config{MaxTunnels: 1, IdleTimeout: 0}.Validate())
	require.NoError(t, Config{MaxTunnels: 1, IdleTimeout: time.Second}.Validate())
}
