// Package config loads egress-proxy's configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Name is the service's name in logs and on every metric series. It runs no
// stream consumer, so it declares no ServiceName.
const Name = "egress-proxy"

// defaultMaxTunnels is the cap on tunnels and forwarded requests used when
// EGRESS_MAX_TUNNELS is unset.
const defaultMaxTunnels = 1024

// Config is egress-proxy's runtime configuration, read once at startup.
type Config struct {
	// ListenPort is the proxy listener (EGRESS_PROXY_LISTEN_PORT).
	ListenPort int
	// HTTPPort serves /health, /ready and /livez (EGRESS_PROXY_HEALTH_PORT).
	HTTPPort string
	// MetricsPort serves /metrics (METRICS_PORT).
	MetricsPort int
	// AllowlistPath is the YAML file of admitted hosts (EGRESS_ALLOWLIST_PATH).
	AllowlistPath string
	// MaxTunnels caps the CONNECT tunnels and forwarded requests open at
	// once, together (EGRESS_MAX_TUNNELS).
	MaxTunnels int
	// IdleTimeout ends a tunnel or forwarded request no byte has crossed for
	// this long (EGRESS_TUNNEL_IDLE_TIMEOUT).
	IdleTimeout time.Duration
	// ShutdownGrace bounds the graceful shutdown (SHUTDOWN_GRACE).
	ShutdownGrace time.Duration
}

// Load reads the configuration, recording missing and invalid values on v.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		ListenPort:    v.PortOrDefault("EGRESS_PROXY_LISTEN_PORT", 3128),
		HTTPPort:      pkgconfig.EnvOrDefault("EGRESS_PROXY_HEALTH_PORT", "8093"),
		MetricsPort:   pkgconfig.LoadMetricsPort(v),
		AllowlistPath: v.Require("EGRESS_ALLOWLIST_PATH"),
		MaxTunnels:    strictIntOrDefault(v, "EGRESS_MAX_TUNNELS", defaultMaxTunnels),
		IdleTimeout:   v.DurationOrDefault("EGRESS_TUNNEL_IDLE_TIMEOUT", 10*time.Minute),
		ShutdownGrace: pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", 15*time.Second),
	}
}

// strictIntOrDefault reads an optional integer env var. An unset or empty
// value yields fallback; a value that is not an integer is recorded on v so
// start-up fails, and fallback is returned so loading can continue.
func strictIntOrDefault(v *pkgconfig.Validator, key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		v.Add(fmt.Sprintf("%s (invalid integer %q)", key, raw))
		return fallback
	}
	return n
}

// Validate rejects limits the proxy cannot run with.
func (c Config) Validate() error {
	if c.MaxTunnels < 1 {
		return errors.New("EGRESS_MAX_TUNNELS must be at least 1")
	}
	if c.IdleTimeout <= 0 {
		return errors.New("EGRESS_TUNNEL_IDLE_TIMEOUT must be positive")
	}
	return nil
}
