package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Config holds all configuration for the executor-controller service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

	// Schedule cancellation
	CancelledSchedulesTTLHours         int
	CancelledSchedulesSweepIntervalMin int

	// HTTP
	HTTPPort int

	// K8s
	K8sNamespace string
	// Max concurrent K8s Jobs the deploy dispatcher keeps in flight.
	MaxConcurrentJobs int
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis:    pkgconfig.LoadRedis(v),
		Postgres: pkgconfig.LoadPostgres(v),

		CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
		CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),

		HTTPPort:          envInt("HTTP_PORT", 8084),
		K8sNamespace:      v.Require("K8S_NAMESPACE"),
		MaxConcurrentJobs: envInt("MAX_CONCURRENT_JOBS", 50),
	}
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
