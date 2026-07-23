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
	// DBT_POSTGRES_DB names the warehouse database dbt job pods materialize into; the
	// executor forwards it (with the reused POSTGRES_* connection) to those pods via
	// k8s dbtConnectionEnvVars. It is consumed there through os.Getenv, not stored on
	// Config — required here only so a missing value fails fast at boot rather than
	// silently forwarding an empty database name to every dbt Job. The executor itself
	// no longer connects to the warehouse (candidate-schema lifecycle is an engine-image
	// Job), so there is no warehouse connection to hold.
	_ = v.Require("DBT_POSTGRES_DB")

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
