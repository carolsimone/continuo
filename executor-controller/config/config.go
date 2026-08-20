package config

import (
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// defaultShutdownGrace bounds the graceful-shutdown sequence: the in-flight
// drain plus the infra-close handlers. It is a safe default so no required env
// var is introduced; override with SHUTDOWN_GRACE (e.g. "30s").
const defaultShutdownGrace = 15 * time.Second

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

	// ShutdownGrace bounds the graceful-shutdown drain + infra teardown.
	ShutdownGrace time.Duration
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	// Every Job this service creates (validation, schema-op, compile, seed, run)
	// attaches the warehouse Secret named by VALIDATION_WAREHOUSE_SECRET via
	// envFrom. It is consumed in the k8s adapter through os.Getenv, not stored on
	// Config — required here only so a missing name fails fast at boot rather
	// than permanently failing every Job at dispatch time.
	_ = v.Require("VALIDATION_WAREHOUSE_SECRET")

	return Config{
		Redis:    pkgconfig.LoadRedis(v),
		Postgres: pkgconfig.LoadPostgres(v),

		CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
		CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),

		HTTPPort:          envInt("HTTP_PORT", 8084),
		K8sNamespace:      v.Require("K8S_NAMESPACE"),
		MaxConcurrentJobs: envInt("MAX_CONCURRENT_JOBS", 50),

		ShutdownGrace: pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", defaultShutdownGrace),
	}
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
