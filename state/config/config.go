package config

import (
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// defaultShutdownGrace bounds the graceful-shutdown sequence: the in-flight
// drain plus the infra-close handlers. It is a safe default so no required env
// var is introduced; override with SHUTDOWN_GRACE (e.g. "30s").
const defaultShutdownGrace = 15 * time.Second

// Config holds all configuration for the state service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

	// gRPC
	GRPCPort int

	// HTTP health
	HealthPort string

	// Scheduler
	SchedulesConfigPath string

	// Retention sweeper — purges processed outbox rows and terminal
	// message_processing dedup rows older than the retention window. Both knobs
	// have safe defaults so no configuration is required.
	RetentionDays             int
	RetentionSweepIntervalMin int

	// ShutdownGrace bounds the graceful-shutdown drain + infra teardown.
	ShutdownGrace time.Duration
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis:    pkgconfig.LoadRedisFromAddr(v),
		Postgres: pkgconfig.LoadPostgres(v),

		GRPCPort:            envInt("GRPC_PORT", 50051),
		HealthPort:          env("HEALTH_PORT", "8082"),
		SchedulesConfigPath: env("SCHEDULES_CONFIG_PATH", "/etc/continuo/schedules.yaml"),

		RetentionDays:             envInt("RETENTION_DAYS", 7),
		RetentionSweepIntervalMin: envInt("RETENTION_SWEEP_INTERVAL_MINUTES", 60),

		ShutdownGrace: pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", defaultShutdownGrace),
	}
}

func env(key, fallback string) string     { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
