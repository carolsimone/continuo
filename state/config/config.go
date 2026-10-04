package config

import (
	"os"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// ServiceName names this service as the producer of the dead letters its
// stream consumers write.
const ServiceName = "state"

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

	// IgnoredPoolKeys names DB_POOL_SIZE and DB_MAX_OVERFLOW when they are set.
	// state does not read them: its pool limits come from DB_MAX_OPEN_CONNS
	// and DB_MAX_IDLE_CONNS.
	IgnoredPoolKeys []string

	// MetricsPort is the port the Prometheus /metrics listener binds (METRICS_PORT).
	MetricsPort int
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

		IgnoredPoolKeys: setKeys("DB_POOL_SIZE", "DB_MAX_OVERFLOW"),

		MetricsPort: pkgconfig.LoadMetricsPort(v),
	}
}

// setKeys returns the keys among keys that are set to a non-empty value.
func setKeys(keys ...string) []string {
	var set []string
	for _, key := range keys {
		if os.Getenv(key) != "" {
			set = append(set, key)
		}
	}
	return set
}

func env(key, fallback string) string     { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
