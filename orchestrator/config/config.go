package config

import (
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// ServiceName names this service as the producer of the dead letters its
// stream consumers write.
const ServiceName = "catalog"

// defaultShutdownGrace bounds the graceful-shutdown sequence: the in-flight
// drain plus the infra-close handlers. It is a safe default so no required env
// var is introduced; override with SHUTDOWN_GRACE (e.g. "30s").
const defaultShutdownGrace = 15 * time.Second

// Neo4jConfig holds Neo4j connection parameters.
type Neo4jConfig struct {
	URI      string
	User     string
	Password string
}

// Config holds all configuration for the orchestrator service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig
	Neo4j    Neo4jConfig

	// S3 — the version-ingestion consumer reads each release's code-bundle
	// document from object storage. Required: an orchestrator that boots without
	// it would silently stop recording code history.
	S3 pkgconfig.S3Config

	// gRPC
	GRPCPort int
	HTTPPort int

	// Sweeper
	RunHistoryRetentionDays   int
	RunSweeperIntervalMinutes int

	// Retention sweeper — purges message_processing dedup rows, in any state,
	// older than the longer of RetentionDays and the 30-day replay horizon. Both
	// knobs have safe defaults so no configuration is required.
	RetentionDays             int
	RetentionSweepIntervalMin int

	// ShutdownGrace bounds the graceful-shutdown drain + infra teardown.
	ShutdownGrace time.Duration

	// MetricsPort is the port the Prometheus /metrics listener binds (METRICS_PORT).
	MetricsPort int
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis:    pkgconfig.LoadRedis(v),
		Postgres: pkgconfig.LoadPostgres(v),
		Neo4j: Neo4jConfig{
			URI:      v.Require("NEO4J_URI"),
			User:     v.Require("NEO4J_USER"),
			Password: v.Require("NEO4J_PASSWORD"),
		},
		S3: pkgconfig.LoadS3(v),

		GRPCPort: envInt("GRPC_PORT", 50052),
		HTTPPort: envInt("HTTP_PORT", 8087),

		RunHistoryRetentionDays:   envInt("RUN_HISTORY_RETENTION_DAYS", 7),
		RunSweeperIntervalMinutes: envInt("RUN_SWEEPER_INTERVAL_MINUTES", 60),

		RetentionDays:             envInt("RETENTION_DAYS", 7),
		RetentionSweepIntervalMin: envInt("RETENTION_SWEEP_INTERVAL_MINUTES", 60),

		ShutdownGrace: pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", defaultShutdownGrace),

		MetricsPort: pkgconfig.LoadMetricsPort(v),
	}
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
