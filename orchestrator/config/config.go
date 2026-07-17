package config

import (
	"os"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

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

	// gRPC
	GRPCPort int
	HTTPPort int

	// Sweeper
	RunHistoryRetentionDays   int
	RunSweeperIntervalMinutes int

	// Reconciler — converges active :Run projections to state's terminal status.
	ReconcilerIntervalSecs int

	// Cancelled schedules consumer
	CancelledSchedulesTTLHours         int
	CancelledSchedulesSweepIntervalMin int

	// Retention sweeper — purges processed orchestrator_outbox rows and terminal
	// message_processing dedup rows older than the retention window. Both knobs
	// have safe defaults so no configuration is required.
	RetentionDays             int
	RetentionSweepIntervalMin int

	// State gRPC endpoint (host:port). Reuses the established
	// STATE_GRPC_ADDR convention exposed globally by the Helm
	// configmap (deploy/continuo/templates/configmap.yaml), so every
	// service that consumes the global configmap automatically gets
	// it populated — no per-service Helm wiring is required.
	StateGRPCAddr string

	// Dispatch watchdog — terminates schedules stuck without a RUNNING
	// task and no progress within WatchdogNoProgressMins.
	WatchdogEnabled        bool
	WatchdogIntervalSecs   int
	WatchdogNoProgressMins int

	// ShutdownGrace bounds the graceful-shutdown drain + infra teardown.
	ShutdownGrace time.Duration
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

		GRPCPort: envInt("GRPC_PORT", 50052),
		HTTPPort: envInt("HTTP_PORT", 8087),

		RunHistoryRetentionDays:   envInt("RUN_HISTORY_RETENTION_DAYS", 7),
		RunSweeperIntervalMinutes: envInt("RUN_SWEEPER_INTERVAL_MINUTES", 60),

		ReconcilerIntervalSecs: envInt("ORCHESTRATOR_RECONCILER_INTERVAL_SECONDS", 60),

		CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
		CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),

		RetentionDays:             envInt("RETENTION_DAYS", 7),
		RetentionSweepIntervalMin: envInt("RETENTION_SWEEP_INTERVAL_MINUTES", 60),

		StateGRPCAddr: v.Require("STATE_GRPC_ADDR"),

		WatchdogEnabled:        envBool("ORCHESTRATOR_WATCHDOG_ENABLED", true),
		WatchdogIntervalSecs:   envInt("ORCHESTRATOR_WATCHDOG_INTERVAL_SECONDS", 60),
		WatchdogNoProgressMins: envInt("ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES", 30),

		ShutdownGrace: pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", defaultShutdownGrace),
	}
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		return v == "true" || v == "1"
	}
	return fallback
}
