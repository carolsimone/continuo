package config

import (
	"os"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

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

	// Consumer streams
	NodeUpdatedStream       string
	NodeUpdatedGroup        string
	ManifestLoadedStream    string
	ManifestLoadedGroup     string
	InitializeRunStream     string
	InitializeRunGroup      string
	SchedulerStartedStream  string
	SchedulerStartedGroup   string
	RerunStream             string
	RerunGroup              string
	RunFinalizedStream      string
	RunFinalizedGroup       string

	// gRPC
	GRPCPort int
	HTTPPort int

	// Sweeper
	RunHistoryRetentionDays   int
	RunSweeperIntervalMinutes int

	// Cancelled schedules consumer
	ScheduleCancelledStream            string
	ScheduleCancelledGroup             string
	CancelledSchedulesTTLHours         int
	CancelledSchedulesSweepIntervalMin int

	// State gRPC endpoint (host:port)
	StateGRPCEndpoint string

	// Dispatch watchdog (Phase B2)
	WatchdogEnabled        bool
	WatchdogIntervalSecs   int
	WatchdogNoProgressMins int
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

		NodeUpdatedStream:      v.Require("NODE_UPDATED_STREAM"),
		NodeUpdatedGroup:       v.Require("NODE_UPDATED_GROUP"),
		ManifestLoadedStream:   v.Require("MANIFEST_LOADED_STREAM"),
		ManifestLoadedGroup:    v.Require("MANIFEST_LOADED_GROUP"),
		InitializeRunStream:    v.Require("INITIALIZE_RUN_STREAM"),
		InitializeRunGroup:     v.Require("INITIALIZE_RUN_GROUP"),
		SchedulerStartedStream: v.Require("SCHEDULER_STARTED_STREAM"),
		SchedulerStartedGroup:  v.Require("SCHEDULER_STARTED_GROUP"),
		RerunStream:            v.Require("RERUN_STREAM"),
		RerunGroup:             v.Require("RERUN_GROUP"),
		RunFinalizedStream:     v.Require("RUN_FINALIZED_STREAM"),
		RunFinalizedGroup:      v.Require("RUN_FINALIZED_GROUP"),

		GRPCPort: envInt("GRPC_PORT", 50052),
		HTTPPort:      envInt("HTTP_PORT", 8087),

		RunHistoryRetentionDays:   envInt("RUN_HISTORY_RETENTION_DAYS", 7),
		RunSweeperIntervalMinutes: envInt("RUN_SWEEPER_INTERVAL_MINUTES", 60),

		ScheduleCancelledStream:            v.Require("SCHEDULE_CANCELLED_STREAM"),
		ScheduleCancelledGroup:             v.Require("SCHEDULE_CANCELLED_GROUP"),
		CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
		CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),

		StateGRPCEndpoint: v.Require("STATE_GRPC_ENDPOINT"),

		WatchdogEnabled:        envBool("ORCHESTRATOR_WATCHDOG_ENABLED", true),
		WatchdogIntervalSecs:   envInt("ORCHESTRATOR_WATCHDOG_INTERVAL_SECONDS", 60),
		WatchdogNoProgressMins: envInt("ORCHESTRATOR_WATCHDOG_NO_PROGRESS_MINUTES", 30),
	}
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }

func envBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		return v == "true" || v == "1"
	}
	return fallback
}
