package config

import (
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

	// gRPC
	GRPCPort int
	HTTPPort int

	// Sweeper
	RunHistoryRetentionDays   int
	RunSweeperIntervalMinutes int
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

		GRPCPort: envInt("GRPC_PORT", 50052),
		HTTPPort:      envInt("HTTP_PORT", 8087),

		RunHistoryRetentionDays:   envInt("RUN_HISTORY_RETENTION_DAYS", 7),
		RunSweeperIntervalMinutes: envInt("RUN_SWEEPER_INTERVAL_MINUTES", 60),
	}
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
