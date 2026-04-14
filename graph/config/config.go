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

// Config holds all configuration for the graph service.
type Config struct {
	Neo4j Neo4jConfig

	GRPCPort   int
	HealthPort string
	LogLevel   string
	Env        string

	RunHistoryRetentionDays   int
	RunSweeperIntervalMinutes int
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Neo4j: Neo4jConfig{
			URI:      v.Require("NEO4J_URI"),
			User:     v.Require("NEO4J_USER"),
			Password: v.Require("NEO4J_PASSWORD"),
		},

		GRPCPort:   envInt("GRPC_PORT", 50052),
		HealthPort: env("HEALTH_PORT", "8081"),
		LogLevel:   env("LOG_LEVEL", "INFO"),
		Env:        env("ENV", "local"),

		RunHistoryRetentionDays:   envInt("RUN_HISTORY_RETENTION_DAYS", 7),
		RunSweeperIntervalMinutes: envInt("RUN_SWEEPER_INTERVAL_MINUTES", 60),
	}
}

func env(key, fallback string) string    { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
