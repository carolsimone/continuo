package config

import (
	"os"
	"strconv"
)

// GetGRPCPort returns the port for the gRPC server
func GetGRPCPort() int {
	return getEnvInt("GRPC_PORT", 50052)
}

// GetHealthPort returns the port for the HTTP health check server
func GetHealthPort() string {
	return getEnv("HEALTH_PORT", "8081")
}

// GetNeo4jURI returns the Neo4j connection URI
func GetNeo4jURI() string {
	return getEnv("NEO4J_URI", "bolt://localhost:7687")
}

// GetNeo4jUser returns the Neo4j username
func GetNeo4jUser() string {
	return getEnv("NEO4J_USER", "neo4j")
}

// GetNeo4jPassword returns the Neo4j password
func GetNeo4jPassword() string {
	return getEnv("NEO4J_PASSWORD", "atlas_password")
}

// GetLogLevel returns the log level
func GetLogLevel() string {
	return getEnv("LOG_LEVEL", "INFO")
}

// GetEnv returns the environment (local, dev, prod)
func GetEnv() string {
	return getEnv("ENV", "local")
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt retrieves an integer environment variable or returns a default value
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

// GetRunHistoryRetentionDays returns how many days completed Run nodes are
// kept in Neo4j before the sweeper deletes them. Default: 7.
// Override: RUN_HISTORY_RETENTION_DAYS env var.
func GetRunHistoryRetentionDays() int {
	return getEnvInt("RUN_HISTORY_RETENTION_DAYS", 7)
}

// GetRunSweeperIntervalMinutes returns how often the sweeper checks for
// expired Run nodes. Default: 60 (hourly).
// Override: RUN_SWEEPER_INTERVAL_MINUTES env var.
func GetRunSweeperIntervalMinutes() int {
	return getEnvInt("RUN_SWEEPER_INTERVAL_MINUTES", 60)
}
