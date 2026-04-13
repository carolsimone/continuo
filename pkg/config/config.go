package config

import (
	"math"
	"os"
	"strconv"
	"time"
)

const (
	K8sCommandsStream = "k8s:commands"
)

// Backoff strategies
const (
	BackoffStrategyFixed       = "fixed"
	BackoffStrategyExponential = "exponential"
)

// Polling configuration
const (
	MaxPollAttempts        = 10
	PollingIntervalSeconds = 10
)

func GetEnvOrDefault(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func GetEnvIntOrDefault(key string, fallback int) int {
	if value, exists := os.LookupEnv(key); exists {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return fallback
}

// GetRedisHost returns the Redis host from environment or default
func GetRedisHost() string {
	return getEnv("REDIS_HOST", "localhost")
}

// GetRedisPort returns the Redis port from environment or default
func GetRedisPort() int {
	return getEnvInt("REDIS_PORT", 6379)
}

// GetRedisPassword returns the Redis password from environment or empty string
func GetRedisPassword() string {
	return getEnv("REDIS_PASSWORD", "")
}

// GetPostgresHost returns the PostgreSQL host from environment or default
func GetPostgresHost() string {
	return getEnv("POSTGRES_HOST", "localhost")
}

// GetPostgresPort returns the PostgreSQL port from environment or default
func GetPostgresPort() int {
	return getEnvInt("POSTGRES_PORT", 5432)
}

// GetPostgresUser returns the PostgreSQL user from environment or default
func GetPostgresUser() string {
	return getEnv("POSTGRES_USER", "continuo_svc")
}

// GetPostgresPassword returns the PostgreSQL password from environment or default
func GetPostgresPassword() string {
	return getEnv("POSTGRES_PASSWORD", "runner")
}

// GetPostgresDB returns the PostgreSQL database name from environment or default
func GetPostgresDB() string {
	return getEnv("POSTGRES_DB", "runner")
}

// GetDBPoolSize returns the database connection pool size from environment or default
func GetDBPoolSize() int {
	return getEnvInt("DB_POOL_SIZE", 10)
}

// GetDBMaxOpenConns returns the maximum number of open connections from environment or default
func GetDBMaxOpenConns() int {
	return getEnvInt("DB_MAX_OPEN_CONNS", 20)
}

// CalculateNextCheckTime calculates the next check time based on backoff strategy
// Returns Unix timestamp as float64 (seconds since epoch)
func CalculateNextCheckTime(pollAttempts int, strategy string) float64 {
	now := float64(time.Now().Unix())

	switch strategy {
	case BackoffStrategyExponential:
		// 2^pollAttempts
		multiplier := math.Pow(2, float64(pollAttempts))
		return now + (PollingIntervalSeconds * multiplier)
	default:
		// Fixed backoff
		return now + PollingIntervalSeconds
	}
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt retrieves an environment variable as integer or returns a default value
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}
