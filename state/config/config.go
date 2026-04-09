package config

import (
	"os"
	"strconv"
)

// GetGRPCPort returns the port for the gRPC server
func GetGRPCPort() int {
	return getEnvInt("GRPC_PORT", 50051)
}

// GetHealthPort returns the port for the HTTP health check server
func GetHealthPort() string {
	return getEnv("HEALTH_PORT", "8082")
}

// GetRedisAddr returns Redis server address
func GetRedisAddr() string {
	return getEnv("REDIS_ADDR", "localhost:6379")
}

// GetRedisStreamSchedulerStarted returns the stream name for scheduler started events
func GetRedisStreamSchedulerStarted() string {
	return getEnv("REDIS_STREAM_SCHEDULER_STARTED", "scheduler.started:v1")
}

// GetSchedulesConfigPath returns the path to schedules.yaml.
// Env: SCHEDULES_CONFIG_PATH, default: /etc/continuo/schedules.yaml
func GetSchedulesConfigPath() string {
	return getEnv("SCHEDULES_CONFIG_PATH", "/etc/continuo/schedules.yaml")
}

// GetRedisStreamSchedulesLoaded returns the stream name for schedules.loaded events.
// Env: REDIS_STREAM_SCHEDULES_LOADED, default: schedules.loaded:v1
func GetRedisStreamSchedulesLoaded() string {
	return getEnv("REDIS_STREAM_SCHEDULES_LOADED", "schedules.loaded:v1")
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
