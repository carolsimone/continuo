package config

import (
	"os"
	"strconv"
)

// Redis configuration
func GetRedisHost() string {
	return getEnv("REDIS_HOST", "localhost")
}

func GetRedisPort() int {
	return getEnvAsInt("REDIS_PORT", 6379)
}

func GetRedisConsumerStream() string {
	return getEnv("REDIS_CONSUMER_STREAM", "scheduler.started:v1")
}

func GetRedisConsumerGroup() string {
	return getEnv("REDIS_CONSUMER_GROUP", "startup_controller_consumers")
}

func GetRedisProducerStream() string {
	return getEnv("REDIS_PRODUCER_STREAM", "query.model:v1")
}

// PostgreSQL configuration
func GetPostgresHost() string {
	return getEnv("POSTGRES_HOST", "localhost")
}

func GetPostgresPort() int {
	return getEnvAsInt("POSTGRES_PORT", 5432)
}

func GetPostgresDB() string {
	return getEnv("POSTGRES_DB", "continuo")
}

func GetPostgresUser() string {
	return getEnv("POSTGRES_USER", "postgres")
}

func GetPostgresPassword() string {
	return getEnv("POSTGRES_PASSWORD", "password")
}

// gRPC configuration
func GetStateServiceGRPCAddr() string {
	return getEnv("STATE_SERVICE_GRPC_ADDR", "localhost:50051")
}

// GetGraphServiceGRPCAddr returns the graph service gRPC address.
func GetGraphServiceGRPCAddr() string {
	return getEnv("GRAPH_SERVICE_GRPC_ADDR", "graph:50052")
}

// HTTP configuration
func GetHTTPPort() int {
	return getEnvAsInt("HTTP_PORT", 8083)
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}
