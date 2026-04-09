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
	return getEnv("REDIS_CONSUMER_STREAM", "query.model:v1")
}

func GetRedisConsumerRetryStream() string {
	return getEnv("REDIS_CONSUMER_RETRY_STREAM", "task.retry:v1")
}

func GetRedisConsumerGroup() string {
	return getEnv("REDIS_CONSUMER_GROUP", "executor_controller_consumers")
}

func GetRedisProducerStream() string {
	return getEnv("REDIS_PRODUCER_STREAM", "executor.deployed:v1")
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

// HTTP configuration
func GetHTTPPort() int {
	return getEnvAsInt("HTTP_PORT", 8084)
}

// K8s configuration
func GetK8sNamespace() string {
	return getEnv("K8S_NAMESPACE", "default")
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
