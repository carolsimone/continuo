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

func GetRedisConsumerDeployedStream() string {
	return getEnv("REDIS_CONSUMER_DEPLOYED_STREAM", "executor.deployed:v1")
}

func GetRedisConsumerCheckStream() string {
	return getEnv("REDIS_CONSUMER_CHECK_STREAM", "k8s.check:v1")
}

func GetRedisConsumerGroup() string {
	return getEnv("REDIS_CONSUMER_GROUP", "k8s_controller_consumers")
}

func GetRedisProducerCheckStream() string {
	return getEnv("REDIS_PRODUCER_CHECK_STREAM", "k8s.check:v1")
}

func GetRedisProducerRetryStream() string {
	return getEnv("REDIS_PRODUCER_RETRY_STREAM", "task.retry:v1")
}

func GetRedisProducerFailedStream() string {
	return getEnv("REDIS_PRODUCER_FAILED_STREAM", "task.failed:v1")
}

func GetRedisProducerUpdateTableStream() string {
	return getEnv("REDIS_PRODUCER_UPDATE_TABLE_STREAM", "update.table:v1")
}

// PostgreSQL configuration
func GetPostgresHost() string {
	return getEnv("POSTGRES_HOST", "localhost")
}

func GetPostgresPort() int {
	return getEnvAsInt("POSTGRES_PORT", 5432)
}

func GetPostgresDB() string {
	return getEnv("POSTGRES_DB", "runner")
}

func GetPostgresUser() string {
	return getEnv("POSTGRES_USER", "runner")
}

func GetPostgresPassword() string {
	return getEnv("POSTGRES_PASSWORD", "runner")
}

// gRPC configuration
func GetStateServiceGRPCAddr() string {
	return getEnv("STATE_SERVICE_GRPC_ADDR", "localhost:50051")
}

// HTTP configuration
func GetHTTPPort() int {
	return getEnvAsInt("HTTP_PORT", 8085)
}

// K8s configuration
func GetK8sNamespace() string {
	return getEnv("K8S_NAMESPACE", "default")
}

func GetK8sCheckDelaySeconds() int {
	return getEnvAsInt("K8S_CHECK_DELAY_SECONDS", 30)
}

// S3 configuration
func GetS3EndpointURL() string {
	return getEnv("S3_ENDPOINT_URL", "http://localstack:4566")
}

func GetS3Bucket() string {
	return getEnv("S3_BUCKET", "continuo")
}

func GetS3Region() string {
	return getEnv("AWS_DEFAULT_REGION", "us-east-1")
}

func GetS3AccessKeyID() string {
	return getEnv("AWS_ACCESS_KEY_ID", "test")
}

func GetS3SecretAccessKey() string {
	return getEnv("AWS_SECRET_ACCESS_KEY", "test")
}

// Log extraction configuration
func GetLogTailLines() int {
	return getEnvAsInt("LOG_TAIL_LINES", 50)
}

// Application configuration
func GetErrorMessageMaxLength() int {
	return getEnvAsInt("ERROR_MESSAGE_MAX_LENGTH", 4096)
}

// Stuck Entry Resolver configuration
func GetResolverCheckIntervalSeconds() int {
	return getEnvAsInt("RESOLVER_CHECK_INTERVAL_SECONDS", 30)
}

func GetResolverStuckThresholdSeconds() int {
	return getEnvAsInt("RESOLVER_STUCK_THRESHOLD_SECONDS", 60)
}

func GetResolverBatchSize() int {
	return getEnvAsInt("RESOLVER_BATCH_SIZE", 50)
}

func GetResolverMaxAttempts() int {
	return getEnvAsInt("RESOLVER_MAX_ATTEMPTS", 5)
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
