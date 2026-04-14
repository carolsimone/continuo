package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// ResolverConfig holds stuck-entry resolver tuning parameters.
type ResolverConfig struct {
	CheckIntervalSeconds  int
	StuckThresholdSeconds int
	BatchSize             int
	MaxAttempts           int
}

// Config holds all configuration for the k8s-controller service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig
	S3       pkgconfig.S3Config
	Resolver ResolverConfig

	// Redis streams
	RedisConsumerDeployedStream    string
	RedisConsumerCheckStream       string
	RedisConsumerGroup             string
	RedisProducerCheckStream       string
	RedisProducerRetryStream       string
	RedisProducerFailedStream      string
	RedisProducerUpdateTableStream string

	// gRPC clients
	StateGRPCAddr string

	// HTTP
	HTTPPort int

	// K8s
	K8sNamespace         string
	K8sCheckDelaySeconds int

	// Log extraction
	LogTailLines          int
	ErrorMessageMaxLength int
}

// Load reads configuration from environment variables.
func Load() Config {
	return Config{
		Redis:    pkgconfig.LoadRedis(),
		Postgres: pkgconfig.LoadPostgres(),
		S3:       pkgconfig.LoadS3(),

		Resolver: ResolverConfig{
			CheckIntervalSeconds:  envInt("RESOLVER_CHECK_INTERVAL_SECONDS", 30),
			StuckThresholdSeconds: envInt("RESOLVER_STUCK_THRESHOLD_SECONDS", 60),
			BatchSize:             envInt("RESOLVER_BATCH_SIZE", 50),
			MaxAttempts:           envInt("RESOLVER_MAX_ATTEMPTS", 5),
		},

		RedisConsumerDeployedStream:    env("REDIS_CONSUMER_DEPLOYED_STREAM", "executor.deployed:v1"),
		RedisConsumerCheckStream:       env("REDIS_CONSUMER_CHECK_STREAM", "k8s.check:v1"),
		RedisConsumerGroup:             env("REDIS_CONSUMER_GROUP", "k8s_controller_consumers"),
		RedisProducerCheckStream:       env("REDIS_PRODUCER_CHECK_STREAM", "k8s.check:v1"),
		RedisProducerRetryStream:       env("REDIS_PRODUCER_RETRY_STREAM", "task.retry:v1"),
		RedisProducerFailedStream:      env("REDIS_PRODUCER_FAILED_STREAM", "task.failed:v1"),
		RedisProducerUpdateTableStream: env("REDIS_PRODUCER_UPDATE_TABLE_STREAM", "update.table:v1"),

		StateGRPCAddr: env("STATE_SERVICE_GRPC_ADDR", "localhost:50051"),

		HTTPPort:              envInt("HTTP_PORT", 8085),
		K8sNamespace:          env("K8S_NAMESPACE", "default"),
		K8sCheckDelaySeconds:  envInt("K8S_CHECK_DELAY_SECONDS", 30),
		LogTailLines:          envInt("LOG_TAIL_LINES", 50),
		ErrorMessageMaxLength: envInt("ERROR_MESSAGE_MAX_LENGTH", 4096),
	}
}

func env(key, fallback string) string    { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
