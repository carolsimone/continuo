package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Config holds all configuration for the executor-controller service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

	// Redis streams
	RedisConsumerStream      string
	RedisConsumerRetryStream string
	RedisConsumerGroup       string
	RedisProducerStream      string

	// gRPC clients
	StateGRPCAddr string

	// HTTP
	HTTPPort int

	// K8s
	K8sNamespace string
}

// Load reads configuration from environment variables.
func Load() Config {
	return Config{
		Redis:    pkgconfig.LoadRedis(),
		Postgres: pkgconfig.LoadPostgres(),

		RedisConsumerStream:      env("REDIS_CONSUMER_STREAM", "query.model:v1"),
		RedisConsumerRetryStream: env("REDIS_CONSUMER_RETRY_STREAM", "task.retry:v1"),
		RedisConsumerGroup:       env("REDIS_CONSUMER_GROUP", "executor_controller_consumers"),
		RedisProducerStream:      env("REDIS_PRODUCER_STREAM", "executor.deployed:v1"),

		StateGRPCAddr: env("STATE_SERVICE_GRPC_ADDR", "localhost:50051"),

		HTTPPort:     envInt("HTTP_PORT", 8084),
		K8sNamespace: env("K8S_NAMESPACE", "default"),
	}
}

func env(key, fallback string) string    { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
