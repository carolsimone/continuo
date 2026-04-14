package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Config holds all configuration for the dependency-controller service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

	// Redis streams
	RedisConsumerStream string
	RedisConsumerGroup  string
	RedisProducerStream string

	// gRPC clients
	StateGRPCAddr string
	GraphGRPCAddr string

	// HTTP
	HTTPPort int
}

// Load reads configuration from environment variables.
func Load() Config {
	return Config{
		Redis:    pkgconfig.LoadRedis(),
		Postgres: pkgconfig.LoadPostgres(),

		RedisConsumerStream: env("REDIS_CONSUMER_STREAM", "update.table:v1"),
		RedisConsumerGroup:  env("REDIS_CONSUMER_GROUP", "dependency_controller_consumers"),
		RedisProducerStream: env("REDIS_PRODUCER_STREAM", "query.model:v1"),

		StateGRPCAddr: env("STATE_SERVICE_GRPC_ADDR", "localhost:50051"),
		GraphGRPCAddr: env("GRAPH_SERVICE_GRPC_ADDR", "localhost:50052"),

		HTTPPort: envInt("HTTP_PORT", 8086),
	}
}

func env(key, fallback string) string    { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
