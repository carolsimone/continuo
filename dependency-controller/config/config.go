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
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis:    pkgconfig.LoadRedis(v),
		Postgres: pkgconfig.LoadPostgres(v),

		RedisConsumerStream: v.Require("REDIS_CONSUMER_STREAM"),
		RedisConsumerGroup:  v.Require("REDIS_CONSUMER_GROUP"),
		RedisProducerStream: v.Require("REDIS_PRODUCER_STREAM"),

		StateGRPCAddr: v.Require("STATE_SERVICE_GRPC_ADDR"),
		GraphGRPCAddr: v.Require("GRAPH_SERVICE_GRPC_ADDR"),

		HTTPPort: envInt("HTTP_PORT", 8086),
	}
}

func env(key, fallback string) string     { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
