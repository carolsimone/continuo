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
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis:    pkgconfig.LoadRedis(v),
		Postgres: pkgconfig.LoadPostgres(v),

		RedisConsumerStream:      v.Require("REDIS_CONSUMER_STREAM"),
		RedisConsumerRetryStream: v.Require("REDIS_CONSUMER_RETRY_STREAM"),
		RedisConsumerGroup:       v.Require("REDIS_CONSUMER_GROUP"),
		RedisProducerStream:      v.Require("REDIS_PRODUCER_STREAM"),

		StateGRPCAddr: v.Require("STATE_SERVICE_GRPC_ADDR"),

		HTTPPort:     envInt("HTTP_PORT", 8084),
		K8sNamespace: v.Require("K8S_NAMESPACE"),
	}
}

func env(key, fallback string) string     { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
