package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Config holds all configuration for the startup-controller service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

	// Redis streams — existing
	RedisConsumerStream string
	RedisConsumerGroup  string
	RedisProducerStream string

	// Redis streams — initialize.run producer
	InitializeRunProducerStream string

	// Redis streams — run.initialized consumer
	RunInitializedConsumerStream string
	RunInitializedConsumerGroup  string

	// Redis streams — rerun.ready consumer
	RerunReadyConsumerStream string
	RerunReadyConsumerGroup  string

	// gRPC clients
	StateGRPCAddr string

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

		InitializeRunProducerStream: env("REDIS_PRODUCER_INIT_STREAM", "initialize.run:v1"),

		RunInitializedConsumerStream: env("REDIS_CONSUMER_RUN_INITIALIZED_STREAM", "run.initialized:v1"),
		RunInitializedConsumerGroup:  env("REDIS_CONSUMER_RUN_INITIALIZED_GROUP", "startup_run_initialized"),

		RerunReadyConsumerStream: env("REDIS_CONSUMER_RERUN_READY_STREAM", "rerun.ready:v1"),
		RerunReadyConsumerGroup:  env("REDIS_CONSUMER_RERUN_READY_GROUP", "startup_rerun_ready"),

		StateGRPCAddr: v.Require("STATE_SERVICE_GRPC_ADDR"),

		HTTPPort: envInt("HTTP_PORT", 8083),
	}
}

func env(key, fallback string) string     { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
