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
	RedisStatusStream        string // task.status.updated:v1
	RedisNodeUpdatedStream   string // node.updated:v1 — for executor-side terminal failures

	// Schedule cancellation
	ScheduleCancelledStream            string
	ScheduleCancelledGroup             string
	CancelledSchedulesTTLHours         int
	CancelledSchedulesSweepIntervalMin int

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
		RedisStatusStream:        v.Require("REDIS_STATUS_STREAM"),
		RedisNodeUpdatedStream:   v.Require("REDIS_NODE_UPDATED_STREAM"),

		ScheduleCancelledStream:            v.Require("SCHEDULE_CANCELLED_STREAM"),
		ScheduleCancelledGroup:             v.Require("SCHEDULE_CANCELLED_GROUP"),
		CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
		CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),

		HTTPPort:     envInt("HTTP_PORT", 8084),
		K8sNamespace: v.Require("K8S_NAMESPACE"),
	}
}

func env(key, fallback string) string     { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
