package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Config holds all configuration for the state service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

	// Redis streams
	RedisStreamSchedulerStarted            string
	RedisStreamSchedulesLoaded             string
	RedisStreamRunEntriesDispatched        string
	RedisStreamRunEntriesDispatchFailed    string
	RedisStreamTaskStatusUpdated           string
	RedisStreamTaskExecutionRecorded       string

	// gRPC
	GRPCPort int

	// HTTP health
	HealthPort string

	// Scheduler
	SchedulesConfigPath string
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis:    pkgconfig.LoadRedisFromAddr(v),
		Postgres: pkgconfig.LoadPostgres(v),

		RedisStreamSchedulerStarted:         v.Require("REDIS_STREAM_SCHEDULER_STARTED"),
		RedisStreamSchedulesLoaded:          v.Require("REDIS_STREAM_SCHEDULES_LOADED"),
		RedisStreamRunEntriesDispatched:     v.Require("REDIS_STREAM_RUN_ENTRIES_DISPATCHED"),
		RedisStreamRunEntriesDispatchFailed: v.Require("REDIS_STREAM_RUN_ENTRIES_DISPATCH_FAILED"),
		RedisStreamTaskStatusUpdated:        v.Require("REDIS_STREAM_TASK_STATUS_UPDATED"),
		RedisStreamTaskExecutionRecorded:    v.Require("REDIS_STREAM_TASK_EXECUTION_RECORDED"),

		GRPCPort:            envInt("GRPC_PORT", 50051),
		HealthPort:          env("HEALTH_PORT", "8082"),
		SchedulesConfigPath: env("SCHEDULES_CONFIG_PATH", "/etc/continuo/schedules.yaml"),
	}
}

func env(key, fallback string) string     { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
