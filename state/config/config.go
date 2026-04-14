package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Config holds all configuration for the state service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

	// Redis streams
	RedisStreamSchedulerStarted string
	RedisStreamSchedulesLoaded  string

	// gRPC
	GRPCPort int

	// HTTP health
	HealthPort string

	// Scheduler
	SchedulesConfigPath string
}

// Load reads configuration from environment variables.
func Load() Config {
	return Config{
		Redis:    pkgconfig.LoadRedisFromAddr(),
		Postgres: pkgconfig.LoadPostgres(),

		RedisStreamSchedulerStarted: env("REDIS_STREAM_SCHEDULER_STARTED", "scheduler.started:v1"),
		RedisStreamSchedulesLoaded:  env("REDIS_STREAM_SCHEDULES_LOADED", "schedules.loaded:v1"),

		GRPCPort:   envInt("GRPC_PORT", 50051),
		HealthPort: env("HEALTH_PORT", "8082"),

		SchedulesConfigPath: env("SCHEDULES_CONFIG_PATH", "/etc/continuo/schedules.yaml"),
	}
}

func env(key, fallback string) string    { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
