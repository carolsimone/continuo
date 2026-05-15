package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Config holds all configuration for the state service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

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

		GRPCPort:            envInt("GRPC_PORT", 50051),
		HealthPort:          env("HEALTH_PORT", "8082"),
		SchedulesConfigPath: env("SCHEDULES_CONFIG_PATH", "/etc/continuo/schedules.yaml"),
	}
}

func env(key, fallback string) string     { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
