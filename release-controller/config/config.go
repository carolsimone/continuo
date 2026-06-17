package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Postgres holds connection parameters for the release-controller Postgres instance.
type Postgres struct {
	Host, Port, User, Password, DB string
}

// Redis holds connection parameters for the Redis instance.
type Redis struct {
	Host, Port, Password string
}

// Config holds all configuration for the release-controller service.
type Config struct {
	Postgres              Postgres
	Redis                 Redis
	S3                    pkgconfig.S3Config
	HTTPPort              string
	ParseTimeout          string
	ParseHardTimeout      string
	ValidationTimeout     string
	ValidationHardTimeout string
	JanitorInterval       string
	RetentionDays         string
	RecoverStuckInterval  string
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Postgres: Postgres{
			Host:     v.Require("POSTGRES_HOST"),
			Port:     pkgconfig.EnvOrDefault("POSTGRES_PORT", "5432"),
			User:     v.Require("POSTGRES_USER"),
			Password: v.Require("POSTGRES_PASSWORD"),
			DB:       pkgconfig.EnvOrDefault("POSTGRES_DB", "continuo_release"),
		},
		Redis: Redis{
			Host:     v.Require("REDIS_HOST"),
			Port:     pkgconfig.EnvOrDefault("REDIS_PORT", "6379"),
			Password: pkgconfig.EnvOrDefault("REDIS_PASSWORD", ""),
		},
		S3:                    pkgconfig.LoadS3(v),
		HTTPPort:              pkgconfig.EnvOrDefault("RELEASE_CONTROLLER_HTTP_PORT", "8088"),
		ParseTimeout:          pkgconfig.EnvOrDefault("RELEASE_PARSE_TIMEOUT", "10m"),
		ParseHardTimeout:      pkgconfig.EnvOrDefault("RELEASE_PARSE_HARD_TIMEOUT", "1h"),
		ValidationTimeout:     pkgconfig.EnvOrDefault("RELEASE_VALIDATION_TIMEOUT", "30m"),
		ValidationHardTimeout: pkgconfig.EnvOrDefault("RELEASE_VALIDATION_HARD_TIMEOUT", "2h"),
		JanitorInterval:       pkgconfig.EnvOrDefault("RELEASE_JANITOR_INTERVAL", "24h"),
		RetentionDays:         pkgconfig.EnvOrDefault("RELEASE_RETENTION_DAYS", "90"),
		RecoverStuckInterval:  pkgconfig.EnvOrDefault("RECOVER_STUCK_INTERVAL", "1m"),
	}
}
