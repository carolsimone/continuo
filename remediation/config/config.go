package config

import pkgconfig "github.com/carolsimone/continuo/pkg/config"

// Postgres holds connection parameters for the remediation service Postgres instance.
type Postgres struct {
	Host, Port, User, Password, DB string
}

// Redis holds connection parameters for the Redis instance.
type Redis struct {
	Host, Port, Password string
}

// Config holds all configuration for the remediation service.
type Config struct {
	Postgres Postgres
	Redis    Redis
	S3       pkgconfig.S3Config
	HTTPPort string
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
			DB:       pkgconfig.EnvOrDefault("POSTGRES_DB", "continuo_remediation"),
		},
		Redis: Redis{
			Host:     v.Require("REDIS_HOST"),
			Port:     pkgconfig.EnvOrDefault("REDIS_PORT", "6379"),
			Password: pkgconfig.EnvOrDefault("REDIS_PASSWORD", ""),
		},
		S3:       pkgconfig.LoadS3(v),
		HTTPPort: pkgconfig.EnvOrDefault("REMEDIATION_HTTP_PORT", "8090"),
	}
}
