package config

import pkgconfig "github.com/carolsimone/continuo/pkg/config"

// ServiceName names this service as the producer of the dead letters its
// stream consumers write.
const ServiceName = "remediation"

// Redis holds connection parameters for the Redis instance.
type Redis struct {
	Host, Port, Password string
}

// Config holds all configuration for the remediation service.
type Config struct {
	Postgres pkgconfig.PostgresConfig
	Redis    Redis
	S3       pkgconfig.S3Config
	HTTPPort string
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Postgres: pkgconfig.LoadPostgresWithDefaultDB(v, "continuo_remediation"),
		Redis: Redis{
			Host:     v.Require("REDIS_HOST"),
			Port:     pkgconfig.EnvOrDefault("REDIS_PORT", "6379"),
			Password: v.Require("REDIS_PASSWORD"),
		},
		S3:       pkgconfig.LoadS3(v),
		HTTPPort: pkgconfig.EnvOrDefault("REMEDIATION_HTTP_PORT", "8090"),
	}
}
