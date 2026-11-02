package config

import pkgconfig "github.com/carolsimone/continuo/pkg/config"

// ServiceName names this service as the producer of the dead letters its
// stream consumers write.
const ServiceName = "remediation"

// Config holds all configuration for the remediation service.
type Config struct {
	Postgres pkgconfig.PostgresConfig
	Redis    pkgconfig.RedisConfig
	S3       pkgconfig.S3Config
	HTTPPort string

	// MetricsPort is the port the Prometheus /metrics listener binds (METRICS_PORT).
	MetricsPort int
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Postgres: pkgconfig.LoadPostgresWithDefaultDB(v, "continuo_remediation"),
		Redis:    pkgconfig.LoadRedis(v),
		S3:       pkgconfig.LoadS3(v),
		HTTPPort: pkgconfig.EnvOrDefault("REMEDIATION_HTTP_PORT", "8090"),

		MetricsPort: pkgconfig.LoadMetricsPort(v),
	}
}
