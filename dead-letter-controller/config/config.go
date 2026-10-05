// Package config loads dead-letter-controller's configuration from the environment.
package config

import (
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// ServiceName names this service as the producer of the dead letters its
// stream consumers and outbox write.
const ServiceName = "dead-letter-controller"

const defaultShutdownGrace = 15 * time.Second

// Config is dead-letter-controller's configuration.
type Config struct {
	Redis         pkgconfig.RedisConfig
	Postgres      pkgconfig.PostgresConfig
	GRPCPort      int
	HTTPPort      string
	MetricsPort   int
	ShutdownGrace time.Duration
}

// Load reads the configuration; v accumulates missing required variables.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis:         pkgconfig.LoadRedis(v),
		Postgres:      pkgconfig.LoadPostgres(v),
		GRPCPort:      pkgconfig.EnvIntOrDefault("DEAD_LETTER_GRPC_PORT", 50055),
		HTTPPort:      pkgconfig.EnvOrDefault("DEAD_LETTER_HTTP_PORT", "8096"),
		MetricsPort:   pkgconfig.LoadMetricsPort(v),
		ShutdownGrace: pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", defaultShutdownGrace),
	}
}
