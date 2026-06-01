package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Config holds all configuration for the executor-controller service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

	// DBTWarehouse is the connection to the dbt materialization database (the same
	// Postgres server executor forwards to dbt job pods, database DBT_POSTGRES_DB).
	// Used to drop candidate schemas after validation completes. Host/port/user/password
	// are shared with the executor's own POSTGRES_* vars; only the database name differs.
	DBTWarehouse DBTWarehouse

	// Schedule cancellation
	CancelledSchedulesTTLHours         int
	CancelledSchedulesSweepIntervalMin int

	// HTTP
	HTTPPort int

	// K8s
	K8sNamespace string
	// Max concurrent K8s Jobs the deploy dispatcher keeps in flight.
	MaxConcurrentJobs int
}

// DBTWarehouse holds connection parameters for the dbt materialization database.
type DBTWarehouse struct {
	Host     string
	Port     int
	DB       string
	User     string
	Password string
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis:    pkgconfig.LoadRedis(v),
		Postgres: pkgconfig.LoadPostgres(v),

		DBTWarehouse: DBTWarehouse{
			Host:     pkgconfig.EnvOrDefault("POSTGRES_HOST", ""),
			Port:     pkgconfig.EnvIntOrDefault("POSTGRES_PORT", 5432),
			DB:       v.Require("DBT_POSTGRES_DB"),
			User:     pkgconfig.EnvOrDefault("POSTGRES_USER", ""),
			Password: pkgconfig.EnvOrDefault("POSTGRES_PASSWORD", ""),
		},

		CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
		CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),

		HTTPPort:          envInt("HTTP_PORT", 8084),
		K8sNamespace:      v.Require("K8S_NAMESPACE"),
		MaxConcurrentJobs: envInt("MAX_CONCURRENT_JOBS", 50),
	}
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
