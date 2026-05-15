package config

import (
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// ResolverConfig holds stuck-entry resolver tuning parameters.
type ResolverConfig struct {
	CheckIntervalSeconds  int
	StuckThresholdSeconds int
	BatchSize             int
	MaxAttempts           int
}

// Config holds all configuration for the k8s-controller service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig
	S3       pkgconfig.S3Config
	Resolver ResolverConfig

	// HTTP
	HTTPPort int

	// K8s
	K8sNamespace         string
	K8sCheckDelaySeconds int

	// Retry configuration
	DefaultTaskMaxRetries int

	// Log extraction
	LogTailLines          int
	ErrorMessageMaxLength int

	// Schedule cancellation
	CancelledSchedulesTTLHours         int
	CancelledSchedulesSweepIntervalMin int
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis:    pkgconfig.LoadRedis(v),
		Postgres: pkgconfig.LoadPostgres(v),
		S3:       pkgconfig.LoadS3(v),

		Resolver: ResolverConfig{
			CheckIntervalSeconds:  envInt("RESOLVER_CHECK_INTERVAL_SECONDS", 30),
			StuckThresholdSeconds: envInt("RESOLVER_STUCK_THRESHOLD_SECONDS", 60),
			BatchSize:             envInt("RESOLVER_BATCH_SIZE", 50),
			MaxAttempts:           envInt("RESOLVER_MAX_ATTEMPTS", 5),
		},

		HTTPPort:              envInt("HTTP_PORT", 8085),
		K8sNamespace:          v.Require("K8S_NAMESPACE"),
		K8sCheckDelaySeconds:  envInt("K8S_CHECK_DELAY_SECONDS", 30),
		DefaultTaskMaxRetries: envInt("DEFAULT_TASK_MAX_RETRIES", 2),
		LogTailLines:          envInt("LOG_TAIL_LINES", 50),
		ErrorMessageMaxLength: envInt("ERROR_MESSAGE_MAX_LENGTH", 4096),

		CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
		CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),
	}
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
