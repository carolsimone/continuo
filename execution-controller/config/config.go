package config

import (
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// defaultShutdownGrace bounds the graceful-shutdown sequence: the in-flight
// drain plus the infra-close handlers. Override with SHUTDOWN_GRACE (e.g. "30s").
const defaultShutdownGrace = 15 * time.Second

// Config holds every setting the service reads at boot.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig
	// S3 is where terminal Jobs' logs and structured results are uploaded.
	S3 pkgconfig.S3Config

	// Schedule cancellation guard table: rows older than the TTL are swept.
	CancelledSchedulesTTLHours         int
	CancelledSchedulesSweepIntervalMin int

	HTTPPort int

	// K8sNamespace is where every Job is created and observed.
	K8sNamespace string
	// MaxConcurrentJobs caps the Jobs the dispatcher keeps in flight.
	MaxConcurrentJobs int
	// K8sCheckDelaySeconds is the delay between two status checks of a running Job.
	K8sCheckDelaySeconds int
	// DefaultTaskMaxRetries applies when a dispatch carries no retry budget.
	DefaultTaskMaxRetries int
	// LogTailLines is how many trailing log lines feed the error message.
	LogTailLines int
	// ErrorMessageMaxLength truncates the error message recorded for a failed Job.
	ErrorMessageMaxLength int

	// ShutdownGrace bounds the graceful-shutdown drain + infra teardown.
	ShutdownGrace time.Duration
}

// Load reads configuration from environment variables. v accumulates missing
// required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	// Every Job this service creates attaches the warehouse Secret named by
	// VALIDATION_WAREHOUSE_SECRET via envFrom. The k8s adapter reads it through
	// os.Getenv; it is required here so a missing name fails at boot rather
	// than failing every Job at dispatch time.
	_ = v.Require("VALIDATION_WAREHOUSE_SECRET")

	return Config{
		Redis:    pkgconfig.LoadRedis(v),
		Postgres: pkgconfig.LoadPostgres(v),
		S3:       pkgconfig.LoadS3(v),

		CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
		CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),

		HTTPPort:              envInt("HTTP_PORT", 8084),
		K8sNamespace:          v.Require("K8S_NAMESPACE"),
		MaxConcurrentJobs:     envInt("MAX_CONCURRENT_JOBS", 50),
		K8sCheckDelaySeconds:  envInt("K8S_CHECK_DELAY_SECONDS", 10),
		DefaultTaskMaxRetries: envInt("DEFAULT_TASK_MAX_RETRIES", 2),
		LogTailLines:          envInt("LOG_TAIL_LINES", 50),
		ErrorMessageMaxLength: envInt("ERROR_MESSAGE_MAX_LENGTH", 4096),

		ShutdownGrace: pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", defaultShutdownGrace),
	}
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
