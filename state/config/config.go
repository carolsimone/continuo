package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// ServiceName names this service as the producer of the dead letters its
// stream consumers write.
const ServiceName = "state"

// The retry budget's default and the range state accepts: the chart's
// runs.taskMaxRetries, 0 to 10.
const (
	DefaultTaskMaxRetries = 2
	MinTaskMaxRetries     = 0
	MaxTaskMaxRetries     = 10
)

// taskMaxRetriesKey names the retry budget stamped on each task state plans.
const taskMaxRetriesKey = "TASK_MAX_RETRIES"

// defaultShutdownGrace bounds the graceful-shutdown sequence: the in-flight
// drain plus the infra-close handlers. It is a safe default so no required env
// var is introduced; override with SHUTDOWN_GRACE (e.g. "30s").
const defaultShutdownGrace = 15 * time.Second

// Config holds all configuration for the state service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

	// S3 locates the object storage holding release topology artifacts.
	// Endpoint, bucket and region are required; empty access keys select the
	// SDK's default credential chain.
	S3 pkgconfig.S3Config

	// gRPC
	GRPCPort int

	// HTTP health
	HealthPort string

	// Scheduler
	SchedulesConfigPath string

	// Retention sweeper — purges processed outbox rows older than
	// RetentionDays, message_processing dedup rows, in any state, older than
	// the longer of RetentionDays and the 30-day replay horizon, and promoted
	// topology releases loaded more than RetentionDays ago that are neither the
	// live release nor named by a pending or running run. Both knobs have safe
	// defaults so no configuration is required.
	RetentionDays             int
	RetentionSweepIntervalMin int

	// ShutdownGrace bounds the graceful-shutdown drain + infra teardown.
	ShutdownGrace time.Duration

	// IgnoredPoolKeys names DB_POOL_SIZE and DB_MAX_OVERFLOW when they are set.
	// state does not read them: its pool limits come from DB_MAX_OPEN_CONNS
	// and DB_MAX_IDLE_CONNS.
	IgnoredPoolKeys []string

	// Maintenance is MAINTENANCE_ENABLED: while true, state skips cron fires and
	// refuses the RPCs that start runs.
	Maintenance bool

	// MetricsPort is the port the Prometheus /metrics listener binds (METRICS_PORT).
	MetricsPort int

	// TaskMaxRetries is TASK_MAX_RETRIES: how many retries each task state
	// plans gets after its first attempt, stamped on the task when its run is
	// created.
	TaskMaxRetries int

	// TaskTimeout is TASK_TIMEOUT_SECONDS: how long the container of a task
	// whose node declares no timeout may run, sent on each such dispatch.
	TaskTimeout time.Duration
}

// Load reads configuration from environment variables.
// v accumulates missing required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	return Config{
		Redis:    pkgconfig.LoadRedisFromAddr(v),
		Postgres: pkgconfig.LoadPostgres(v),
		S3:       pkgconfig.LoadS3(v),

		GRPCPort:            envInt("GRPC_PORT", 50051),
		HealthPort:          env("HEALTH_PORT", "8082"),
		SchedulesConfigPath: env("SCHEDULES_CONFIG_PATH", "/etc/continuo/schedules.yaml"),

		RetentionDays:             envInt("RETENTION_DAYS", 7),
		RetentionSweepIntervalMin: envInt("RETENTION_SWEEP_INTERVAL_MINUTES", 60),

		ShutdownGrace: pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", defaultShutdownGrace),

		IgnoredPoolKeys: setKeys("DB_POOL_SIZE", "DB_MAX_OVERFLOW"),

		Maintenance: pkgconfig.LoadMaintenance(v),

		MetricsPort: pkgconfig.LoadMetricsPort(v),

		TaskMaxRetries: taskMaxRetries(v),
		TaskTimeout:    pkgconfig.LoadTaskTimeout(v),
	}
}

// taskMaxRetries reads TASK_MAX_RETRIES, a whole number from MinTaskMaxRetries
// to MaxTaskMaxRetries; unset selects DefaultTaskMaxRetries. Any other value
// is recorded on v, so state refuses to start.
func taskMaxRetries(v *pkgconfig.Validator) int {
	raw := os.Getenv(taskMaxRetriesKey)
	if raw == "" {
		return DefaultTaskMaxRetries
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < MinTaskMaxRetries || n > MaxTaskMaxRetries {
		v.Add(fmt.Sprintf("%s (invalid value %q: expected a whole number from %d to %d)",
			taskMaxRetriesKey, raw, MinTaskMaxRetries, MaxTaskMaxRetries))
		return DefaultTaskMaxRetries
	}
	return n
}

// setKeys returns the keys among keys that are set to a non-empty value.
func setKeys(keys ...string) []string {
	var set []string
	for _, key := range keys {
		if os.Getenv(key) != "" {
			set = append(set, key)
		}
	}
	return set
}

func env(key, fallback string) string     { return pkgconfig.EnvOrDefault(key, fallback) }
func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
