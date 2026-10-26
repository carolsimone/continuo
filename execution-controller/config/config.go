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
const ServiceName = "execution-controller"

// defaultShutdownGrace bounds the graceful-shutdown sequence: the in-flight
// drain plus the infra-close handlers. Override with SHUTDOWN_GRACE (e.g. "30s").
const defaultShutdownGrace = 15 * time.Second

// defaultMaxConcurrentJobs is the admission cap when MAX_CONCURRENT_JOBS is unset.
const defaultMaxConcurrentJobs = 50

// The start timeout's default and the range execution-controller accepts:
// the chart's execution.startTimeoutMinutes, 1 to 120 minutes.
const (
	defaultJobStartTimeout = 15 * time.Minute
	MinJobStartTimeout     = time.Minute
	MaxJobStartTimeout     = 2 * time.Hour
)

// The Kubernetes client's request rate limit: its defaults and the ranges
// execution-controller accepts, the chart's execution.kubeClientQPS and
// execution.kubeClientBurst.
const (
	defaultK8sClientQPS   = 50
	defaultK8sClientBurst = 100
	MinK8sClientQPS       = 1
	MaxK8sClientQPS       = 1000
	MinK8sClientBurst     = 1
	MaxK8sClientBurst     = 2000
)

// Config holds every setting the service reads at boot.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig
	// S3 is where terminal Jobs' logs and structured results are uploaded.
	S3 pkgconfig.S3Config

	// Retention sweeper: processed execution_outbox rows are deleted after
	// RetentionDays, message_processing dedup rows after the longer of
	// RetentionDays and the 30-day replay horizon. Both have safe defaults.
	RetentionDays             int
	RetentionSweepIntervalMin int

	HTTPPort int

	// K8sNamespace is where every Job is created and observed.
	K8sNamespace string
	// MaxConcurrentJobs caps the deployments holding an execution slot
	// (reserved, starting, running or terminating) across every replica.
	MaxConcurrentJobs int
	// JobStartTimeout is how long a Job's pod may take, after the Job is
	// created, to start any container; past it the attempt is ended.
	JobStartTimeout time.Duration
	// K8sClientQPS and K8sClientBurst limit the requests per second the
	// Kubernetes client sends, and how many it may send at once in a spike.
	K8sClientQPS   int
	K8sClientBurst int
	// LogTailLines is how many trailing log lines feed the error message.
	LogTailLines int
	// ErrorMessageMaxLength truncates the error message recorded for a failed Job.
	ErrorMessageMaxLength int

	// ShutdownGrace bounds the graceful-shutdown drain + infra teardown.
	ShutdownGrace time.Duration

	// TaskTimeout is TASK_TIMEOUT_SECONDS: how long the container of every
	// validation, seed-build and compile Job may run. A task's own timeout
	// arrives on its dispatch.
	TaskTimeout time.Duration

	// MetricsPort is the port the Prometheus /metrics listener binds (METRICS_PORT).
	MetricsPort int
}

// Load reads configuration from environment variables. v accumulates missing
// required vars; check v.Missing() after calling.
func Load(v *pkgconfig.Validator) Config {
	// Every Job this service creates attaches the warehouse Secret named by
	// VALIDATION_WAREHOUSE_SECRET via envFrom. The k8s adapter reads it through
	// os.Getenv; it is required here so a missing name fails at boot rather
	// than failing every Job at dispatch time.
	_ = v.Require("VALIDATION_WAREHOUSE_SECRET")

	k8sClientQPS, k8sClientBurst := k8sClientRateLimit(v)

	return Config{
		Redis:    pkgconfig.LoadRedis(v),
		Postgres: pkgconfig.LoadPostgres(v),
		S3:       pkgconfig.LoadS3(v),

		RetentionDays:             envInt("RETENTION_DAYS", 7),
		RetentionSweepIntervalMin: envInt("RETENTION_SWEEP_INTERVAL_MINUTES", 60),

		HTTPPort:              envInt("HTTP_PORT", 8084),
		K8sNamespace:          v.Require("K8S_NAMESPACE"),
		MaxConcurrentJobs:     maxConcurrentJobs(v),
		JobStartTimeout:       jobStartTimeout(v),
		K8sClientQPS:          k8sClientQPS,
		K8sClientBurst:        k8sClientBurst,
		LogTailLines:          envInt("LOG_TAIL_LINES", 50),
		ErrorMessageMaxLength: envInt("ERROR_MESSAGE_MAX_LENGTH", 4096),

		ShutdownGrace: pkgconfig.EnvDurationOrDefault("SHUTDOWN_GRACE", defaultShutdownGrace),

		TaskTimeout: pkgconfig.LoadTaskTimeout(v),

		MetricsPort: pkgconfig.LoadMetricsPort(v),
	}
}

// maxConcurrentJobs reads the admission cap: the most deployments holding an
// execution slot at once, across every replica. A value that is not a whole
// number of at least 1 is a validation failure.
func maxConcurrentJobs(v *pkgconfig.Validator) int {
	if n := v.PositiveIntOrZero("MAX_CONCURRENT_JOBS"); n > 0 {
		return n
	}
	return defaultMaxConcurrentJobs
}

// jobStartTimeout reads JOB_START_TIMEOUT_SECONDS, a whole number of seconds
// from MinJobStartTimeout to MaxJobStartTimeout; unset selects the default. Any
// other value is a validation failure naming the key. The seconds are compared
// with the range before they become a duration, so a value too large for one
// is refused rather than wrapped into the range.
func jobStartTimeout(v *pkgconfig.Validator) time.Duration {
	const key = "JOB_START_TIMEOUT_SECONDS"
	raw := os.Getenv(key)
	if raw == "" {
		return defaultJobStartTimeout
	}
	minSecs, maxSecs := int(MinJobStartTimeout.Seconds()), int(MaxJobStartTimeout.Seconds())
	n, err := strconv.Atoi(raw)
	if err != nil || n < minSecs || n > maxSecs {
		v.Add(fmt.Sprintf("%s (invalid value %q: expected whole seconds from %d to %d)",
			key, raw, minSecs, maxSecs))
		return defaultJobStartTimeout
	}
	return time.Duration(n) * time.Second
}

// k8sClientRateLimit reads K8S_CLIENT_QPS and K8S_CLIENT_BURST, whole numbers
// within their ranges; unset selects the default. The burst must be at least
// the QPS. Any other value is a validation failure naming the key.
func k8sClientRateLimit(v *pkgconfig.Validator) (qps, burst int) {
	qps = wholeNumberInRange(v, "K8S_CLIENT_QPS", defaultK8sClientQPS, MinK8sClientQPS, MaxK8sClientQPS)
	burst = wholeNumberInRange(v, "K8S_CLIENT_BURST", defaultK8sClientBurst, MinK8sClientBurst, MaxK8sClientBurst)
	if burst < qps {
		v.Add(fmt.Sprintf("K8S_CLIENT_BURST (%d is below K8S_CLIENT_QPS %d: the burst must be at least the QPS)", burst, qps))
	}
	return qps, burst
}

// wholeNumberInRange reads key as a whole number from lo to hi; unset selects
// fallback. Any other value is a validation failure naming the key.
func wholeNumberInRange(v *pkgconfig.Validator, key string, fallback, lo, hi int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < lo || n > hi {
		v.Add(fmt.Sprintf("%s (invalid value %q: expected a whole number from %d to %d)", key, raw, lo, hi))
		return fallback
	}
	return n
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
