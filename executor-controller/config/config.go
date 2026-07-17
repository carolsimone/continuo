package config

import (
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Worker execution defaults. They describe a pool that is not enabled: the
// executor ships routing every record to the Jobs path, and a worker canary is
// turned on per service through EXECUTION_MODE_OVERRIDES_JSON.
const (
	defaultWorkerIdleTimeout = 300 * time.Second
	defaultWorkerLeaseTTL    = 60 * time.Second
	defaultWorkerClaimWait   = 20 * time.Second
)

// WorkerHeartbeatInterval is how often a worker pod extends the lease it holds.
//
// It is not configurable. The worker keeps this cadence itself — heartbeat_seconds
// in dbt/base/continuo_dbt_runtime/worker.py — and nothing carries a different
// one to the pod, so a setting here would describe the interval without changing
// it. It is stated because WORKER_LEASE_TTL has to accommodate the real cadence:
// validating the lease against a number the worker does not use would check a
// fiction. TestWorkerHeartbeatIntervalMatchesTheWorker pins the two together.
const WorkerHeartbeatInterval = 10 * time.Second

// heartbeatsPerLease is the liveness margin every lease must allow: a worker
// gets at least three heartbeat attempts inside one lease deadline, so two lost
// heartbeats do not cost it a task the reaper would otherwise reassign.
const heartbeatsPerLease = 3

// Config holds all configuration for the executor-controller service.
type Config struct {
	Redis    pkgconfig.RedisConfig
	Postgres pkgconfig.PostgresConfig

	// S3 is the object store holding runtime artifacts and task results. Worker
	// pods hold no credentials of their own: they read their artifact and write
	// their results through URLs the executor signs with these.
	S3 pkgconfig.S3Config

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

	// ExecutionMode is how production records reach dbt unless their service
	// says otherwise: one Kubernetes Job per task, or a claim against a pool of
	// reusable worker pods.
	ExecutionMode model.ExecutionMode
	// ExecutionModeOverrides pins individual services to a mode, keyed by
	// service name. It is the rollout lever: with the default mode jobs, naming
	// a service here runs it — and only it — on workers.
	ExecutionModeOverrides map[string]model.ExecutionMode
	// MaxConcurrentExecutions caps the execution slots the executor keeps in
	// flight across both paths: Kubernetes Jobs and worker-claimed tasks draw
	// from the same budget.
	MaxConcurrentExecutions int
	// WorkerIdleTimeout is how long a worker pod waits without claiming any task
	// before it exits.
	WorkerIdleTimeout time.Duration
	// WorkerLeaseTTL is how long a claim holds a task before the reaper may
	// reassign it. It must leave room for the worker's own heartbeat cadence,
	// WorkerHeartbeatInterval, which the pod keeps and this cannot change.
	WorkerLeaseTTL time.Duration
	// WorkerClaimWait is how long a claim request blocks waiting for work before
	// it returns empty.
	WorkerClaimWait time.Duration
	// WorkerControlPlaneURL is the base URL worker pods call to claim tasks and
	// report their outcomes.
	WorkerControlPlaneURL string
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
	leaseTTL := pkgconfig.EnvDurationOrDefault("WORKER_LEASE_TTL", defaultWorkerLeaseTTL)
	if heartbeatsPerLease*WorkerHeartbeatInterval >= leaseTTL {
		v.Add("WORKER_LEASE_TTL(> 3x the worker's heartbeat)")
	}

	mode := loadExecutionMode(v)
	overrides := loadExecutionModeOverrides(v)

	return Config{
		Redis:    pkgconfig.LoadRedis(v),
		Postgres: pkgconfig.LoadPostgres(v),
		S3:       pkgconfig.LoadS3(v),

		DBTWarehouse: DBTWarehouse{
			Host:     pkgconfig.EnvOrDefault("POSTGRES_HOST", ""),
			Port:     pkgconfig.EnvIntOrDefault("POSTGRES_PORT", 5432),
			DB:       v.Require("DBT_POSTGRES_DB"),
			User:     pkgconfig.EnvOrDefault("POSTGRES_USER", ""),
			Password: pkgconfig.EnvOrDefault("POSTGRES_PASSWORD", ""),
		},

		CancelledSchedulesTTLHours:         envInt("CANCELLED_SCHEDULES_TTL_HOURS", 24),
		CancelledSchedulesSweepIntervalMin: envInt("CANCELLED_SCHEDULES_SWEEP_INTERVAL_MINUTES", 60),

		HTTPPort:     envInt("HTTP_PORT", 8084),
		K8sNamespace: v.Require("K8S_NAMESPACE"),

		ExecutionMode:           mode,
		ExecutionModeOverrides:  overrides,
		MaxConcurrentExecutions: loadMaxConcurrentExecutions(v),
		WorkerIdleTimeout:       pkgconfig.EnvDurationOrDefault("WORKER_IDLE_TIMEOUT", defaultWorkerIdleTimeout),
		WorkerLeaseTTL:          leaseTTL,
		WorkerClaimWait:         pkgconfig.EnvDurationOrDefault("WORKER_CLAIM_WAIT", defaultWorkerClaimWait),
		WorkerControlPlaneURL:   loadWorkerControlPlaneURL(v, mode, overrides),
	}
}

// loadWorkerControlPlaneURL reads the base URL worker pods call to claim tasks
// and report outcomes. It has no default: the value is baked into every pod a
// pool creates, so an empty one would produce workers that can reach nothing and
// fail only once they are already running. It is required exactly when a pool can
// be created — the default mode is workers, or a service is pinned to them —
// which puts the failure at boot, before any pool exists. A deployment routing
// every service to Jobs creates no pool and needs no URL.
func loadWorkerControlPlaneURL(
	v *pkgconfig.Validator,
	mode model.ExecutionMode,
	overrides map[string]model.ExecutionMode,
) string {
	url := os.Getenv("WORKER_CONTROL_PLANE_URL")
	if url == "" && workersReachable(mode, overrides) {
		v.Add("WORKER_CONTROL_PLANE_URL")
	}
	return url
}

// workersReachable reports whether any service can route to a worker pool under
// this configuration.
func workersReachable(mode model.ExecutionMode, overrides map[string]model.ExecutionMode) bool {
	if mode == model.ExecutionModeWorkers {
		return true
	}
	for _, serviceMode := range overrides {
		if serviceMode == model.ExecutionModeWorkers {
			return true
		}
	}
	return false
}

// loadMaxConcurrentExecutions reads the executor's shared capacity budget. It is
// required and has no in-code default, so an executor can never size its own
// concurrency from a literal that does not match the cluster it runs in.
// MAX_CONCURRENT_JOBS is accepted as a transition alias for deployments that
// still carry the older spelling.
func loadMaxConcurrentExecutions(v *pkgconfig.Validator) int {
	rawLimit := os.Getenv("MAX_CONCURRENT_EXECUTIONS")
	if rawLimit == "" {
		rawLimit = os.Getenv("MAX_CONCURRENT_JOBS")
	}
	if rawLimit == "" {
		v.Add("MAX_CONCURRENT_EXECUTIONS")
		return 0
	}
	limit, err := strconv.Atoi(rawLimit)
	if err != nil || limit <= 0 {
		v.Add("MAX_CONCURRENT_EXECUTIONS(positive)")
		return 0
	}
	return limit
}

// loadExecutionMode reads the default path production records take. Absent means
// jobs: workers are opt-in.
func loadExecutionMode(v *pkgconfig.Validator) model.ExecutionMode {
	raw := os.Getenv("EXECUTION_MODE")
	if raw == "" {
		return model.ExecutionModeJobs
	}
	mode := model.ExecutionMode(raw)
	if !mode.Valid() {
		v.Add("EXECUTION_MODE(jobs|workers)")
		return model.ExecutionModeJobs
	}
	return mode
}

// loadExecutionModeOverrides reads the per-service mode pins. An unparseable or
// unknown value is rejected rather than dropped: a typo in a canary's service
// name or mode must not silently leave that service on the default path.
func loadExecutionModeOverrides(v *pkgconfig.Validator) map[string]model.ExecutionMode {
	raw := os.Getenv("EXECUTION_MODE_OVERRIDES_JSON")
	if raw == "" {
		return nil
	}
	var byService map[string]string
	if err := json.Unmarshal([]byte(raw), &byService); err != nil {
		v.Add("EXECUTION_MODE_OVERRIDES_JSON(json)")
		return nil
	}
	overrides := make(map[string]model.ExecutionMode, len(byService))
	for service, raw := range byService {
		mode := model.ExecutionMode(raw)
		if !mode.Valid() {
			v.Add("EXECUTION_MODE_OVERRIDES_JSON(jobs|workers)")
			return nil
		}
		overrides[service] = mode
	}
	return overrides
}

func envInt(key string, fallback int) int { return pkgconfig.EnvIntOrDefault(key, fallback) }
