package config

import (
	"os"
	"slices"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// loadEnv sets the always-required vars plus the caller's overrides for one
// Load. An empty override value unsets the variable. t.Setenv restores the
// previous environment when the test ends.
func loadEnv(t *testing.T, overrides map[string]string) (Config, *pkgconfig.Validator) {
	t.Helper()
	env := map[string]string{
		"REDIS_HOST":                    "localhost",
		"REDIS_PORT":                    "6379",
		"REDIS_PASSWORD":                "secret",
		"POSTGRES_HOST":                 "localhost",
		"POSTGRES_PORT":                 "5432",
		"POSTGRES_DB":                   "continuo_executor",
		"POSTGRES_USER":                 "continuo",
		"POSTGRES_PASSWORD":             "secret",
		"K8S_NAMESPACE":                 "default",
		"DBT_POSTGRES_DB":               "continuo_dbt",
		"S3_ENDPOINT_URL":               "http://localstack:4566",
		"S3_BUCKET":                     "continuo",
		"AWS_DEFAULT_REGION":            "us-east-1",
		"AWS_ACCESS_KEY_ID":             "test",
		"AWS_SECRET_ACCESS_KEY":         "test",
		"MAX_CONCURRENT_EXECUTIONS":     "50",
		"MAX_CONCURRENT_JOBS":           "",
		"EXECUTION_MODE":                "",
		"EXECUTION_MODE_OVERRIDES_JSON": "",
		"WORKER_IDLE_TIMEOUT":           "",
		"WORKER_LEASE_TTL":              "",
		"WORKER_CLAIM_WAIT":             "",
		"WORKER_CONTROL_PLANE_URL":      "",
	}
	for k, v := range overrides {
		env[k] = v
	}
	for k, v := range env {
		// t.Setenv registers the cleanup that restores the original value;
		// unsetting afterwards keeps "absent" distinct from "set to empty".
		t.Setenv(k, v)
		if v == "" {
			os.Unsetenv(k)
		}
	}
	v := &pkgconfig.Validator{}
	return Load(v), v
}

func TestLoad_DefaultsAreTheJobsPath(t *testing.T) {
	cfg, v := loadEnv(t, nil)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing vars, got %v", v.Missing())
	}
	if cfg.ExecutionMode != model.ExecutionModeJobs {
		t.Errorf("want default execution mode jobs, got %q", cfg.ExecutionMode)
	}
	if len(cfg.ExecutionModeOverrides) != 0 {
		t.Errorf("want no per-service overrides by default, got %v", cfg.ExecutionModeOverrides)
	}
	if cfg.WorkerIdleTimeout != 300*time.Second {
		t.Errorf("want idle timeout 300s, got %s", cfg.WorkerIdleTimeout)
	}
	if cfg.WorkerLeaseTTL != 60*time.Second {
		t.Errorf("want lease TTL 60s, got %s", cfg.WorkerLeaseTTL)
	}
	if cfg.WorkerClaimWait != 20*time.Second {
		t.Errorf("want claim wait 20s, got %s", cfg.WorkerClaimWait)
	}
}

// TestLoad_S3IsRequired pins that the executor cannot start without knowing
// where the object store is: worker pods hold no credentials of their own and
// reach every artifact through URLs this configuration signs.
func TestLoad_S3IsRequired(t *testing.T) {
	for _, key := range []string{"S3_ENDPOINT_URL", "S3_BUCKET", "AWS_DEFAULT_REGION"} {
		t.Run(key, func(t *testing.T) {
			_, v := loadEnv(t, map[string]string{key: ""})
			if !slices.Contains(v.Missing(), key) {
				t.Errorf("want %s reported missing, got %v", key, v.Missing())
			}
		})
	}
}

func TestLoad_S3FromEnv(t *testing.T) {
	cfg, v := loadEnv(t, nil)
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing vars, got %v", v.Missing())
	}
	if cfg.S3.Bucket != "continuo" {
		t.Errorf("want bucket continuo, got %q", cfg.S3.Bucket)
	}
	if cfg.S3.EndpointURL != "http://localstack:4566" {
		t.Errorf("want the configured endpoint, got %q", cfg.S3.EndpointURL)
	}
	if cfg.S3.Region != "us-east-1" {
		t.Errorf("want region us-east-1, got %q", cfg.S3.Region)
	}
}

func TestLoad_MaxConcurrentExecutionsFromEnv(t *testing.T) {
	cfg, v := loadEnv(t, map[string]string{"MAX_CONCURRENT_EXECUTIONS": "12"})
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing vars, got %v", v.Missing())
	}
	if cfg.MaxConcurrentExecutions != 12 {
		t.Fatalf("want 12, got %d", cfg.MaxConcurrentExecutions)
	}
}

// TestLoad_MaxConcurrentExecutionsIsRequired pins that the capacity limit has no
// in-code default: an executor started without it fails fast rather than
// silently sizing its own concurrency.
func TestLoad_MaxConcurrentExecutionsIsRequired(t *testing.T) {
	_, v := loadEnv(t, map[string]string{"MAX_CONCURRENT_EXECUTIONS": ""})
	if !slices.Contains(v.Missing(), "MAX_CONCURRENT_EXECUTIONS") {
		t.Fatalf("want MAX_CONCURRENT_EXECUTIONS reported missing, got %v", v.Missing())
	}
}

func TestLoad_MaxConcurrentExecutionsRejectsNonPositive(t *testing.T) {
	for _, raw := range []string{"0", "-1", "abc"} {
		t.Run(raw, func(t *testing.T) {
			_, v := loadEnv(t, map[string]string{"MAX_CONCURRENT_EXECUTIONS": raw})
			if !slices.Contains(v.Missing(), "MAX_CONCURRENT_EXECUTIONS(positive)") {
				t.Fatalf("want %q rejected, got missing=%v", raw, v.Missing())
			}
		})
	}
}

// TestLoad_MaxConcurrentJobsIsAnAlias pins the transition spelling: a deployment
// still setting MAX_CONCURRENT_JOBS keeps its configured capacity.
func TestLoad_MaxConcurrentJobsIsAnAlias(t *testing.T) {
	cfg, v := loadEnv(t, map[string]string{
		"MAX_CONCURRENT_EXECUTIONS": "",
		"MAX_CONCURRENT_JOBS":       "7",
	})
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing vars, got %v", v.Missing())
	}
	if cfg.MaxConcurrentExecutions != 7 {
		t.Fatalf("want 7 from the alias, got %d", cfg.MaxConcurrentExecutions)
	}
}

func TestLoad_MaxConcurrentExecutionsWinsOverTheAlias(t *testing.T) {
	cfg, _ := loadEnv(t, map[string]string{
		"MAX_CONCURRENT_EXECUTIONS": "9",
		"MAX_CONCURRENT_JOBS":       "7",
	})
	if cfg.MaxConcurrentExecutions != 9 {
		t.Fatalf("want the canonical var to win with 9, got %d", cfg.MaxConcurrentExecutions)
	}
}

func TestLoad_ExecutionModeWorkers(t *testing.T) {
	// Enabling workers makes the control-plane URL required, so this supplies one:
	// the subject here is the mode, not the URL rule.
	cfg, v := loadEnv(t, map[string]string{
		"EXECUTION_MODE":           "workers",
		"WORKER_CONTROL_PLANE_URL": "http://executor-controller:8084",
	})
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing vars, got %v", v.Missing())
	}
	if cfg.ExecutionMode != model.ExecutionModeWorkers {
		t.Fatalf("want workers, got %q", cfg.ExecutionMode)
	}
}

func TestLoad_UnknownExecutionModeIsRejected(t *testing.T) {
	_, v := loadEnv(t, map[string]string{"EXECUTION_MODE": "lambdas"})
	if !slices.Contains(v.Missing(), "EXECUTION_MODE(jobs|workers)") {
		t.Fatalf("want unknown mode rejected, got %v", v.Missing())
	}
}

func TestLoad_ExecutionModeOverridesParse(t *testing.T) {
	// Pinning finance to workers makes the control-plane URL required, so this
	// supplies one: the subject here is the override map, not the URL rule.
	cfg, v := loadEnv(t, map[string]string{
		"EXECUTION_MODE_OVERRIDES_JSON": `{"finance":"workers","legacy":"jobs"}`,
		"WORKER_CONTROL_PLANE_URL":      "http://executor-controller:8084",
	})
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing vars, got %v", v.Missing())
	}
	want := map[string]model.ExecutionMode{
		"finance": model.ExecutionModeWorkers,
		"legacy":  model.ExecutionModeJobs,
	}
	if len(cfg.ExecutionModeOverrides) != len(want) {
		t.Fatalf("want %v, got %v", want, cfg.ExecutionModeOverrides)
	}
	for svc, mode := range want {
		if cfg.ExecutionModeOverrides[svc] != mode {
			t.Errorf("service %q: want %q, got %q", svc, mode, cfg.ExecutionModeOverrides[svc])
		}
	}
}

func TestLoad_MalformedExecutionModeOverridesIsRejected(t *testing.T) {
	_, v := loadEnv(t, map[string]string{"EXECUTION_MODE_OVERRIDES_JSON": `{"finance":`})
	if !slices.Contains(v.Missing(), "EXECUTION_MODE_OVERRIDES_JSON(json)") {
		t.Fatalf("want malformed JSON rejected, got %v", v.Missing())
	}
}

func TestLoad_UnknownModeInOverridesIsRejected(t *testing.T) {
	_, v := loadEnv(t, map[string]string{"EXECUTION_MODE_OVERRIDES_JSON": `{"finance":"lambdas"}`})
	if !slices.Contains(v.Missing(), "EXECUTION_MODE_OVERRIDES_JSON(jobs|workers)") {
		t.Fatalf("want unknown override mode rejected, got %v", v.Missing())
	}
}

// TestLoad_WorkerControlPlaneURLIsRequiredWhenTheDefaultModeIsWorkers pins the
// fail-closed boot: the URL is baked into every pod a pool creates, so an empty
// one yields workers that can never reach the control plane. It is caught before
// a pool can be created, not after.
func TestLoad_WorkerControlPlaneURLIsRequiredWhenTheDefaultModeIsWorkers(t *testing.T) {
	_, v := loadEnv(t, map[string]string{
		"EXECUTION_MODE":           "workers",
		"WORKER_CONTROL_PLANE_URL": "",
	})
	if !slices.Contains(v.Missing(), "WORKER_CONTROL_PLANE_URL") {
		t.Fatalf("want WORKER_CONTROL_PLANE_URL reported missing, got %v", v.Missing())
	}
}

// TestLoad_WorkerControlPlaneURLIsRequiredWhenAnOverrideEnablesWorkers pins that
// the canary lever carries the same requirement: naming one service on workers
// is enough to create a pool, so it is enough to demand the URL.
func TestLoad_WorkerControlPlaneURLIsRequiredWhenAnOverrideEnablesWorkers(t *testing.T) {
	_, v := loadEnv(t, map[string]string{
		"EXECUTION_MODE_OVERRIDES_JSON": `{"finance":"workers"}`,
		"WORKER_CONTROL_PLANE_URL":      "",
	})
	if !slices.Contains(v.Missing(), "WORKER_CONTROL_PLANE_URL") {
		t.Fatalf("want WORKER_CONTROL_PLANE_URL reported missing, got %v", v.Missing())
	}
}

// TestLoad_WorkerControlPlaneURLIsNotRequiredOnTheJobsPath pins the other side of
// the rule: a deployment that routes every service to Jobs never creates a pool,
// so it must not be forced to configure a URL nothing would use.
func TestLoad_WorkerControlPlaneURLIsNotRequiredOnTheJobsPath(t *testing.T) {
	_, v := loadEnv(t, map[string]string{
		"EXECUTION_MODE_OVERRIDES_JSON": `{"legacy":"jobs"}`,
		"WORKER_CONTROL_PLANE_URL":      "",
	})
	if slices.Contains(v.Missing(), "WORKER_CONTROL_PLANE_URL") {
		t.Fatalf("want the jobs path to boot without a control-plane URL, got %v", v.Missing())
	}
}

func TestLoad_WorkerControlPlaneURLFromEnv(t *testing.T) {
	cfg, v := loadEnv(t, map[string]string{
		"EXECUTION_MODE":           "workers",
		"WORKER_CONTROL_PLANE_URL": "http://executor-controller:8084",
	})
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing vars, got %v", v.Missing())
	}
	if cfg.WorkerControlPlaneURL != "http://executor-controller:8084" {
		t.Fatalf("want the configured URL, got %q", cfg.WorkerControlPlaneURL)
	}
}

// TestLoad_LeaseMustHoldThreeHeartbeats pins the liveness margin: a worker must
// be able to miss two heartbeats without its lease being reaped. The cadence is
// the worker's own, so a lease that cannot hold three of them is rejected — the
// lease is the only side of this an operator can set.
func TestLoad_LeaseMustHoldThreeHeartbeats(t *testing.T) {
	// Three 10s heartbeats exactly fill a 30s lease, leaving no margin.
	_, v := loadEnv(t, map[string]string{"WORKER_LEASE_TTL": "30s"})
	if !slices.Contains(v.Missing(), "WORKER_LEASE_TTL(> 3x the worker's heartbeat)") {
		t.Fatalf("want a lease that only just holds three heartbeats rejected, got %v",
			v.Missing())
	}
}

func TestLoad_LeaseHoldingThreeHeartbeatsIsAccepted(t *testing.T) {
	cfg, v := loadEnv(t, map[string]string{"WORKER_LEASE_TTL": "31s"})
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing vars, got %v", v.Missing())
	}
	if cfg.WorkerLeaseTTL != 31*time.Second {
		t.Fatalf("want 31s, got %s", cfg.WorkerLeaseTTL)
	}
}
