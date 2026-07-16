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
		"MAX_CONCURRENT_EXECUTIONS":     "50",
		"MAX_CONCURRENT_JOBS":           "",
		"EXECUTION_MODE":                "",
		"EXECUTION_MODE_OVERRIDES_JSON": "",
		"WORKER_IDLE_TIMEOUT":           "",
		"WORKER_LEASE_TTL":              "",
		"WORKER_HEARTBEAT_INTERVAL":     "",
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
	if cfg.WorkerHeartbeatInterval != 15*time.Second {
		t.Errorf("want heartbeat interval 15s, got %s", cfg.WorkerHeartbeatInterval)
	}
	if cfg.WorkerClaimWait != 20*time.Second {
		t.Errorf("want claim wait 20s, got %s", cfg.WorkerClaimWait)
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
	cfg, v := loadEnv(t, map[string]string{"EXECUTION_MODE": "workers"})
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
	cfg, v := loadEnv(t, map[string]string{
		"EXECUTION_MODE_OVERRIDES_JSON": `{"finance":"workers","legacy":"jobs"}`,
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

// TestLoad_HeartbeatMustFitThreeTimesInTheLease pins the liveness margin: a
// worker must be able to miss two heartbeats without its lease being reaped.
func TestLoad_HeartbeatMustFitThreeTimesInTheLease(t *testing.T) {
	_, v := loadEnv(t, map[string]string{
		"WORKER_LEASE_TTL":          "30s",
		"WORKER_HEARTBEAT_INTERVAL": "10s",
	})
	if !slices.Contains(v.Missing(), "WORKER_HEARTBEAT_INTERVAL(3x < WORKER_LEASE_TTL)") {
		t.Fatalf("want a heartbeat that exactly fills the lease rejected, got %v", v.Missing())
	}
}

func TestLoad_HeartbeatFittingThreeTimesIsAccepted(t *testing.T) {
	cfg, v := loadEnv(t, map[string]string{
		"WORKER_LEASE_TTL":          "31s",
		"WORKER_HEARTBEAT_INTERVAL": "10s",
	})
	if len(v.Missing()) != 0 {
		t.Fatalf("want no missing vars, got %v", v.Missing())
	}
	if cfg.WorkerLeaseTTL != 31*time.Second || cfg.WorkerHeartbeatInterval != 10*time.Second {
		t.Fatalf("want 31s/10s, got %s/%s", cfg.WorkerLeaseTTL, cfg.WorkerHeartbeatInterval)
	}
}
