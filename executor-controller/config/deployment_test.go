package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"gopkg.in/yaml.v3"
)

// The executor is deployed from four independent manifests, and a variable
// missing from any one of them is a real defect: the deployment that lacks it
// silently runs on in-code defaults. These tests read all four and hold them to
// the same configuration.
//
// They assert the variable NAMES as well as the values, because a name Load does
// not read is the failure that hides best: the manifest looks configured, the
// value is ignored, and the executor runs on a default nobody chose.

// wantedExecutorEnv is the worker configuration every deployment path carries.
// The durations are Go duration strings because that is what Load parses; a
// bare number would be silently discarded for the in-code default.
var wantedExecutorEnv = map[string]string{
	"EXECUTION_MODE":                "jobs",
	"EXECUTION_MODE_OVERRIDES_JSON": "{}",
	"MAX_CONCURRENT_EXECUTIONS":     "50",
	"WORKER_IDLE_TIMEOUT":           "300s",
	"WORKER_LEASE_TTL":              "60s",
	"WORKER_HEARTBEAT_INTERVAL":     "15s",
	"WORKER_CLAIM_WAIT":             "20s",
	"WORKER_CONTROL_PLANE_URL":      "http://executor-controller:8084",
}

// repoRoot walks up from the test's directory until it finds go.work.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.work not found above the test directory")
		}
		dir = parent
	}
}

func readYAML(t *testing.T, rel string, out any) {
	t.Helper()
	//nolint:gosec // G304: rel is repoRoot(t) (discovered by walking up for go.work) joined with a fixed literal manifest path, not external input
	body, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	if err := yaml.Unmarshal(body, out); err != nil {
		t.Fatalf("parse %s: %v", rel, err)
	}
}

// composeEnv reads the executor's environment from the Compose file, whose
// entries are "KEY=VALUE" strings.
func composeEnv(t *testing.T) map[string]string {
	t.Helper()
	// Other services in the file write environment as a map, so decoding is
	// deferred until the executor's own entry is in hand.
	var file struct {
		Services map[string]struct {
			Environment yaml.Node `yaml:"environment"`
		} `yaml:"services"`
	}
	readYAML(t, "docker-compose.yml", &file)

	executor := file.Services["executor-controller"]
	var entries []string
	if err := executor.Environment.Decode(&entries); err != nil {
		t.Fatalf("decode the executor's compose environment: %v", err)
	}

	env := map[string]string{}
	for _, entry := range entries {
		key, value, found := strings.Cut(entry, "=")
		if found {
			env[key] = value
		}
	}
	return env
}

// helmEnv reads the executor's env map from a chart's values.
func helmEnv(t *testing.T, rel string) map[string]string {
	t.Helper()
	var file struct {
		Services []struct {
			Name string            `yaml:"name"`
			Env  map[string]string `yaml:"env"`
		} `yaml:"services"`
	}
	readYAML(t, rel, &file)

	for _, svc := range file.Services {
		if svc.Name == "executor-controller" {
			return svc.Env
		}
	}
	t.Fatalf("%s has no executor-controller service", rel)
	return nil
}

// e2eEnv reads the executor container's env from the e2e manifest, which holds
// several documents and lists env as {name, value} pairs.
func e2eEnv(t *testing.T) map[string]string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(
		repoRoot(t), "tests/e2e/k8s/executor-controller-deployment.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	env := map[string]string{}
	for _, doc := range strings.Split(string(body), "\n---") {
		var manifest struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Env []struct {
								Name  string `yaml:"name"`
								Value string `yaml:"value"`
							} `yaml:"env"`
						} `yaml:"containers"`
					} `yaml:"spec"`
				} `yaml:"template"`
			} `yaml:"spec"`
		}
		if err := yaml.Unmarshal([]byte(doc), &manifest); err != nil {
			t.Fatalf("parse e2e manifest document: %v", err)
		}
		if manifest.Kind != "Deployment" || manifest.Metadata.Name != "executor-controller" {
			continue
		}
		for _, container := range manifest.Spec.Template.Spec.Containers {
			for _, e := range container.Env {
				env[e.Name] = e.Value
			}
		}
	}
	if len(env) == 0 {
		t.Fatal("the e2e manifest yielded no executor-controller environment")
	}
	return env
}

// deploymentPaths is every manifest the executor is deployed from.
func deploymentPaths(t *testing.T) map[string]map[string]string {
	t.Helper()
	return map[string]map[string]string{
		"docker-compose.yml":                                composeEnv(t),
		"deploy/app/values.yaml":                            helmEnv(t, "deploy/app/values.yaml"),
		"deploy/continuo/values.yaml":                       helmEnv(t, "deploy/continuo/values.yaml"),
		"tests/e2e/k8s/executor-controller-deployment.yaml": e2eEnv(t),
	}
}

// TestDeploymentConfig_EveryPathCarriesTheExecutorEnvironment pins that all four
// manifests configure the executor identically, by the names Load reads.
func TestDeploymentConfig_EveryPathCarriesTheExecutorEnvironment(t *testing.T) {
	for path, env := range deploymentPaths(t) {
		t.Run(path, func(t *testing.T) {
			for key, want := range wantedExecutorEnv {
				got, ok := env[key]
				if !ok {
					t.Errorf("%s does not set %s", path, key)
					continue
				}
				if got != want {
					t.Errorf("%s sets %s=%q, want %q", path, key, got, want)
				}
			}
		})
	}
}

// TestDeploymentConfig_DeployedValuesLoadAsIntended feeds each manifest's own
// values through Load. It is what proves the names and the values agree with the
// code: a variable Load does not read, or a duration it cannot parse, leaves the
// resulting Config on a default and fails here.
func TestDeploymentConfig_DeployedValuesLoadAsIntended(t *testing.T) {
	for path, env := range deploymentPaths(t) {
		t.Run(path, func(t *testing.T) {
			cfg, _ := loadEnv(t, env)

			if cfg.ExecutionMode != model.ExecutionModeJobs {
				t.Errorf("%s: want the jobs path, got %q", path, cfg.ExecutionMode)
			}
			if len(cfg.ExecutionModeOverrides) != 0 {
				t.Errorf("%s: want no service pinned to workers, got %v",
					path, cfg.ExecutionModeOverrides)
			}
			if cfg.MaxConcurrentExecutions != 50 {
				t.Errorf("%s: want 50 execution slots, got %d",
					path, cfg.MaxConcurrentExecutions)
			}
			if cfg.WorkerIdleTimeout != 300*time.Second {
				t.Errorf("%s: want a 300s idle timeout, got %s", path, cfg.WorkerIdleTimeout)
			}
			if cfg.WorkerLeaseTTL != 60*time.Second {
				t.Errorf("%s: want a 60s lease TTL, got %s", path, cfg.WorkerLeaseTTL)
			}
			if cfg.WorkerHeartbeatInterval != 15*time.Second {
				t.Errorf("%s: want a 15s heartbeat, got %s", path, cfg.WorkerHeartbeatInterval)
			}
			if cfg.WorkerClaimWait != 20*time.Second {
				t.Errorf("%s: want a 20s claim wait, got %s", path, cfg.WorkerClaimWait)
			}
			if cfg.WorkerControlPlaneURL != wantedExecutorEnv["WORKER_CONTROL_PLANE_URL"] {
				t.Errorf("%s: want the control plane URL, got %q",
					path, cfg.WorkerControlPlaneURL)
			}
		})
	}
}

// TestDeploymentConfig_NoPathBootsWithMissingVars pins that each manifest is
// complete: the executor started with exactly what it deploys reports nothing
// missing.
func TestDeploymentConfig_NoPathBootsWithMissingVars(t *testing.T) {
	for path, env := range deploymentPaths(t) {
		t.Run(path, func(t *testing.T) {
			// loadEnv supplies the infrastructure vars each manifest sources from
			// secrets and configMaps rather than literals.
			_, v := loadEnv(t, env)
			if missing := v.Missing(); len(missing) > 0 {
				t.Errorf("%s is missing %v", path, missing)
			}
		})
	}
}

// TestDeploymentConfig_NoPathShipsTheRetiredAlias pins that no manifest still
// sets MAX_CONCURRENT_JOBS. The alias remains readable for a deployment that has
// not caught up, but shipping it would let a path drift onto the old spelling
// unnoticed — which is exactly how a variable goes missing from one manifest.
func TestDeploymentConfig_NoPathShipsTheRetiredAlias(t *testing.T) {
	for path, env := range deploymentPaths(t) {
		t.Run(path, func(t *testing.T) {
			if _, ok := env["MAX_CONCURRENT_JOBS"]; ok {
				t.Errorf("%s still sets MAX_CONCURRENT_JOBS; it deploys "+
					"MAX_CONCURRENT_EXECUTIONS", path)
			}
		})
	}
}
