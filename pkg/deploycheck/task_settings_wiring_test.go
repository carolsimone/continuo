package deploycheck

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// The task settings reach every process that acts on them: the chart's shared
// ConfigMap, run-controller and execution-controller in compose, and
// execution-controller in the e2e cluster (run-controller runs in compose
// there).
func TestTaskSettingsReachEveryDeploymentPath(t *testing.T) {
	cm := readRepoFile(t, "deploy", "continuo", "templates", "configmap.yaml")
	for _, key := range []string{"TASK_MAX_RETRIES", pkgconfig.TaskTimeoutEnvKey} {
		if !strings.Contains(cm, key+":") {
			t.Errorf("templates/configmap.yaml does not set %s", key)
		}
	}
	compose := readRepoFile(t, "docker-compose.yml")
	for dir, keys := range map[string][]string{
		"run-controller":       {"TASK_MAX_RETRIES", pkgconfig.TaskTimeoutEnvKey},
		"execution-controller": {pkgconfig.TaskTimeoutEnvKey},
	} {
		svc := composeServiceName(dir)
		block := regexp.MustCompile(`(?ms)^  ` + regexp.QuoteMeta(svc) + `:\n(.*?)(?:^  \S|\z)`).FindStringSubmatch(compose)
		for _, key := range keys {
			if block == nil || !strings.Contains(block[1], key+"=") {
				t.Errorf("docker-compose.yml service %q does not set %s", svc, key)
			}
		}
	}
	e2e := readRepoFile(t, "tests", "e2e", "k8s", "execution-controller-deployment.yaml")
	if !strings.Contains(e2e, "name: "+pkgconfig.TaskTimeoutEnvKey) {
		t.Errorf("tests/e2e/k8s/execution-controller-deployment.yaml does not set %s", pkgconfig.TaskTimeoutEnvKey)
	}
}

// The chart's runs.taskTimeoutHours is the range the services accept, in
// hours: a value the chart allows but a service refused would render and then
// stop the service at startup.
func TestTaskTimeoutRangeMatchesTheChart(t *testing.T) {
	var schema struct {
		Properties struct {
			Runs struct {
				Properties struct {
					TaskTimeoutHours struct {
						Minimum int `json:"minimum"`
						Maximum int `json:"maximum"`
					} `json:"taskTimeoutHours"`
				} `json:"properties"`
			} `json:"runs"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "values.schema.json")), &schema); err != nil {
		t.Fatal(err)
	}
	got := schema.Properties.Runs.Properties.TaskTimeoutHours
	if min := time.Duration(got.Minimum) * time.Hour; min != pkgconfig.MinTaskTimeout {
		t.Errorf("runs.taskTimeoutHours minimum = %v, the services accept from %v", min, pkgconfig.MinTaskTimeout)
	}
	if max := time.Duration(got.Maximum) * time.Hour; max != pkgconfig.MaxTaskTimeout {
		t.Errorf("runs.taskTimeoutHours maximum = %v, the services accept up to %v", max, pkgconfig.MaxTaskTimeout)
	}
}

// Both services read the task timeout through the shared loader, so they
// accept the same range.
func TestTaskTimeoutServicesLoadItThroughPkgConfig(t *testing.T) {
	for _, svc := range []string{"run-controller", "execution-controller"} {
		if !strings.Contains(readRepoFile(t, svc, "config", "config.go"), "pkgconfig.LoadTaskTimeout(v)") {
			t.Errorf("%s/config/config.go does not read %s through pkgconfig.LoadTaskTimeout", svc, pkgconfig.TaskTimeoutEnvKey)
		}
	}
}
