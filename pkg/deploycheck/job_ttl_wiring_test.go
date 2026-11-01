package deploycheck

import (
	"regexp"
	"strings"
	"testing"
)

// jobTTLKey is the environment key the chart's execution.jobTTLMinutes reaches
// execution-controller through, in seconds.
const jobTTLKey = "JOB_TTL_SECONDS"

// The finished-Job TTL reaches execution-controller on every deployment path,
// and its configuration reads it: the chart's shared ConfigMap (rendered from
// execution.jobTTLMinutes), the execution-controller service in compose, the
// e2e cluster's execution-controller Deployment and
// execution-controller/config/config.go. Its range and default are pinned to
// the chart's by TestJobTTLRangeAndDefaultMatchTheChart in
// execution-controller/config.
func TestJobTTLReachesEveryDeploymentPath(t *testing.T) {
	cm := readRepoFile(t, "deploy", "continuo", "templates", "configmap.yaml")
	if !strings.Contains(cm, "\n  "+jobTTLKey+`: {{ include "continuo.execution.jobTTLSeconds" . | quote }}`) {
		t.Errorf("templates/configmap.yaml does not set %s from execution.jobTTLMinutes", jobTTLKey)
	}
	compose := readRepoFile(t, "docker-compose.yml")
	block := regexp.MustCompile(`(?ms)^  execution-controller:\n(.*?)(?:^  \S|\z)`).FindStringSubmatch(compose)
	if block == nil || !strings.Contains(block[1], "- "+jobTTLKey+"=") {
		t.Errorf("docker-compose.yml service execution-controller does not set %s", jobTTLKey)
	}
	e2e := readRepoFile(t, "tests", "e2e", "k8s", "execution-controller-deployment.yaml")
	if !strings.Contains(e2e, "- name: "+jobTTLKey+"\n") {
		t.Errorf("tests/e2e/k8s/execution-controller-deployment.yaml does not set %s", jobTTLKey)
	}
	if !strings.Contains(readRepoFile(t, "execution-controller", "config", "config.go"), `"`+jobTTLKey+`"`) {
		t.Errorf("execution-controller/config/config.go does not read %s", jobTTLKey)
	}
}
