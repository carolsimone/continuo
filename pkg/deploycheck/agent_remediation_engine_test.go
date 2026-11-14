package deploycheck

import (
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// acceptedAgentRemediationEngines are the values agent-remediation's
// configuration accepts for AGENT_REMEDIATION_ENGINE, read from the constants
// in its config.go.
func acceptedAgentRemediationEngines(t *testing.T) []string {
	t.Helper()
	cfg := readRepoFile(t, "agent-remediation", "config", "config.go")
	var engines []string
	for _, m := range regexp.MustCompile(`(?m)^\tEngine[A-Za-z]+\s*=\s*"([a-z]+)"$`).FindAllStringSubmatch(cfg, -1) {
		engines = append(engines, m[1])
	}
	if len(engines) == 0 {
		t.Fatal("agent-remediation/config/config.go declares no Engine constant; the check would pass vacuously")
	}
	slices.Sort(engines)
	return engines
}

// agentRemediation.engine reaches agent-remediation on every deployment path,
// and each path names only an engine its configuration accepts: the chart's
// shared ConfigMap, the agent-remediation service in compose, the chart's
// values default and schema, and agent-remediation/config/config.go. There is
// no Kubernetes manifest for agent-remediation under tests/e2e/k8s, so compose
// and the chart are the paths.
func TestAgentRemediationEngineMatchesTheChart(t *testing.T) {
	accepted := acceptedAgentRemediationEngines(t)

	cfg := readRepoFile(t, "agent-remediation", "config", "config.go")
	if !strings.Contains(cfg, `"AGENT_REMEDIATION_ENGINE"`) {
		t.Error("agent-remediation/config/config.go does not read AGENT_REMEDIATION_ENGINE")
	}

	cm := readRepoFile(t, "deploy", "continuo", "templates", "configmap.yaml")
	line := regexp.MustCompile(`(?m)^  AGENT_REMEDIATION_ENGINE: \{\{ dig "engine" "([a-z]+)" \(default \(dict\) \.Values\.agentRemediation\) \| quote \}\}$`).FindStringSubmatch(cm)
	if line == nil {
		t.Error(`templates/configmap.yaml does not set AGENT_REMEDIATION_ENGINE from agentRemediation.engine with: dig "engine" "<default>" (default (dict) .Values.agentRemediation) | quote`)
	} else if !slices.Contains(accepted, line[1]) {
		t.Errorf("templates/configmap.yaml defaults AGENT_REMEDIATION_ENGINE to %q, which config.go does not accept (%v)", line[1], accepted)
	}

	values := readRepoFile(t, "deploy", "continuo", "values.yaml")
	def := regexp.MustCompile(`(?m)^agentRemediation:\n(?:  #.*\n)*  engine: ([a-z]+)\b`).FindStringSubmatch(values)
	switch {
	case def == nil:
		t.Error("values.yaml has no agentRemediation.engine")
	case def[1] != "legacy":
		t.Errorf("values.yaml agentRemediation.engine defaults to %q: an install that sets nothing keeps the legacy engine", def[1])
	}

	var schema struct {
		Properties struct {
			AgentRemediation struct {
				AdditionalProperties bool `json:"additionalProperties"`
				Properties           struct {
					Engine struct {
						Enum []string `json:"enum"`
					} `json:"engine"`
				} `json:"properties"`
			} `json:"agentRemediation"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "values.schema.json")), &schema); err != nil {
		t.Fatalf("parse values.schema.json: %v", err)
	}
	got := slices.Clone(schema.Properties.AgentRemediation.Properties.Engine.Enum)
	slices.Sort(got)
	if !slices.Equal(got, accepted) {
		t.Errorf("values.schema.json agentRemediation.engine enum is %v, want the values config.go accepts: %v", got, accepted)
	}

	compose := readRepoFile(t, "docker-compose.yml")
	block := regexp.MustCompile(`(?ms)^  agent-remediation:\n(.*?)(?:^  \S|\z)`).FindStringSubmatch(compose)
	if block == nil {
		t.Fatal("docker-compose.yml has no agent-remediation service")
	}
	env := regexp.MustCompile(`(?m)^      - AGENT_REMEDIATION_ENGINE=([a-z]+)$`).FindStringSubmatch(block[1])
	switch {
	case env == nil:
		t.Error("docker-compose.yml service agent-remediation does not set AGENT_REMEDIATION_ENGINE")
	case !slices.Contains(accepted, env[1]):
		t.Errorf("docker-compose.yml sets AGENT_REMEDIATION_ENGINE=%s, which config.go does not accept (%v)", env[1], accepted)
	}
}
