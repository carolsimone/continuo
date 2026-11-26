package deploycheck

import (
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The chart refuses a remediation file whose version agent-remediation does not
// load, so the versions continuo.agentRemediation.assertWorkflowFiles accepts
// are exactly the SupportedVersion of agent-remediation/domain/workflow. The
// constant is read from the source: pkg does not import agent-remediation.
func TestRemediationWorkflowFileVersionMatchesTheChart(t *testing.T) {
	src := readRepoFile(t, "agent-remediation", "domain", "workflow", "workflow.go")
	m := regexp.MustCompile(`(?m)^const SupportedVersion = (\d+)$`).FindStringSubmatch(src)
	if m == nil {
		t.Fatal("agent-remediation/domain/workflow/workflow.go declares no `const SupportedVersion = <n>`")
	}
	want := []string{m[1]}

	helpers := readRepoFile(t, "deploy", "continuo", "templates", "_helpers.tpl")
	_, body, found := strings.Cut(helpers, `define "continuo.agentRemediation.assertWorkflowFiles"`)
	if !found {
		t.Fatal(`templates/_helpers.tpl defines no "continuo.agentRemediation.assertWorkflowFiles"`)
	}
	body, _, _ = strings.Cut(body, "{{- define ")
	list := regexp.MustCompile(`\$supportedVersions := list ([0-9 ]+)`).FindStringSubmatch(body)
	if list == nil {
		t.Fatal(`continuo.agentRemediation.assertWorkflowFiles sets no "$supportedVersions := list <n> ..."`)
	}
	got := strings.Fields(list[1])
	if !slices.Equal(got, want) {
		t.Errorf("the chart accepts remediation file versions %v, agent-remediation loads %v", got, want)
	}
}

// The chart refuses a remediation file whose kind agent-remediation does not
// load, so the kinds continuo.agentRemediation.assertWorkflowFiles accepts are
// exactly the kind constants of agent-remediation/adapters/workflowfile
// (`kind<Name> = "<kind>"`). Both are read from the source by their shape, and
// the test fails when either shape is not found.
func TestRemediationWorkflowFileKindsMatchTheChart(t *testing.T) {
	src := readRepoFile(t, "agent-remediation", "adapters", "workflowfile", "file.go")
	var want []string
	for _, m := range regexp.MustCompile(`(?m)^\s*kind[A-Z][A-Za-z]* += "([a-z]+)"$`).FindAllStringSubmatch(src, -1) {
		want = append(want, m[1])
	}
	if len(want) == 0 {
		t.Fatal("agent-remediation/adapters/workflowfile/file.go declares no `kind<Name> = \"<kind>\"` constant")
	}

	helpers := readRepoFile(t, "deploy", "continuo", "templates", "_helpers.tpl")
	_, body, found := strings.Cut(helpers, `define "continuo.agentRemediation.assertWorkflowFiles"`)
	if !found {
		t.Fatal(`templates/_helpers.tpl defines no "continuo.agentRemediation.assertWorkflowFiles"`)
	}
	body, _, _ = strings.Cut(body, "{{- define ")
	list := regexp.MustCompile(`has \(toString \$doc\.kind\) \(list ((?:"[a-z]+" ?)+)\)`).FindStringSubmatch(body)
	if list == nil {
		t.Fatal(`continuo.agentRemediation.assertWorkflowFiles checks no "has (toString $doc.kind) (list \"<kind>\" ...)"`)
	}
	var got []string
	for _, m := range regexp.MustCompile(`"([a-z]+)"`).FindAllStringSubmatch(list[1], -1) {
		got = append(got, m[1])
	}

	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("the chart accepts remediation file kinds %v, agent-remediation loads %v", got, want)
	}
}

// workflowsValuesComponent is the part of a values.yaml components entry the
// workflows folder check reads.
type workflowsValuesComponent struct {
	Name         string            `yaml:"name"`
	Env          map[string]string `yaml:"env"`
	VolumeMounts []struct {
		Name      string `yaml:"name"`
		MountPath string `yaml:"mountPath"`
		ReadOnly  bool   `yaml:"readOnly"`
	} `yaml:"volumeMounts"`
	Volumes []workflowsValuesVolume `yaml:"volumes"`
}

type workflowsValuesVolume struct {
	Name      string `yaml:"name"`
	ConfigMap *struct {
		Name string `yaml:"name"`
	} `yaml:"configMap"`
}

// agent-remediation reads an install's remediation files from
// AGENT_REMEDIATION_WORKFLOWS_DIR, or from config.go's default when it is unset.
// Both deployment paths mount the files at that default and set the variable to
// it: the chart's agent-remediation components entry, whose volume the template
// resolves to the workflows ConfigMap, and the compose service. There is no
// Kubernetes manifest for agent-remediation under tests/e2e/k8s.
func TestRemediationWorkflowsDirMatchesTheChart(t *testing.T) {
	cfg := readRepoFile(t, "agent-remediation", "config", "config.go")
	m := regexp.MustCompile(`(?m)^const defaultWorkflowsDir = "([^"]+)"$`).FindStringSubmatch(cfg)
	if m == nil {
		t.Fatal("agent-remediation/config/config.go declares no `const defaultWorkflowsDir = \"<path>\"`")
	}
	want := m[1]

	var values struct {
		Components []workflowsValuesComponent `yaml:"components"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "values.yaml")), &values); err != nil {
		t.Fatalf("parse values.yaml: %v", err)
	}
	i := slices.IndexFunc(values.Components, func(s workflowsValuesComponent) bool { return s.Name == "agent-remediation" })
	if i < 0 {
		t.Fatal("values.yaml has no agent-remediation components entry")
	}
	svc := values.Components[i]
	if got := svc.Env["AGENT_REMEDIATION_WORKFLOWS_DIR"]; got != want {
		t.Errorf("values.yaml agent-remediation env AGENT_REMEDIATION_WORKFLOWS_DIR is %q, want config.go's default %q", got, want)
	}
	mounted := false
	for _, mount := range svc.VolumeMounts {
		if mount.Name == "remediation-workflows" {
			mounted = true
			if mount.MountPath != want || !mount.ReadOnly {
				t.Errorf("values.yaml agent-remediation mounts remediation-workflows at %q (readOnly %v), want %q read-only", mount.MountPath, mount.ReadOnly, want)
			}
		}
	}
	if !mounted {
		t.Error("values.yaml agent-remediation has no volumeMounts entry for remediation-workflows")
	}
	if !slices.ContainsFunc(svc.Volumes, func(v workflowsValuesVolume) bool {
		return v.Name == "remediation-workflows" && v.ConfigMap != nil && v.ConfigMap.Name == ""
	}) {
		t.Error(`values.yaml agent-remediation has no volume remediation-workflows with configMap: {name: ""}, which mounts the chart's workflows ConfigMap`)
	}
	deployment := readRepoFile(t, "deploy", "continuo", "templates", "deployment.yaml")
	if !strings.Contains(deployment, `"volume" "remediation-workflows" "name" "continuo.agentRemediationWorkflowsConfigMapName"`) {
		t.Error("templates/deployment.yaml does not resolve agent-remediation's remediation-workflows volume to continuo.agentRemediationWorkflowsConfigMapName")
	}

	var compose struct {
		Services map[string]struct {
			Environment composeEnvironment `yaml:"environment"`
			Volumes     []string           `yaml:"volumes"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, "docker-compose.yml")), &compose); err != nil {
		t.Fatalf("parse docker-compose.yml: %v", err)
	}
	agent, ok := compose.Services["agent-remediation"]
	if !ok {
		t.Fatal("docker-compose.yml has no agent-remediation service")
	}
	if got := agent.Environment["AGENT_REMEDIATION_WORKFLOWS_DIR"]; got != want {
		t.Errorf("docker-compose.yml agent-remediation sets AGENT_REMEDIATION_WORKFLOWS_DIR=%q, want config.go's default %q", got, want)
	}
	if !slices.ContainsFunc(agent.Volumes, func(v string) bool {
		parts := strings.Split(v, ":")
		return len(parts) == 3 && parts[1] == want && parts[2] == "ro"
	}) {
		t.Errorf("docker-compose.yml agent-remediation mounts no folder at %s read-only", want)
	}
}

// The loader skips every file whose name starts with a dot, so the schema
// refuses such a key: it would mount a file agent-remediation never reads. A
// key is also a ConfigMap data key, so it holds only letters, digits, '.', '_'
// and '-', and it ends in .yaml or .yml, the files the loader reads.
func TestRemediationWorkflowFileNamesTheSchemaAccepts(t *testing.T) {
	var schema struct {
		Properties struct {
			AgentRemediation struct {
				Properties struct {
					Workflows struct {
						AdditionalProperties *bool `json:"additionalProperties"`
						Properties           struct {
							Files struct {
								PropertyNames struct {
									Pattern string `json:"pattern"`
								} `json:"propertyNames"`
								AdditionalProperties struct {
									Type string `json:"type"`
								} `json:"additionalProperties"`
							} `json:"files"`
						} `json:"properties"`
					} `json:"workflows"`
				} `json:"properties"`
			} `json:"agentRemediation"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(readRepoFile(t, "deploy", "continuo", "values.schema.json")), &schema); err != nil {
		t.Fatalf("parse values.schema.json: %v", err)
	}
	workflows := schema.Properties.AgentRemediation.Properties.Workflows
	if workflows.AdditionalProperties == nil || *workflows.AdditionalProperties {
		t.Error("values.schema.json agentRemediation.workflows must set additionalProperties: false")
	}
	files := workflows.Properties.Files
	if files.AdditionalProperties.Type != "string" {
		t.Errorf("values.schema.json agentRemediation.workflows.files values have type %q, want string", files.AdditionalProperties.Type)
	}
	pattern := files.PropertyNames.Pattern
	if pattern == "" {
		t.Fatal("values.schema.json agentRemediation.workflows.files has no propertyNames pattern")
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("agentRemediation.workflows.files propertyNames pattern %q: %v", pattern, err)
	}
	for _, name := range []string{"dbt-validation.yaml", "a.yml", "types.dbt_x-1.yaml", "rejected-release.yaml"} {
		if !re.MatchString(name) {
			t.Errorf("pattern %q refuses %q, a file name agent-remediation loads", pattern, name)
		}
	}
	for _, name := range []string{".hidden.yaml", "..data", ".yaml", "a.json", "a.yaml.bak", "a/b.yaml", "/a.yaml", " a.yaml", "a b.yaml"} {
		if re.MatchString(name) {
			t.Errorf("pattern %q accepts %s, which agent-remediation skips or a ConfigMap cannot hold", pattern, strconv.Quote(name))
		}
	}
}
