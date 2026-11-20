package workflowfile

import "gopkg.in/yaml.v3"

// Document kinds a file's `kind` field may hold.
const (
	kindFlow    = "flow"
	kindType    = "type"
	kindFixture = "fixture"
)

// Allowed keys of every mapping a flow or type document holds. A key outside
// these sets is refused with its line.
var (
	flowKeys      = []string{"kind", "version", "name", "trigger", "types", "context", "across_clusters", "gate", "tries", "output", "model_may_see", "approver", "based_on"}
	outputKeys    = []string{"on_pass", "on_give_up"}
	outputOneKeys = []string{"action", "emits"}
	typeKeys      = []string{"kind", "version", "name", "match", "context", "agent", "preflight", "based_on"}
	matchKeys     = []string{"cluster", "source", "node_types"}
	agentKeys     = []string{"prompt", "tools", "rounds"}
)

// flowFile is the shape of a `kind: flow` document. Fields that keep a
// position or distinguish "absent" from "empty" are yaml.Node values.
type flowFile struct {
	Kind          string      `yaml:"kind"`
	Version       int         `yaml:"version"`
	Name          string      `yaml:"name"`
	Trigger       string      `yaml:"trigger"`
	Types         yaml.Node   `yaml:"types"`
	Context       []yaml.Node `yaml:"context"`
	AcrossCluster []yaml.Node `yaml:"across_clusters"`
	Gate          []yaml.Node `yaml:"gate"`
	Tries         int         `yaml:"tries"`
	Output        outputFile  `yaml:"output"`
	ModelMaySee   yaml.Node   `yaml:"model_may_see"`
	Approver      yaml.Node   `yaml:"approver"`
	BasedOn       yaml.Node   `yaml:"based_on"`
}

type outputFile struct {
	OnPass   outputOneFile `yaml:"on_pass"`
	OnGiveUp outputOneFile `yaml:"on_give_up"`
}

type outputOneFile struct {
	Action string `yaml:"action"`
	Emits  string `yaml:"emits"`
}

// typeFile is the shape of a `kind: type` document.
type typeFile struct {
	Kind      string      `yaml:"kind"`
	Version   int         `yaml:"version"`
	Name      string      `yaml:"name"`
	Match     matchFile   `yaml:"match"`
	Context   []yaml.Node `yaml:"context"`
	Agent     agentFile   `yaml:"agent"`
	Preflight []yaml.Node `yaml:"preflight"`
	BasedOn   yaml.Node   `yaml:"based_on"`
}

type matchFile struct {
	Cluster   string    `yaml:"cluster"`
	Source    string    `yaml:"source"`
	NodeTypes yaml.Node `yaml:"node_types"`
}

type agentFile struct {
	Prompt string      `yaml:"prompt"`
	Tools  []yaml.Node `yaml:"tools"`
	Rounds int         `yaml:"rounds"`
}
