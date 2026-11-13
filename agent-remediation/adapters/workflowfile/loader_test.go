package workflowfile

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

const flowYAML = `kind: flow
version: 1
name: rejected-release
trigger: remediation.requested
types: [dbt-compile, dbt-parse]
context: [failing_node_source]
across_clusters: [edits_map_to_a_service]
gate: [verification_run]
tries: 3
output:
  on_pass:    { action: pull_request, emits: remediation.proposed }
  on_give_up: { action: hand_over }
`

const typeYAML = `kind: type
version: 1
name: dbt-compile
match: { cluster: single, source: compile, node_types: [model, none] }
context: [failing_node_source, dbt_log]
agent: { prompt: dbt-compile, tools: [], rounds: 2 }
preflight: [target_file_was_shown, content_changed]
`

func TestLoad_ReadsAFlowAndATypeFromOneFolder(t *testing.T) {
	fsys := fstest.MapFS{
		"flows/rejected-release.yaml": {Data: []byte(flowYAML)},
		"types/dbt-compile.yaml":      {Data: []byte(typeYAML)},
		"types/notes.txt":             {Data: []byte("not a yaml file")},
	}

	set, err := Load(fsys)
	require.NoError(t, err)

	flow, ok := set.Flows["rejected-release"]
	require.True(t, ok)
	assert.Equal(t, 1, flow.Version)
	assert.Equal(t, "remediation.requested", flow.Trigger)
	assert.Equal(t, []string{"dbt-compile", "dbt-parse"}, flow.Types)
	assert.Equal(t, 3, flow.Tries)
	assert.Equal(t, "flows/rejected-release.yaml", flow.File)
	assert.Equal(t, workflow.Pos{File: "flows/rejected-release.yaml", Line: 1}, flow.Pos)
	assert.Equal(t, workflow.Pos{File: "flows/rejected-release.yaml", Line: 5}, flow.TypesPos)
	assert.Equal(t, []workflow.StepRef{{Name: "failing_node_source", Pos: workflow.Pos{File: "flows/rejected-release.yaml", Line: 6}}}, flow.Context)
	assert.Equal(t, workflow.StepName("edits_map_to_a_service"), flow.AcrossClusters[0].Name)
	assert.Equal(t, workflow.StepName("verification_run"), flow.Gate[0].Name)
	assert.Equal(t, workflow.OutputPullRequest, flow.OnPass.Action)
	assert.Equal(t, "remediation.proposed", flow.OnPass.Emits)
	assert.Equal(t, 11, flow.OnPass.Pos.Line)
	assert.Equal(t, workflow.OutputHandOver, flow.OnGiveUp.Action)
	assert.Empty(t, flow.OnGiveUp.Emits)

	typ, ok := set.Types["dbt-compile"]
	require.True(t, ok)
	assert.Equal(t, workflow.ClusterSingle, typ.Match.Cluster)
	assert.Equal(t, "compile", typ.Match.Source)
	assert.Equal(t, []string{"model", "none"}, typ.Match.NodeTypes)
	assert.Equal(t, workflow.StepName("dbt-compile"), typ.Agent.Prompt.Name)
	assert.Equal(t, 6, typ.Agent.Prompt.Pos.Line)
	assert.Empty(t, typ.Agent.Tools)
	assert.Equal(t, 2, typ.Agent.Rounds)
	assert.Equal(t, "types/dbt-compile.yaml", typ.File)
	require.Len(t, typ.Preflight, 2)
	assert.Equal(t, 7, typ.Preflight[1].Pos.Line)
}

func TestLoad_RevisionIsThePrefixOfTheFilesSha256(t *testing.T) {
	fsys := fstest.MapFS{"t.yaml": {Data: []byte("kind: type\nname: x\n")}}
	set, err := Load(fsys)
	require.NoError(t, err)
	assert.Len(t, set.Types["x"].Revision, 12)

	other := fstest.MapFS{"t.yaml": {Data: []byte("kind: type\nname: x\n# edited\n")}}
	set2, err := Load(other)
	require.NoError(t, err)
	assert.NotEqual(t, set.Types["x"].Revision, set2.Types["x"].Revision)
	assert.Regexp(t, `^[0-9a-f]{12}$`, set.Types["x"].Revision)
}

func TestLoad_AbsentNodeTypesMatchesAnyAndAnEmptyListIsKeptEmpty(t *testing.T) {
	fsys := fstest.MapFS{
		"a.yaml": {Data: []byte("kind: type\nname: any\nmatch: { cluster: single }\n")},
		"b.yaml": {Data: []byte("kind: type\nname: none\nmatch: { cluster: single, node_types: [] }\n")},
	}
	set, err := Load(fsys)
	require.NoError(t, err)
	assert.Nil(t, set.Types["any"].Match.NodeTypes)
	assert.NotNil(t, set.Types["none"].Match.NodeTypes)
	assert.Empty(t, set.Types["none"].Match.NodeTypes)
}

func TestLoad_ReadsSeveralDocumentsOfOneFileAndSkipsFixtures(t *testing.T) {
	fsys := fstest.MapFS{
		"all.yaml": {Data: []byte(flowYAML + "---\n" + typeYAML + "---\nkind: fixture\nanything: goes\nfor: [a, b]\n")},
	}

	set, err := Load(fsys)
	require.NoError(t, err)
	assert.Len(t, set.Flows, 1)
	require.Len(t, set.Types, 1)
	assert.Equal(t, 14, set.Types["dbt-compile"].Pos.Line)
	assert.Equal(t, set.Flows["rejected-release"].Revision, set.Types["dbt-compile"].Revision)
}

func TestLoad_NamesTheFileAndLineOfAnUnknownField(t *testing.T) {
	fsys := fstest.MapFS{
		"types/x.yaml": {Data: []byte("kind: type\nversion: 1\nname: x\nagent:\n  rouds: 3\n")},
	}
	_, err := Load(fsys)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "types/x.yaml:5")
	assert.Contains(t, err.Error(), "rouds")
}

func TestLoad_RefusesAnUnknownFieldAtTheTopOfAFlow(t *testing.T) {
	fsys := fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\nname: f\nretries: 3\n")}}
	_, err := Load(fsys)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "f.yaml:3")
}

func TestLoad_RefusesAFileWithNoKind(t *testing.T) {
	fsys := fstest.MapFS{"x.yaml": {Data: []byte("name: x\nversion: 1\n")}}
	_, err := Load(fsys)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "x.yaml:1")
	assert.Contains(t, err.Error(), "kind")
}

func TestLoad_RefusesAnUnknownKind(t *testing.T) {
	fsys := fstest.MapFS{"x.yaml": {Data: []byte("kind: workflow\nname: x\n")}}
	_, err := Load(fsys)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "x.yaml:1")
	assert.Contains(t, err.Error(), "workflow")
}

func TestLoad_RefusesTwoFilesThatDefineTheSameName(t *testing.T) {
	fsys := fstest.MapFS{
		"a/one.yaml": {Data: []byte("kind: type\nname: dup\n")},
		"b/two.yaml": {Data: []byte("kind: type\nname: dup\n")},
	}
	_, err := Load(fsys)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a/one.yaml")
	assert.Contains(t, err.Error(), "b/two.yaml:1")
	assert.Contains(t, err.Error(), `"dup"`)
}

func TestLoad_NamesTheLineOfAWrongValueType(t *testing.T) {
	fsys := fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\nname: f\ntries: many\n")}}
	_, err := Load(fsys)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "f.yaml:3")
}

func TestLoad_RefusesANonScalarStepReference(t *testing.T) {
	fsys := fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\nname: f\ngate: [{a: b}]\n")}}
	_, err := Load(fsys)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "f.yaml:3")
}

func TestLoad_RefusesAFileThatIsNotYAML(t *testing.T) {
	fsys := fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\n\tname: [\n")}}
	_, err := Load(fsys)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "f.yaml")
}
