package workflowfile

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
)

const flowYAML = `kind: flow
version: 1
name: rejected-release
trigger: heal.triggered
types: [dbt-compile, dbt-parse]
context: [failing_node_source]
across_clusters: [edits_map_to_a_project]
gate: [verification_run]
tries: 3
output:
  on_pass:    { action: pull_request, emits: remediation.proposed }
  on_give_up: { action: hand_over }
model_may_see: [code, logs]
approver: node_owner
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
	assert.Equal(t, "heal.triggered", flow.Trigger)
	assert.Equal(t, []string{"dbt-compile", "dbt-parse"}, flow.Types)
	assert.Equal(t, 3, flow.Tries)
	assert.Equal(t, "flows/rejected-release.yaml", flow.File)
	assert.Equal(t, workflow.Pos{File: "flows/rejected-release.yaml", Line: 1}, flow.Pos)
	assert.Equal(t, workflow.Pos{File: "flows/rejected-release.yaml", Line: 5}, flow.TypesPos)
	assert.Equal(t, []workflow.StepRef{{Name: "failing_node_source", Pos: workflow.Pos{File: "flows/rejected-release.yaml", Line: 6}}}, flow.Context)
	assert.Equal(t, workflow.StepName("edits_map_to_a_project"), flow.AcrossClusters[0].Name)
	assert.Equal(t, workflow.StepName("verification_run"), flow.Gate[0].Name)
	assert.Equal(t, workflow.OutputPullRequest, flow.OnPass.Action)
	assert.Equal(t, "remediation.proposed", flow.OnPass.Emits)
	assert.Equal(t, 11, flow.OnPass.Pos.Line)
	assert.Equal(t, workflow.OutputHandOver, flow.OnGiveUp.Action)
	assert.Empty(t, flow.OnGiveUp.Emits)
	assert.Equal(t, []workflow.DataClass{workflow.ClassCode, workflow.ClassLogs}, flow.ModelMaySee)
	assert.Equal(t, workflow.Pos{File: "flows/rejected-release.yaml", Line: 13}, flow.ModelMaySeePos)
	assert.Equal(t, workflow.StepRef{Name: "node_owner", Pos: workflow.Pos{File: "flows/rejected-release.yaml", Line: 14}}, flow.Approver)

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
	assert.Equal(t, 16, set.Types["dbt-compile"].Pos.Line)
	assert.Equal(t, set.Flows["rejected-release"].Revision, set.Types["dbt-compile"].Revision)
}

// How the loader reads each way a file can place its `---` markers, blank
// lines and comments. Text before the first marker is no document when it
// holds only blank lines and comments, and a file of such text alone loads
// nothing. Every marker starts a document, so a marker followed by nothing,
// by blank lines and comments only, or by another marker is an empty document,
// which is refused at its line. The chart's continuo.agentRemediation.
// assertWorkflowFiles makes the same decision for every shape, and
// scripts/install-test/lint.sh renders each of them (fixtures under
// scripts/install-test/fixtures/remediation-workflows/empty-documents/).
func TestLoad_EmptyDocuments(t *testing.T) {
	const doc = "kind: type\nversion: 1\nname: probe\nmatch: { cluster: single, source: compile }\n"
	const doc2 = "kind: type\nversion: 1\nname: probe-2\nmatch: { cluster: single, source: compile }\n"
	for _, tc := range []struct {
		name, data string
		// refusedAt is the line the refusal names, 0 when the file loads.
		refusedAt int
	}{
		{name: "a leading marker", data: "---\n" + doc},
		{name: "a leading marker with a comment", data: "--- # the probe\n" + doc},
		{name: "comments and blank lines, then a marker", data: "# the probe\n\n---\n" + doc},
		{name: "two documents", data: doc + "---\n" + doc2},
		{name: "a document end marker", data: doc + "...\n"},
		{name: "a document end marker, then comments", data: doc + "...\n# done\n"},
		{name: "a document end marker, then a document", data: doc + "...\n---\n" + doc2},
		{name: "an empty file", data: ""},
		{name: "a file of comments", data: "# nothing here\n# yet\n"},
		{name: "a file of blank lines", data: "\n\n"},
		{name: "a trailing marker", data: doc + "---\n", refusedAt: 6},
		{name: "a trailing marker with no newline", data: doc + "---", refusedAt: 6},
		{name: "a trailing marker with blanks after it", data: doc + "---   \n", refusedAt: 6},
		{name: "a trailing marker with CRLF line ends", data: strings.ReplaceAll(doc, "\n", "\r\n") + "---\r\n", refusedAt: 6},
		{name: "a marker followed by comments", data: doc + "---\n# only a comment\n", refusedAt: 7},
		{name: "a marker followed by blank lines", data: doc + "---\n\n\n", refusedAt: 8},
		{name: "a marker with a comment, then nothing", data: doc + "--- # nothing\n", refusedAt: 6},
		{name: "two leading markers", data: "---\n---\n" + doc, refusedAt: 2},
		{name: "two markers between documents", data: doc + "---\n---\n" + doc2, refusedAt: 6},
		{name: "a file of one marker", data: "---\n", refusedAt: 2},
		{name: "comments, then a marker", data: "# the probe\n---\n", refusedAt: 3},
		{name: "a document end marker, then a marker", data: doc + "...\n---\n", refusedAt: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(fstest.MapFS{"f.yaml": {Data: []byte(tc.data)}})

			if tc.refusedAt == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, "f.yaml:"+strconv.Itoa(tc.refusedAt)+": a document must be a mapping with a kind", err.Error())
		})
	}
}

// How the loader reads YAML directives (`%YAML`, `%TAG`) and `...` end
// markers. A directive line belongs to the document the next `---` marker
// starts: before the first marker, after a `...` end marker, or after a
// document, so a directive that no marker follows, or that text follows before
// a marker, is refused. Only `%YAML 1.1` is a version the loader reads. After
// a `...` end marker only blank lines, comments and directives may come before
// the next marker, and a `...` before any document is refused. A line that
// starts with % inside a quoted string is text. The chart's
// continuo.agentRemediation.assertWorkflowFiles makes the same decision for
// every shape, and scripts/install-test/lint.sh renders each of them (fixtures
// under scripts/install-test/fixtures/remediation-workflows/directives/).
func TestLoad_Directives(t *testing.T) {
	const doc = "kind: type\nversion: 1\nname: probe\nmatch: { cluster: single, source: compile }\n"
	const doc2 = "kind: type\nversion: 1\nname: probe-2\nmatch: { cluster: single, source: compile }\n"
	for _, tc := range []struct {
		name, data string
		// refused is the message of the refusal, "" when the file loads.
		refused string
	}{
		{name: "%YAML 1.1, a marker, a document", data: "%YAML 1.1\n---\n" + doc},
		{name: "%YAML 1.1 with a comment, a marker, a document", data: "%YAML 1.1 # the version\n---\n" + doc},
		{name: "%TAG, a marker, a document", data: "%TAG !e! tag:example.com,2000:\n---\n" + doc},
		{name: "%YAML and %TAG, a marker, a document", data: "%YAML 1.1\n%TAG !e! tag:example.com,2000:\n---\n" + doc},
		{name: "comments, a directive, comments, a marker, a document", data: "# the probe\n\n%YAML 1.1\n# more\n\n---\n" + doc},
		{name: "a directive with CRLF line ends", data: "%YAML 1.1\r\n---\r\n" + strings.ReplaceAll(doc, "\n", "\r\n")},
		{name: "a directive after an end marker, then a marker", data: doc + "...\n%YAML 1.1\n---\n" + doc2},
		{name: "a directive after a document with no end marker, then a marker", data: doc + "%YAML 1.1\n---\n" + doc2},
		{name: "an end marker with a comment, then a marker", data: doc + "... # end\n---\n" + doc2},
		{name: "two end markers, then a marker", data: doc + "...\n...\n---\n" + doc2},
		{name: "a quoted string continued on a line that starts with %", data: "kind: type\nversion: 1\nname: \"probe\n%x\"\nmatch: { cluster: single, source: compile }\n"},
		{name: "%YAML 1.2", data: "%YAML 1.2\n---\n" + doc, refused: "f.yaml:1: found incompatible YAML document"},
		{name: "%YAML 1.2 before a second document", data: doc + "...\n%YAML 1.2\n---\n" + doc2, refused: "f.yaml:5: found incompatible YAML document"},
		{name: "two %YAML directives", data: "%YAML 1.1\n%YAML 1.1\n---\n" + doc, refused: "f.yaml:1: found duplicate %YAML directive"},
		{name: "two %TAG directives of one handle", data: "%TAG !e! tag:a,2000:\n%TAG !e! tag:b,2000:\n---\n" + doc, refused: "f.yaml:1: found duplicate %TAG directive"},
		{name: "an unknown directive", data: "%FOO bar\n---\n" + doc, refused: "f.yaml:1: found unknown directive name"},
		{name: "a directive, then a document with no marker", data: "%YAML 1.1\n" + doc, refused: "f.yaml:2: mapping values are not allowed in this context"},
		{name: "a directive alone", data: "%YAML 1.1\n", refused: "f.yaml:1: did not find expected <document start>"},
		{name: "a directive and a comment, then nothing", data: "%YAML 1.1\n# c\n", refused: "f.yaml:2: did not find expected <document start>"},
		{name: "a directive and a marker, then nothing", data: "%YAML 1.1\n---\n", refused: "f.yaml:3: a document must be a mapping with a kind"},
		{name: "a directive at the end of the file after a document", data: doc + "%YAML 1.1\n", refused: "f.yaml:5: did not find expected <document start>"},
		{name: "a directive after an end marker, then nothing", data: doc + "...\n%YAML 1.1\n", refused: "f.yaml:6: did not find expected <document start>"},
		{name: "a directive right after a marker", data: doc + "---\n%YAML 1.1\n---\n" + doc2, refused: "f.yaml:6: a document must be a mapping with a kind"},
		{name: "a directive, then an end marker, then a marker", data: "%YAML 1.1\n...\n---\n" + doc, refused: "f.yaml:1: did not find expected <document start>"},
		{name: "a document after an end marker with no marker", data: doc + "...\n" + doc2, refused: "f.yaml:5: did not find expected <document start>"},
		{name: "text after an end marker on its line", data: doc + "... x\n---\n" + doc2, refused: "f.yaml:4: did not find expected <document start>"},
		{name: "an end marker before any document", data: "...\n---\n" + doc, refused: "f.yaml:1: did not find expected node content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(fstest.MapFS{"f.yaml": {Data: []byte(tc.data)}})

			if tc.refused == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.refused, err.Error())
		})
	}
}

// chartFixtures is the folder of the remediation files the chart's lint
// (scripts/install-test/lint.sh) renders.
const chartFixtures = "../../../scripts/install-test/fixtures/remediation-workflows"

// The chart refuses at render time what the loader refuses at startup: each
// file the lint renders as accepted-* loads, as flows and types and as
// fixtures, and the component's loader refuses each refused-* file, for every
// folder of shapes the chart reads line by line (markers, end markers,
// directives, duplicate kind and version lines).
func TestLoad_AgreesWithTheChartsFixtures(t *testing.T) {
	for _, folder := range []string{"empty-documents", "directives", "duplicate-keys"} {
		entries, err := os.ReadDir(filepath.Join(chartFixtures, folder))
		require.NoError(t, err)
		require.NotEmpty(t, entries, folder)
		for _, e := range entries {
			name := e.Name()
			t.Run(folder+"/"+name, func(t *testing.T) {
				data, err := os.ReadFile(filepath.Join(chartFixtures, folder, name)) //nolint:gosec // G304: a fixture file of the chart's lint in this repository.
				require.NoError(t, err)
				fsys := fstest.MapFS{name: {Data: data}}
				_, loadErr := Load(fsys)
				_, fixtureErr := LoadFixtures(fsys)
				switch {
				case strings.HasPrefix(name, "accepted-"):
					assert.NoError(t, loadErr)
					assert.NoError(t, fixtureErr)
				case strings.HasPrefix(name, "refused-"):
					assert.Error(t, loadErr)
				default:
					t.Fatalf("%s is neither accepted-* nor refused-*", name)
				}
			})
		}
	}
}

// A key that one mapping holds twice is refused wherever it is: at the top of
// a flow or type, in a nested mapping, in flow style, and in a fixture, which
// LoadFixtures reads. Helm's fromYaml keeps the last of two keys, so the chart
// can check only the lines it sees (duplicate top-level kind and version
// lines) and leaves every other duplicate to this loader.
func TestLoad_RefusesADuplicateKey(t *testing.T) {
	for _, tc := range []struct {
		name, data, refused string
	}{
		{name: "version twice", data: "version: 2\nkind: type\nversion: 1\nname: probe\n",
			refused: `f.yaml:3: mapping key "version" already defined at line 1`},
		{name: "kind twice", data: "kind: type\nversion: 1\nkind: type\nname: probe\n",
			refused: `f.yaml:3: mapping key "kind" already defined at line 1`},
		{name: "a nested key twice", data: "kind: type\nversion: 1\nname: probe\nmatch:\n  cluster: single\n  cluster: pair\n",
			refused: `f.yaml:6: mapping key "cluster" already defined at line 5`},
		{name: "a key twice in flow style", data: "kind: type\nversion: 1\nname: probe\nmatch: { cluster: single, cluster: pair }\n",
			refused: `f.yaml:4: mapping key "cluster" already defined at line 4`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(fstest.MapFS{"f.yaml": {Data: []byte(tc.data)}})

			require.Error(t, err)
			assert.Equal(t, tc.refused, err.Error())
		})
	}
	t.Run("version twice in a fixture", func(t *testing.T) {
		_, err := LoadFixtures(fstest.MapFS{"f.yaml": {Data: []byte("kind: fixture\nversion: 1\nversion: 1\ntype: dbt-compile\n")}})

		require.Error(t, err)
		assert.Contains(t, err.Error(), `f.yaml:3: mapping key "version" already defined at line 2`)
	})
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
	assert.Contains(t, err.Error(), "f.yaml:2")
}

func TestLoad_RefusesAnAliasThatWouldHideUnknownFields(t *testing.T) {
	cases := map[string]string{
		"alias value of a mapping": "kind: type\nname: x\nmatch: &m {cluster: single, source: compile}\nagent: *m\n",
		"alias in the other order": "kind: type\nname: x\nagent: &a {prompt: p, rounds: 1}\nmatch: *a\n",
		"alias in a list":          "kind: type\nname: x\ncontext: [&c a, *c]\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(fstest.MapFS{"x.yaml": {Data: []byte(body)}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "alias")
			assert.Regexp(t, `x\.yaml:[0-9]+:`, err.Error())
		})
	}
}

func TestLoad_RefusesAMergeKey(t *testing.T) {
	t.Run("from an alias", func(t *testing.T) {
		body := "kind: type\nname: x\nbase: &b {rouds: 1}\nagent:\n  <<: *b\n"
		_, err := Load(fstest.MapFS{"x.yaml": {Data: []byte(body)}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "x.yaml:")
	})
	t.Run("from an inline mapping", func(t *testing.T) {
		body := "kind: type\nname: x\nagent:\n  <<: {rouds: 1}\n"
		_, err := Load(fstest.MapFS{"x.yaml": {Data: []byte(body)}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "x.yaml:4")
	})
}

func TestLoad_RefusesATypeOrFlowWithNoName(t *testing.T) {
	for kind, want := range map[string]string{"type": "type has no name", "flow": "flow has no name"} {
		_, err := Load(fstest.MapFS{"x.yaml": {Data: []byte("kind: " + kind + "\nversion: 1\n")}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "x.yaml:1")
		assert.Contains(t, err.Error(), want)
	}
}

func TestLoad_RefusesTypesThatIsNotAList(t *testing.T) {
	for name, body := range map[string]string{
		"scalar":  "kind: flow\nname: f\ntypes: foo\n",
		"mapping": "kind: flow\nname: f\ntypes: {a: b}\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(fstest.MapFS{"f.yaml": {Data: []byte(body)}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "f.yaml:3")
			assert.Contains(t, err.Error(), "types")
		})
	}
}

func TestLoad_RefusesAnUnknownKeyInANestedMapping(t *testing.T) {
	cases := map[string]struct{ body, at string }{
		"output.on_pass":    {"kind: flow\nname: f\noutput:\n  on_pass:\n    action: pull_request\n    emit: x\n", "f.yaml:6"},
		"output.on_give_up": {"kind: flow\nname: f\noutput:\n  on_give_up: { action: hand_over, mails: x }\n", "f.yaml:4"},
		"output":            {"kind: flow\nname: f\noutput:\n  on_fail: {}\n", "f.yaml:4"},
		"match":             {"kind: type\nname: t\nmatch:\n  cluster: single\n  sources: compile\n", "t.yaml:5"},
		"match flow style":  {"kind: type\nname: t\nmatch: { cluster: single, sourse: compile }\n", "t.yaml:3"},
		"agent flow style":  {"kind: type\nname: t\nagent: { prompt: p, round: 1 }\n", "t.yaml:3"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			file := strings.SplitN(c.at, ":", 2)[0]
			_, err := Load(fstest.MapFS{file: {Data: []byte(c.body)}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.at)
		})
	}
}

// A flow with no model_may_see loads, and the runner's checks refuse it, so the
// refusal reaches the operator with the rest of the flow's problems. The
// position of the missing list is the flow's own.
func TestLoad_AnAbsentModelMaySeeLoadsEmptyAtTheFlowsLine(t *testing.T) {
	set, err := Load(fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\nname: f\n")}})
	require.NoError(t, err)
	assert.Empty(t, set.Flows["f"].ModelMaySee)
	assert.Equal(t, workflow.Pos{File: "f.yaml", Line: 1}, set.Flows["f"].ModelMaySeePos)
}

func TestLoad_KeepsEveryClassOfModelMaySeeInBlockStyle(t *testing.T) {
	set, err := Load(fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\nname: f\nmodel_may_see:\n  - rows\n  - code\n")}})
	require.NoError(t, err)
	assert.Equal(t, []workflow.DataClass{workflow.ClassRows, workflow.ClassCode}, set.Flows["f"].ModelMaySee)
	assert.Equal(t, 3, set.Flows["f"].ModelMaySeePos.Line)
}

func TestLoad_RefusesModelMaySeeThatIsNotAListOfNames(t *testing.T) {
	for name, tc := range map[string]struct{ body, at string }{
		"scalar":      {"kind: flow\nname: f\nmodel_may_see: code\n", "f.yaml:3"},
		"mapping":     {"kind: flow\nname: f\nmodel_may_see: {code: true}\n", "f.yaml:3"},
		"nested list": {"kind: flow\nname: f\nmodel_may_see:\n  - code\n  - [logs]\n", "f.yaml:5"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(fstest.MapFS{"f.yaml": {Data: []byte(tc.body)}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.at)
			assert.Contains(t, err.Error(), "model_may_see")
		})
	}
}

// A flow with no approver loads with none, which Validate refuses; an approver
// that is not one step name is refused with its line.
func TestLoad_ReadsTheApproverAsOneStepName(t *testing.T) {
	set, err := Load(fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\nname: f\n")}})
	require.NoError(t, err)
	assert.Equal(t, workflow.StepRef{}, set.Flows["f"].Approver)

	for name, tc := range map[string]struct{ body, want string }{
		"list":    {"kind: flow\nname: f\napprover: [node_owner]\n", "f.yaml:3"},
		"mapping": {"kind: flow\nname: f\napprover:\n  step: node_owner\n", "f.yaml:4"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Load(fstest.MapFS{"f.yaml": {Data: []byte(tc.body)}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "approver")
		})
	}
}

// A Kubernetes ConfigMap mount holds each file twice: as a symlink at the top
// (probe.yaml -> ..data/probe.yaml) and inside a timestamped directory that
// ..data points at. Entries whose names start with a dot are the mount's own
// bookkeeping, so each file loads once.
func TestLoad_SkipsKubernetesConfigMapInternals(t *testing.T) {
	dir := kubeletMount(t, "probe.yaml", "kind: type\nversion: 1\nname: probe\nmatch: { cluster: single }\n")

	set, err := Load(os.DirFS(dir))

	require.NoError(t, err)
	require.Len(t, set.Types, 1)
	assert.Equal(t, "probe.yaml", set.Types["probe"].File)
}

// kubeletMount lays out dir like a kubelet's ConfigMap volume holding one file:
// the file in a timestamped directory, a ..data symlink to that directory and
// a symlink of the file's name through ..data.
func kubeletMount(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	stamp := "..2026_11_19_10_00_00.0123456789"
	require.NoError(t, os.Mkdir(filepath.Join(dir, stamp), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, stamp, name), []byte(content), 0o600))
	require.NoError(t, os.Symlink(stamp, filepath.Join(dir, "..data")))
	require.NoError(t, os.Symlink(filepath.Join("..data", name), filepath.Join(dir, name)))
	return dir
}

func TestLoad_SkipsHiddenFilesAndDirectories(t *testing.T) {
	fsys := fstest.MapFS{
		"types/x.yaml":         {Data: []byte("kind: type\nname: x\n")},
		".gitkeep":             {Data: []byte("")},
		".hidden.yaml":         {Data: []byte("kind: type\nname: hidden\n")},
		".dir/inside.yaml":     {Data: []byte("kind: type\nname: inside\n")},
		"types/.draft.yaml":    {Data: []byte("kind: type\nname: draft\n")},
		"types/.old/copy.yaml": {Data: []byte("kind: type\nname: copy\n")},
	}

	set, err := Load(fsys)

	require.NoError(t, err)
	assert.Len(t, set.Types, 1)
	assert.Contains(t, set.Types, "x")
}

func TestLoad_AcceptsYmlFiles(t *testing.T) {
	fsys := fstest.MapFS{
		"f.yml": {Data: []byte(flowYAML)},
		"t.yml": {Data: []byte(typeYAML)},
		"u.YML": {Data: []byte("kind: type\nname: upper\n")},
	}

	set, err := Load(fsys)

	require.NoError(t, err)
	assert.Contains(t, set.Flows, "rejected-release")
	assert.Contains(t, set.Types, "dbt-compile")
	assert.NotContains(t, set.Types, "upper", "an extension other than .yaml and .yml is not read")
}

func TestLoad_ReadsBasedOn(t *testing.T) {
	fsys := fstest.MapFS{
		"f.yaml": {Data: []byte("kind: flow\nname: f\nbased_on: rejected-release@0123456789ab\n")},
		"t.yaml": {Data: []byte("kind: type\nname: t\nbased_on: dbt-compile@fedcba987654\n")},
		"n.yaml": {Data: []byte("kind: type\nname: n\n")},
	}

	set, err := Load(fsys)

	require.NoError(t, err)
	assert.Equal(t, "rejected-release@0123456789ab", set.Flows["f"].BasedOn)
	assert.Equal(t, "dbt-compile@fedcba987654", set.Types["t"].BasedOn)
	assert.Empty(t, set.Types["n"].BasedOn)
}

// based_on is one name and one revision joined by an @, so a value that is
// not is refused with the line of the key, and a fixture has no based_on.
func TestLoad_RefusesAMalformedBasedOn(t *testing.T) {
	for name, value := range map[string]string{
		"no @":           "dbt-compile",
		"two @":          "dbt-compile@abc@def",
		"empty name":     `"@0123456789ab"`,
		"empty revision": "dbt-compile@",
		"empty":          `""`,
		"null":           "",
	} {
		for kind, body := range map[string]string{
			"flow": "kind: flow\nname: f\nbased_on: " + value + "\n",
			"type": "kind: type\nname: t\nmatch: { cluster: single }\nbased_on: " + value + "\n",
		} {
			t.Run(kind+" with "+name, func(t *testing.T) {
				_, err := Load(fstest.MapFS{"x.yaml": {Data: []byte(body)}})

				require.Error(t, err)
				line := strings.Count(strings.SplitAfter(body, "based_on")[0], "\n") + 1
				assert.Contains(t, err.Error(), "x.yaml:"+strconv.Itoa(line)+":")
				assert.Contains(t, err.Error(), "based_on")
				assert.Contains(t, err.Error(), "<name>@<revision>")
			})
		}
	}

	t.Run("a list", func(t *testing.T) {
		_, err := Load(fstest.MapFS{"x.yaml": {Data: []byte("kind: type\nname: t\nbased_on: [a@b]\n")}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "x.yaml:3")
	})
}

// A match.source that no failure can carry selects nothing, and the node it
// was meant for is handed over with no type and no error at startup, so the
// loader refuses it with the line of the key and the sources that exist.
func TestLoad_RefusesAnUnknownMatchSource(t *testing.T) {
	fsys := fstest.MapFS{
		"types/typo.yaml": {Data: []byte("kind: type\nversion: 1\nname: typo\nmatch:\n  cluster: single\n  source: valdation\n")},
	}

	_, err := Load(fsys)

	require.Error(t, err)
	assert.Contains(t, err.Error(), `types/typo.yaml:6: match.source "valdation" is not a remediation source (valid: validation, compile, seed_build, duplicate_table, parse)`)
}

func TestLoad_AnAbsentMatchSourceMatchesAnySource(t *testing.T) {
	set, err := Load(fstest.MapFS{"t.yaml": {Data: []byte("kind: type\nname: any\nmatch: { cluster: single }\n")}})
	require.NoError(t, err)
	assert.Empty(t, set.Types["any"].Match.Source)
}

// builtinYAMLFiles is how many *.yaml and *.yml files the built-in files hold
// under dir. It walks the files and does not load them, so a count taken from a
// load can never hide a file the loader dropped.
func builtinYAMLFiles(t *testing.T, dir string) int {
	t.Helper()
	var n int
	require.NoError(t, fs.WalkDir(workflows.Builtin, dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && (path.Ext(p) == ".yaml" || path.Ext(p) == ".yml") {
			n++
		}
		return err
	}))
	return n
}

// Every built-in flow and type file defines one flow or one type.
func TestLoad_EveryBuiltInFileLoads(t *testing.T) {
	set, err := Load(workflows.Builtin)

	require.NoError(t, err)
	assert.Contains(t, set.Flows, "rejected-release")
	assert.Len(t, set.Flows, builtinYAMLFiles(t, "flows"), "one flow per built-in flow file")
	assert.Len(t, set.Types, builtinYAMLFiles(t, "types"), "one type per built-in type file")
}
