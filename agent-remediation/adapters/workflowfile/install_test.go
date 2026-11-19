package workflowfile

import (
	"io/fs"
	"os"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
)

// builtinFiles is a small set of built-in files: one flow, two types and the
// fixtures of the types and of the flow.
func builtinFiles() fstest.MapFS {
	return fstest.MapFS{
		"flows/rejected-release.yaml": {Data: []byte(flowYAML)},
		"types/dbt-compile.yaml":      {Data: []byte(typeYAML)},
		"types/dbt-parse.yaml": {Data: []byte(`kind: type
version: 1
name: dbt-parse
match: { cluster: single, source: parse }
context: [failing_node_source]
agent: { prompt: dbt-parse, tools: [], rounds: 3 }
preflight: [content_changed]
`)},
		"fixtures/types/dbt-compile.yaml": {Data: []byte(fixtureOf("type: dbt-compile", "built-in-compile"))},
		"fixtures/types/dbt-parse.yaml":   {Data: []byte(fixtureOf("type: dbt-parse", "built-in-parse"))},
		"fixtures/flows/maintenance.yaml": {Data: []byte(fixtureOf("flow: rejected-release\nscenario: maintenance", "built-in-maintenance"))},
		"fixtures/flows/unfixable.yaml":   {Data: []byte(fixtureOf("flow: rejected-release\nscenario: unfixable", "built-in-unfixable"))},
	}
}

// fixtureOf is a fixture file that tests what names (the `type:` line, or the
// `flow:` and `scenario:` lines) and whose one failing node is node, so a test
// can tell which file a loaded fixture came from.
func fixtureOf(names, node string) string {
	return "kind: fixture\nversion: 1\n" + names + "\ntrigger: { nodes: [{ node_id: " + node + " }] }\n"
}

// installedCompile is an install's own dbt-compile: a file of the built-in's
// kind and name that lists other steps.
const installedCompile = `kind: type
version: 1
name: dbt-compile
match: { cluster: single, source: compile, node_types: [dbt-model] }
context: [failing_node_source]
agent: { prompt: dbt-compile, tools: [], rounds: 1 }
preflight: [content_changed]
`

// An install file of the kind and name of a built-in replaces it whole: the
// install's lists stand as written, nothing of the built-in's lists is kept,
// and the load says what replaced what.
func TestLoadInstall_AnInstallFileReplacesTheBuiltInWhole(t *testing.T) {
	install := fstest.MapFS{"my-compile.yaml": {Data: []byte(installedCompile)}}

	set, notes, err := LoadInstall(builtinFiles(), install)

	require.NoError(t, err)
	typ := set.Types["dbt-compile"]
	assert.Equal(t, "my-compile.yaml", typ.File)
	assert.Equal(t, 1, typ.Agent.Rounds)
	require.Len(t, typ.Preflight, 1, "the built-in's three pre-flight checks are not merged into the install's one")
	assert.Equal(t, workflow.StepName("content_changed"), typ.Preflight[0].Name)
	require.Len(t, typ.Context, 1, "the built-in's context steps are not merged into the install's")
	assert.Equal(t, []string{"dbt-model"}, typ.Match.NodeTypes)
	assert.Equal(t, []string{"type dbt-compile: replaced by my-compile.yaml"}, notes)

	assert.Equal(t, "types/dbt-parse.yaml", set.Types["dbt-parse"].File, "a built-in the install does not name stays")
	assert.Contains(t, set.Flows, "rejected-release")
}

func TestLoadInstall_AnInstallFlowReplacesTheBuiltInFlow(t *testing.T) {
	installed := `kind: flow
version: 1
name: rejected-release
trigger: remediation.requested
types: [dbt-compile]
tries: 1
`
	set, notes, err := LoadInstall(builtinFiles(), fstest.MapFS{"flow.yml": {Data: []byte(installed)}})

	require.NoError(t, err)
	assert.Equal(t, []string{"dbt-compile"}, set.Flows["rejected-release"].Types)
	assert.Equal(t, 1, set.Flows["rejected-release"].Tries)
	assert.Equal(t, []string{"flow rejected-release: replaced by flow.yml"}, notes)
}

func TestLoadInstall_AddsAnInstallOnlyType(t *testing.T) {
	install := fstest.MapFS{"csv-extra.yaml": {Data: []byte("kind: type\nversion: 1\nname: csv-extra\nmatch: { cluster: single, source: validation }\n")}}

	set, notes, err := LoadInstall(builtinFiles(), install)

	require.NoError(t, err)
	assert.Contains(t, set.Types, "csv-extra")
	assert.Equal(t, "csv-extra.yaml", set.Types["csv-extra"].File)
	assert.Contains(t, set.Types, "dbt-compile")
	assert.Contains(t, set.Types, "dbt-parse")
	assert.Empty(t, notes, "adding a type replaces nothing")
}

// based_on names the built-in a file was copied from and the revision copied.
// When the built-in has changed since, the load warns and the file still
// loads; a name that is no built-in warns too; a current revision is silent.
func TestLoadInstall_WarnsWhenTheBuiltInMovedSinceBasedOn(t *testing.T) {
	builtin := builtinFiles()
	base, err := Load(builtin)
	require.NoError(t, err)
	current := base.Types["dbt-compile"].Revision
	flowRevision := base.Flows["rejected-release"].Revision

	cases := map[string]struct {
		file, basedOn string
		kind          string
		want          []string
	}{
		"a built-in that moved": {
			file: "mine.yaml", basedOn: "dbt-compile@000000000000", kind: "type",
			want: []string{
				"type dbt-compile: replaced by mine.yaml",
				"mine.yaml: based on dbt-compile@000000000000, which is now @" + current,
			},
		},
		"a built-in that did not move": {
			file: "mine.yaml", basedOn: "dbt-compile@" + current, kind: "type",
			want: []string{"type dbt-compile: replaced by mine.yaml"},
		},
		"a name that is no built-in": {
			file: "mine.yaml", basedOn: "dbt-gone@000000000000", kind: "type",
			want: []string{
				"type dbt-compile: replaced by mine.yaml",
				"mine.yaml: based on dbt-gone, which is not a built-in type",
			},
		},
		"a type name that is only a flow": {
			file: "mine.yaml", basedOn: "rejected-release@" + flowRevision, kind: "type",
			want: []string{
				"type dbt-compile: replaced by mine.yaml",
				"mine.yaml: based on rejected-release, which is not a built-in type",
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			body := "kind: type\nversion: 1\nname: dbt-compile\nbased_on: " + tc.basedOn + "\nmatch: { cluster: single, source: compile }\n"
			install := fstest.MapFS{tc.file: {Data: []byte(body)}}

			set, notes, err := LoadInstall(builtin, install)

			require.NoError(t, err, "a moved built-in is a warning, never a refusal")
			assert.Equal(t, tc.basedOn, set.Types["dbt-compile"].BasedOn)
			assert.Equal(t, tc.want, notes)
		})
	}

	t.Run("a flow", func(t *testing.T) {
		install := fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\nversion: 1\nname: rejected-release\nbased_on: rejected-release@000000000000\n")}}

		_, notes, err := LoadInstall(builtin, install)

		require.NoError(t, err)
		assert.Equal(t, []string{
			"flow rejected-release: replaced by f.yaml",
			"f.yaml: based on rejected-release@000000000000, which is now @" + flowRevision,
		}, notes)
	})

	t.Run("a flow name that is only a type", func(t *testing.T) {
		install := fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\nversion: 1\nname: mine\nbased_on: dbt-compile@" + current + "\n")}}

		_, notes, err := LoadInstall(builtin, install)

		require.NoError(t, err)
		assert.Equal(t, []string{"f.yaml: based on dbt-compile, which is not a built-in flow"}, notes)
	})
}

// A based_on on a file that replaces nothing still warns: it is a copy whose
// source moved or never existed.
func TestLoadInstall_WarnsOfBasedOnOnAnInstallOnlyType(t *testing.T) {
	install := fstest.MapFS{"x.yaml": {Data: []byte("kind: type\nversion: 1\nname: x\nbased_on: dbt-compile@000000000000\nmatch: { cluster: single }\n")}}

	_, notes, err := LoadInstall(builtinFiles(), install)

	require.NoError(t, err)
	require.Len(t, notes, 1)
	assert.Contains(t, notes[0], "x.yaml: based on dbt-compile@000000000000, which is now @")
}

func TestLoadInstall_RefusesTwoInstallFilesWithOneName(t *testing.T) {
	install := fstest.MapFS{
		"a.yaml": {Data: []byte("kind: type\nversion: 1\nname: dbt-compile\n")},
		"b.yaml": {Data: []byte("kind: type\nversion: 1\nname: dbt-compile\n")},
	}

	set, notes, err := LoadInstall(builtinFiles(), install)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "a.yaml")
	assert.Contains(t, err.Error(), "b.yaml")
	assert.Contains(t, err.Error(), `"dbt-compile"`)
	assert.Empty(t, set.Types)
	assert.Empty(t, notes)
}

// A refused install file stops the load with its file and line, and no
// partial set is returned.
func TestLoadInstall_RefusesABadInstallFile(t *testing.T) {
	install := fstest.MapFS{"bad.yaml": {Data: []byte("kind: type\nname: x\nmatch: { cluster: single, source: valdation }\n")}}

	set, _, err := LoadInstall(builtinFiles(), install)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad.yaml:3:")
	assert.Empty(t, set.Types)
	assert.Empty(t, set.Flows)
}

func TestLoadInstall_RefusesABadBuiltInFile(t *testing.T) {
	builtin := fstest.MapFS{"types/bad.yaml": {Data: []byte("kind: type\nname: x\nbogus: 1\n")}}

	_, _, err := LoadInstall(builtin, fstest.MapFS{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "types/bad.yaml:3:")
}

// An empty install folder, a missing one (nil) and the built-ins alone load
// the same set.
func TestLoadInstall_AnEmptyDirLoadsTheBuiltIns(t *testing.T) {
	want, err := Load(workflows.Builtin)
	require.NoError(t, err)

	for name, install := range map[string]fs.FS{
		"an empty folder":      os.DirFS(t.TempDir()),
		"an empty file system": fstest.MapFS{},
		"no folder":            nil,
	} {
		t.Run(name, func(t *testing.T) {
			set, notes, err := LoadInstall(workflows.Builtin, install)

			require.NoError(t, err)
			assert.Equal(t, want, set)
			assert.Empty(t, notes)
		})
	}
}

// A file of an install that is a byte for byte copy of a built-in changes
// nothing, so it is no replacement and the load has nothing to say about it.
func TestLoadInstall_AnIdenticalCopyIsNotReplaced(t *testing.T) {
	builtin := builtinFiles()
	install := fstest.MapFS{
		"dbt-compile.yaml":      builtin["types/dbt-compile.yaml"],
		"rejected-release.yaml": builtin["flows/rejected-release.yaml"],
	}

	set, notes, err := LoadInstall(builtin, install)

	require.NoError(t, err)
	assert.Empty(t, notes)
	base, err := Load(builtin)
	require.NoError(t, err)
	assert.Equal(t, base.Types["dbt-compile"].Revision, set.Types["dbt-compile"].Revision)
	assert.Equal(t, base.Flows["rejected-release"].Types, set.Flows["rejected-release"].Types)
}

func TestLoadInstall_ACopyWithOneByteChangedIsReplaced(t *testing.T) {
	builtin := builtinFiles()
	edited := string(builtin["types/dbt-compile.yaml"].Data) + "# tuned for our repository\n"

	_, notes, err := LoadInstall(builtin, fstest.MapFS{"dbt-compile.yaml": {Data: []byte(edited)}})

	require.NoError(t, err)
	assert.Equal(t, []string{"type dbt-compile: replaced by dbt-compile.yaml"}, notes)
}

// The notes come in a fixed order: flows by name, then types by name, each
// file's replacement before its based_on warning.
func TestLoadInstall_NotesComeInNameOrder(t *testing.T) {
	install := fstest.MapFS{
		"z.yaml": {Data: []byte(installedCompile)},
		"a.yaml": {Data: []byte("kind: type\nversion: 1\nname: dbt-parse\nbased_on: dbt-parse@000000000000\nmatch: { cluster: single }\n")},
		"f.yaml": {Data: []byte("kind: flow\nversion: 1\nname: rejected-release\n")},
	}

	for range 10 {
		_, notes, err := LoadInstall(builtinFiles(), install)

		require.NoError(t, err)
		require.Len(t, notes, 4)
		assert.Equal(t, "flow rejected-release: replaced by f.yaml", notes[0])
		assert.Equal(t, "type dbt-compile: replaced by z.yaml", notes[1])
		assert.Equal(t, "type dbt-parse: replaced by a.yaml", notes[2])
		assert.Contains(t, notes[3], "a.yaml: based on dbt-parse@000000000000, which is now @")
	}
}

// An install's type needs no fixture to load: fixtures are read by the
// conformance check, never by the service.
func TestLoadInstall_DoesNotNeedFixtures(t *testing.T) {
	install := fstest.MapFS{"csv-extra.yaml": {Data: []byte("kind: type\nversion: 1\nname: csv-extra\nmatch: { cluster: single, source: validation }\n")}}

	set, _, err := LoadInstall(builtinFiles(), install)

	require.NoError(t, err)
	assert.Contains(t, set.Types, "csv-extra")

	fixtures, err := LoadInstallFixtures(builtinFiles(), install)
	require.NoError(t, err)
	for _, f := range fixtures {
		assert.NotEqual(t, "csv-extra", f.Type)
	}
	assert.Len(t, fixtures, 4, "the install's type has no fixture, and the built-in fixtures stay")
}

func TestLoadInstallFixtures_ATypeFixtureReplacesTheBuiltInFixtureOfThatType(t *testing.T) {
	install := fstest.MapFS{"compile-fixture.yaml": {Data: []byte(fixtureOf("type: dbt-compile", "install-compile"))}}

	fixtures, err := LoadInstallFixtures(builtinFiles(), install)

	require.NoError(t, err)
	byType := map[string]conformance.Fixture{}
	for _, f := range fixtures {
		if f.Type != "" {
			require.NotContains(t, byType, f.Type, "one fixture per type")
			byType[f.Type] = f
		}
	}
	require.Len(t, byType, 2)
	assert.Equal(t, "install-compile", byType["dbt-compile"].Trigger.Nodes[0].NodeID)
	assert.Equal(t, "compile-fixture.yaml", byType["dbt-compile"].File)
	assert.Equal(t, "built-in-parse", byType["dbt-parse"].Trigger.Nodes[0].NodeID)
	assert.Len(t, fixtures, 4, "both flow fixtures stay")
}

func TestLoadInstallFixtures_AFlowFixtureReplacesTheBuiltInFixtureOfThatFlowAndScenario(t *testing.T) {
	install := fstest.MapFS{"maintenance.yaml": {Data: []byte(fixtureOf("flow: rejected-release\nscenario: maintenance", "install-maintenance"))}}

	fixtures, err := LoadInstallFixtures(builtinFiles(), install)

	require.NoError(t, err)
	byScenario := map[conformance.Scenario]conformance.Fixture{}
	for _, f := range fixtures {
		if f.Flow != "" {
			require.NotContains(t, byScenario, f.Scenario, "one fixture per flow and scenario")
			byScenario[f.Scenario] = f
		}
	}
	require.Len(t, byScenario, 2)
	assert.Equal(t, "install-maintenance", byScenario["maintenance"].Trigger.Nodes[0].NodeID)
	assert.Equal(t, "built-in-unfixable", byScenario["unfixable"].Trigger.Nodes[0].NodeID)
	assert.Len(t, fixtures, 4, "both type fixtures stay")
}

func TestLoadInstallFixtures_AnotherFlowSameScenarioIsAddedNotReplaced(t *testing.T) {
	install := fstest.MapFS{"other.yaml": {Data: []byte(fixtureOf("flow: another-flow\nscenario: maintenance", "other"))}}

	fixtures, err := LoadInstallFixtures(builtinFiles(), install)

	require.NoError(t, err)
	assert.Len(t, fixtures, 5)
}

func TestLoadInstallFixtures_AnInstallOnlyFixtureIsAdded(t *testing.T) {
	install := fstest.MapFS{"csv-extra.yaml": {Data: []byte(fixtureOf("type: csv-extra", "extra"))}}

	fixtures, err := LoadInstallFixtures(builtinFiles(), install)

	require.NoError(t, err)
	assert.Len(t, fixtures, 5)
	var found bool
	for _, f := range fixtures {
		found = found || f.Type == "csv-extra"
	}
	assert.True(t, found)
}

func TestLoadInstallFixtures_NoInstallLoadsTheBuiltIns(t *testing.T) {
	want, err := LoadFixtures(workflows.Builtin)
	require.NoError(t, err)

	for name, install := range map[string]fs.FS{"nil": nil, "empty": fstest.MapFS{}} {
		t.Run(name, func(t *testing.T) {
			got, err := LoadInstallFixtures(workflows.Builtin, install)

			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestLoadInstallFixtures_RefusesABadInstallFixtureWithItsFileAndLine(t *testing.T) {
	install := fstest.MapFS{"bad.yaml": {Data: []byte(fixtureOf("type: dbt-compile", "x") + "answer: {}\n")}}

	fixtures, err := LoadInstallFixtures(builtinFiles(), install)

	require.Error(t, err)
	assert.Nil(t, fixtures)
	assert.Contains(t, err.Error(), "bad.yaml:5:")
}
