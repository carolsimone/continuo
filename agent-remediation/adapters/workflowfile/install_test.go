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

	set, report, err := LoadInstall(builtinFiles(), install)

	require.NoError(t, err)
	typ := set.Types["dbt-compile"]
	assert.Equal(t, "my-compile.yaml", typ.File)
	assert.Equal(t, 1, typ.Agent.Rounds)
	require.Len(t, typ.Preflight, 1, "the built-in's three pre-flight checks are not merged into the install's one")
	assert.Equal(t, workflow.StepName("content_changed"), typ.Preflight[0].Name)
	require.Len(t, typ.Context, 1, "the built-in's context steps are not merged into the install's")
	assert.Equal(t, []string{"dbt-model"}, typ.Match.NodeTypes)
	assert.Equal(t, []string{"type dbt-compile: replaced by my-compile.yaml"}, report.Notes)

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
	set, report, err := LoadInstall(builtinFiles(), fstest.MapFS{"flow.yml": {Data: []byte(installed)}})

	require.NoError(t, err)
	assert.Equal(t, []string{"dbt-compile"}, set.Flows["rejected-release"].Types)
	assert.Equal(t, 1, set.Flows["rejected-release"].Tries)
	assert.Equal(t, []string{"flow rejected-release: replaced by flow.yml"}, report.Notes)
}

func TestLoadInstall_AddsAnInstallOnlyType(t *testing.T) {
	install := fstest.MapFS{"csv-extra.yaml": {Data: []byte("kind: type\nversion: 1\nname: csv-extra\nmatch: { cluster: single, source: validation }\n")}}

	set, report, err := LoadInstall(builtinFiles(), install)

	require.NoError(t, err)
	assert.Contains(t, set.Types, "csv-extra")
	assert.Equal(t, "csv-extra.yaml", set.Types["csv-extra"].File)
	assert.Contains(t, set.Types, "dbt-compile")
	assert.Contains(t, set.Types, "dbt-parse")
	assert.Empty(t, report.Notes, "adding a type replaces nothing")
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

			set, report, err := LoadInstall(builtin, install)

			require.NoError(t, err, "a moved built-in is a warning, never a refusal")
			assert.Equal(t, tc.basedOn, set.Types["dbt-compile"].BasedOn)
			assert.Equal(t, tc.want, report.Notes)
		})
	}

	t.Run("a flow", func(t *testing.T) {
		install := fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\nversion: 1\nname: rejected-release\nbased_on: rejected-release@000000000000\n")}}

		_, report, err := LoadInstall(builtin, install)

		require.NoError(t, err)
		assert.Equal(t, []string{
			"flow rejected-release: replaced by f.yaml",
			"f.yaml: based on rejected-release@000000000000, which is now @" + flowRevision,
		}, report.Notes)
	})

	t.Run("a flow name that is only a type", func(t *testing.T) {
		install := fstest.MapFS{"f.yaml": {Data: []byte("kind: flow\nversion: 1\nname: mine\nbased_on: dbt-compile@" + current + "\n")}}

		_, report, err := LoadInstall(builtin, install)

		require.NoError(t, err)
		assert.Equal(t, []string{"f.yaml: based on dbt-compile, which is not a built-in flow"}, report.Notes)
	})
}

// A based_on on a file that replaces nothing still warns: it is a copy whose
// source moved or never existed.
func TestLoadInstall_WarnsOfBasedOnOnAnInstallOnlyType(t *testing.T) {
	install := fstest.MapFS{"x.yaml": {Data: []byte("kind: type\nversion: 1\nname: x\nbased_on: dbt-compile@000000000000\nmatch: { cluster: single }\n")}}

	_, report, err := LoadInstall(builtinFiles(), install)

	require.NoError(t, err)
	require.Len(t, report.Notes, 1)
	assert.Contains(t, report.Notes[0], "x.yaml: based on dbt-compile@000000000000, which is now @")
}

func TestLoadInstall_RefusesTwoInstallFilesWithOneName(t *testing.T) {
	install := fstest.MapFS{
		"a.yaml": {Data: []byte("kind: type\nversion: 1\nname: dbt-compile\n")},
		"b.yaml": {Data: []byte("kind: type\nversion: 1\nname: dbt-compile\n")},
	}

	set, report, err := LoadInstall(builtinFiles(), install)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "a.yaml")
	assert.Contains(t, err.Error(), "b.yaml")
	assert.Contains(t, err.Error(), `"dbt-compile"`)
	assert.Empty(t, set.Types)
	assert.Empty(t, report.Notes)
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
			set, report, err := LoadInstall(workflows.Builtin, install)

			require.NoError(t, err)
			assert.Equal(t, want, set)
			assert.Empty(t, report.Notes)
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

	set, report, err := LoadInstall(builtin, install)

	require.NoError(t, err)
	assert.Empty(t, report.Notes)
	base, err := Load(builtin)
	require.NoError(t, err)
	assert.Equal(t, base.Types["dbt-compile"].Revision, set.Types["dbt-compile"].Revision)
	assert.Equal(t, base.Flows["rejected-release"].Types, set.Flows["rejected-release"].Types)
}

func TestLoadInstall_ACopyWithOneByteChangedIsReplaced(t *testing.T) {
	builtin := builtinFiles()
	edited := string(builtin["types/dbt-compile.yaml"].Data) + "# tuned for our repository\n"

	_, report, err := LoadInstall(builtin, fstest.MapFS{"dbt-compile.yaml": {Data: []byte(edited)}})

	require.NoError(t, err)
	assert.Equal(t, []string{"type dbt-compile: replaced by dbt-compile.yaml"}, report.Notes)
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
		_, report, err := LoadInstall(builtinFiles(), install)

		require.NoError(t, err)
		require.Len(t, report.Notes, 4)
		assert.Equal(t, "flow rejected-release: replaced by f.yaml", report.Notes[0])
		assert.Equal(t, "type dbt-compile: replaced by z.yaml", report.Notes[1])
		assert.Equal(t, "type dbt-parse: replaced by a.yaml", report.Notes[2])
		assert.Contains(t, report.Notes[3], "a.yaml: based on dbt-parse@000000000000, which is now @")
	}
}

// An install's type needs no fixture to load: fixtures are read by the
// conformance check, never by the component.
func TestLoadInstall_DoesNotNeedFixtures(t *testing.T) {
	install := fstest.MapFS{"csv-extra.yaml": {Data: []byte("kind: type\nversion: 1\nname: csv-extra\nmatch: { cluster: single, source: validation }\n")}}

	set, _, err := LoadInstall(builtinFiles(), install)

	require.NoError(t, err)
	assert.Contains(t, set.Types, "csv-extra")

	kept, own, err := LoadInstallFixtures(builtinFiles(), install)
	require.NoError(t, err)
	assert.Empty(t, own, "the install's type has no fixture")
	assert.Len(t, kept, 4, "the built-in fixtures stay")
}

// fixtureNodes is the one failing node of each fixture, which names the file a
// fixture came from (see fixtureOf).
func fixtureNodes(fixtures []conformance.Fixture) []string {
	nodes := make([]string, 0, len(fixtures))
	for _, f := range fixtures {
		nodes = append(nodes, f.Trigger.Nodes[0].NodeID)
	}
	return nodes
}

// An install's fixture replaces the built-in fixture of its type, and the load
// keeps the two sides apart: the built-ins that stay, then the install's own.
func TestLoadInstallFixtures_ATypeFixtureReplacesTheBuiltInFixtureOfThatType(t *testing.T) {
	install := fstest.MapFS{"compile-fixture.yaml": {Data: []byte(fixtureOf("type: dbt-compile", "install-compile"))}}

	kept, own, err := LoadInstallFixtures(builtinFiles(), install)

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"built-in-parse", "built-in-maintenance", "built-in-unfixable"}, fixtureNodes(kept),
		"the built-in dbt-compile fixture is replaced, the rest stay")
	require.Len(t, own, 1)
	assert.Equal(t, "install-compile", own[0].Trigger.Nodes[0].NodeID)
	assert.Equal(t, "compile-fixture.yaml", own[0].File)
	assert.Equal(t, "dbt-compile", own[0].Type)
}

func TestLoadInstallFixtures_AFlowFixtureReplacesTheBuiltInFixtureOfThatFlowAndScenario(t *testing.T) {
	install := fstest.MapFS{"maintenance.yaml": {Data: []byte(fixtureOf("flow: rejected-release\nscenario: maintenance", "install-maintenance"))}}

	kept, own, err := LoadInstallFixtures(builtinFiles(), install)

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"built-in-compile", "built-in-parse", "built-in-unfixable"}, fixtureNodes(kept),
		"the built-in maintenance fixture is replaced, both type fixtures stay")
	assert.Equal(t, []string{"install-maintenance"}, fixtureNodes(own))
}

// An install file at the path of a built-in fixture is still the install's:
// the side a fixture comes from never depends on its path.
func TestLoadInstallFixtures_AnInstallFileAtABuiltInPathIsTheInstalls(t *testing.T) {
	install := fstest.MapFS{"fixtures/types/dbt-compile.yaml": {Data: []byte(fixtureOf("type: dbt-compile", "install-compile"))}}

	kept, own, err := LoadInstallFixtures(builtinFiles(), install)

	require.NoError(t, err)
	assert.NotContains(t, fixtureNodes(kept), "built-in-compile")
	assert.NotContains(t, fixtureNodes(kept), "install-compile")
	assert.Equal(t, []string{"install-compile"}, fixtureNodes(own))
}

func TestLoadInstallFixtures_AnotherFlowSameScenarioIsAddedNotReplaced(t *testing.T) {
	install := fstest.MapFS{"other.yaml": {Data: []byte(fixtureOf("flow: another-flow\nscenario: maintenance", "other"))}}

	kept, own, err := LoadInstallFixtures(builtinFiles(), install)

	require.NoError(t, err)
	assert.Len(t, kept, 4)
	assert.Equal(t, []string{"other"}, fixtureNodes(own))
}

func TestLoadInstallFixtures_AnInstallOnlyFixtureIsAdded(t *testing.T) {
	install := fstest.MapFS{"csv-extra.yaml": {Data: []byte(fixtureOf("type: csv-extra", "extra"))}}

	kept, own, err := LoadInstallFixtures(builtinFiles(), install)

	require.NoError(t, err)
	assert.Len(t, kept, 4)
	require.Len(t, own, 1)
	assert.Equal(t, "csv-extra", own[0].Type)
}

func TestLoadInstallFixtures_NoInstallLoadsTheBuiltIns(t *testing.T) {
	want, err := LoadFixtures(workflows.Builtin)
	require.NoError(t, err)

	for name, install := range map[string]fs.FS{"nil": nil, "empty": fstest.MapFS{}} {
		t.Run(name, func(t *testing.T) {
			kept, own, err := LoadInstallFixtures(workflows.Builtin, install)

			require.NoError(t, err)
			assert.Equal(t, want, kept)
			assert.Empty(t, own)
		})
	}
}

func TestLoadInstallFixtures_RefusesABadInstallFixtureWithItsFileAndLine(t *testing.T) {
	install := fstest.MapFS{"bad.yaml": {Data: []byte(fixtureOf("type: dbt-compile", "x") + "answer: {}\n")}}

	kept, own, err := LoadInstallFixtures(builtinFiles(), install)

	require.Error(t, err)
	assert.Nil(t, kept)
	assert.Nil(t, own)
	assert.Contains(t, err.Error(), "bad.yaml:5:")
}

// revisionsByFile is the revision of every flow and type of set by the file it
// was loaded from, and fails the test when two definitions of one file carry
// different revisions: a file then names two loaded files.
func revisionsByFile(t *testing.T, set workflow.Set) map[string]string {
	t.Helper()
	out := map[string]string{}
	add := func(file, revision, what string) {
		if prev, ok := out[file]; ok && prev != revision {
			t.Errorf("%s: %s has revision %s, another definition of the file %s", file, what, revision, prev)
		}
		out[file] = revision
	}
	for name, f := range set.Flows {
		add(f.File, f.Revision, "flow "+name)
	}
	for name, typ := range set.Types {
		add(typ.File, typ.Revision, "type "+name)
	}
	return out
}

// An install file at the path of a built-in file that defines another flow or
// type leaves that built-in loaded, so one path would name two loaded files.
// The load refuses it, naming both definitions.
func TestLoadInstall_RefusesAFileAtABuiltInPathWhoseDefinitionStaysLoaded(t *testing.T) {
	for _, tc := range []struct {
		name, path, body, want string
	}{
		{
			name: "type",
			path: "types/dbt-compile.yaml",
			body: "kind: type\nversion: 1\nname: my-compile\nmatch: { cluster: single, source: compile }\n",
			want: `types/dbt-compile.yaml:1: type "my-compile" is in a file with the path of the built-in file that defines type "dbt-compile", which stays loaded; give the install file another path`,
		},
		{
			name: "flow",
			path: "flows/rejected-release.yaml",
			body: "kind: flow\nversion: 1\nname: my-flow\n",
			want: `flows/rejected-release.yaml:1: flow "my-flow" is in a file with the path of the built-in file that defines flow "rejected-release", which stays loaded; give the install file another path`,
		},
		{
			name: "type at a flow's path",
			path: "flows/rejected-release.yaml",
			body: "# a type kept where the built-in flow is\nkind: type\nversion: 1\nname: my-compile\nmatch: { cluster: single, source: compile }\n",
			want: `flows/rejected-release.yaml:2: type "my-compile" is in a file with the path of the built-in file that defines flow "rejected-release", which stays loaded; give the install file another path`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set, report, err := LoadInstall(builtinFiles(), fstest.MapFS{tc.path: {Data: []byte(tc.body)}})

			require.Error(t, err)
			assert.Equal(t, tc.want, err.Error())
			assert.Equal(t, workflow.Set{}, set)
			assert.Equal(t, InstallReport{}, report)
		})
	}
}

// A file at a built-in's path that replaces what the built-in file defines,
// with other content or as a byte for byte copy, loads: the built-in is gone,
// and the path names the install's file alone.
func TestLoadInstall_AFileAtABuiltInPathThatReplacesItLoads(t *testing.T) {
	builtin := builtinFiles()
	for name, data := range map[string][]byte{
		"replacement": []byte(installedCompile),
		"copy":        builtin["types/dbt-compile.yaml"].Data,
	} {
		t.Run(name, func(t *testing.T) {
			set, _, err := LoadInstall(builtin, fstest.MapFS{"types/dbt-compile.yaml": {Data: data}})

			require.NoError(t, err)
			own, err := Load(fstest.MapFS{"types/dbt-compile.yaml": {Data: data}})
			require.NoError(t, err)
			assert.Equal(t, own.Types["dbt-compile"].Revision, set.Types["dbt-compile"].Revision)
			assert.Equal(t, own.Types["dbt-compile"].Revision, revisionsByFile(t, set)["types/dbt-compile.yaml"])
		})
	}
}

// Whether a built-in stays loaded decides, not which install file replaces
// it: an install file at the path of a built-in type that another install
// file replaces loads, and each path names one loaded file.
func TestLoadInstall_AFileAtTheBuiltInPathOfATypeReplacedElsewhereLoads(t *testing.T) {
	install := fstest.MapFS{
		"types/dbt-compile.yaml": {Data: []byte("kind: type\nversion: 1\nname: my-compile\nmatch: { cluster: single, source: compile }\n")},
		"compile.yaml":           {Data: []byte(installedCompile)},
	}

	set, _, err := LoadInstall(builtinFiles(), install)

	require.NoError(t, err)
	assert.Equal(t, "types/dbt-compile.yaml", set.Types["my-compile"].File)
	assert.Equal(t, "compile.yaml", set.Types["dbt-compile"].File)
	revisions := revisionsByFile(t, set)
	assert.NotEqual(t, revisions["types/dbt-compile.yaml"], revisions["compile.yaml"])
}

// An install file at the path of a built-in file whose flow, type or fixture
// stays loaded would make one path name two loaded files, whatever the kinds
// of the two files, so the check refuses it, naming the install file's first
// definition and what the built-in file holds.
func TestCheckInstallPaths_RefusesAFileAtABuiltInPathWhoseContentStaysLoaded(t *testing.T) {
	for _, tc := range []struct {
		name, path, body, want string
	}{
		{
			name: "a fixture at the path of a built-in type",
			path: "types/dbt-parse.yaml",
			body: fixtureOf("type: dbt-compile", "install-compile"),
			want: `types/dbt-parse.yaml:1: the fixture of type "dbt-compile" is in a file with the path of the built-in file that defines type "dbt-parse", which stays loaded; give the install file another path`,
		},
		{
			name: "a fixture at the path of a built-in flow",
			path: "flows/rejected-release.yaml",
			body: fixtureOf("flow: rejected-release\nscenario: maintenance", "install-maintenance"),
			want: `flows/rejected-release.yaml:1: the fixture of flow "rejected-release" and scenario "maintenance" is in a file with the path of the built-in file that defines flow "rejected-release", which stays loaded; give the install file another path`,
		},
		{
			name: "a type at the path of a built-in fixture",
			path: "fixtures/types/dbt-compile.yaml",
			body: "kind: type\nversion: 1\nname: my-compile\nmatch: { cluster: single, source: compile }\n",
			want: `fixtures/types/dbt-compile.yaml:1: type "my-compile" is in a file with the path of the built-in file that holds the fixture of type "dbt-compile", which stays loaded; give the install file another path`,
		},
		{
			name: "a flow at the path of a built-in fixture",
			path: "fixtures/flows/maintenance.yaml",
			body: "kind: flow\nversion: 1\nname: my-flow\n",
			want: `fixtures/flows/maintenance.yaml:1: flow "my-flow" is in a file with the path of the built-in file that holds the fixture of flow "rejected-release" and scenario "maintenance", which stays loaded; give the install file another path`,
		},
		{
			name: "a type fixture at the path of a built-in type fixture",
			path: "fixtures/types/dbt-compile.yaml",
			body: fixtureOf("type: dbt-parse", "install-parse"),
			want: `fixtures/types/dbt-compile.yaml:1: the fixture of type "dbt-parse" is in a file with the path of the built-in file that holds the fixture of type "dbt-compile", which stays loaded; give the install file another path`,
		},
		{
			name: "a flow fixture at the path of a built-in flow fixture",
			path: "fixtures/flows/maintenance.yaml",
			body: fixtureOf("flow: rejected-release\nscenario: unfixable", "install-unfixable"),
			want: `fixtures/flows/maintenance.yaml:1: the fixture of flow "rejected-release" and scenario "unfixable" is in a file with the path of the built-in file that holds the fixture of flow "rejected-release" and scenario "maintenance", which stays loaded; give the install file another path`,
		},
		{
			name: "a type at the path of a built-in type",
			path: "types/dbt-compile.yaml",
			body: "kind: type\nversion: 1\nname: my-compile\nmatch: { cluster: single, source: compile }\n",
			want: `types/dbt-compile.yaml:1: type "my-compile" is in a file with the path of the built-in file that defines type "dbt-compile", which stays loaded; give the install file another path`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckInstallPaths(builtinFiles(), fstest.MapFS{tc.path: {Data: []byte(tc.body)}})

			require.Error(t, err)
			assert.Equal(t, tc.want, err.Error())
		})
	}
}

// A built-in file that holds a flow or type and a fixture is named by all it
// holds, each kind with its own verb.
func TestCheckInstallPaths_NamesEverythingTheBuiltInFileHolds(t *testing.T) {
	builtin := builtinFiles()
	builtin["types/dbt-parse.yaml"] = &fstest.MapFile{Data: append(append([]byte{}, builtin["types/dbt-parse.yaml"].Data...),
		[]byte("---\n"+fixtureOf("type: dbt-parse", "built-in-parse-beside"))...)}
	delete(builtin, "fixtures/types/dbt-parse.yaml")

	err := CheckInstallPaths(builtin, fstest.MapFS{"types/dbt-parse.yaml": {Data: []byte(fixtureOf("type: dbt-compile", "install-compile"))}})

	require.Error(t, err)
	assert.Equal(t, `types/dbt-parse.yaml:1: the fixture of type "dbt-compile" is in a file with the path of the built-in file that defines type "dbt-parse" and holds the fixture of type "dbt-parse", which stay loaded; give the install file another path`, err.Error())
}

// A file at a built-in's path whose content replaces all the built-in file
// holds loads, whatever its kind: the same kind and name, the same fixture key,
// a byte for byte copy, or a built-in that another install file replaces.
func TestCheckInstallPaths_AFileAtABuiltInPathWhoseBuiltInIsReplacedPasses(t *testing.T) {
	builtin := builtinFiles()
	parseType := "kind: type\nversion: 1\nname: dbt-parse\nmatch: { cluster: single, source: parse }\n"
	for _, tc := range []struct {
		name    string
		install fstest.MapFS
	}{
		{name: "a type of the built-in type's name", install: fstest.MapFS{"types/dbt-parse.yaml": {Data: []byte(parseType)}}},
		{name: "a fixture of the built-in fixture's type", install: fstest.MapFS{"fixtures/types/dbt-parse.yaml": {Data: []byte(fixtureOf("type: dbt-parse", "install-parse"))}}},
		{name: "a fixture of the built-in fixture's flow and scenario", install: fstest.MapFS{"fixtures/flows/maintenance.yaml": {Data: []byte(fixtureOf("flow: rejected-release\nscenario: maintenance", "install-maintenance"))}}},
		{name: "a byte for byte copy of a type file", install: fstest.MapFS{"types/dbt-parse.yaml": builtin["types/dbt-parse.yaml"]}},
		{name: "a byte for byte copy of a fixture file", install: fstest.MapFS{"fixtures/types/dbt-compile.yaml": builtin["fixtures/types/dbt-compile.yaml"]}},
		{name: "a fixture at the path of a type another install file replaces", install: fstest.MapFS{
			"types/dbt-parse.yaml": {Data: []byte(fixtureOf("type: dbt-compile", "install-compile"))},
			"parse.yaml":           {Data: []byte(parseType)},
		}},
		{name: "a type at the path of a fixture another install file replaces", install: fstest.MapFS{
			"fixtures/types/dbt-compile.yaml": {Data: []byte("kind: type\nversion: 1\nname: my-compile\nmatch: { cluster: single, source: compile }\n")},
			"compile-fixture.yaml":            {Data: []byte(fixtureOf("type: dbt-compile", "install-compile"))},
		}},
		{name: "a type and its fixture at the path of the built-in type", install: fstest.MapFS{
			"types/dbt-parse.yaml": {Data: []byte(parseType + "---\n" + fixtureOf("type: dbt-parse", "install-parse"))},
		}},
		{name: "no file at a built-in path", install: fstest.MapFS{"mine.yaml": {Data: []byte(fixtureOf("type: dbt-parse", "install-parse"))}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.NoError(t, CheckInstallPaths(builtin, tc.install))
		})
	}
}

// The built-in folder given as the install folder replaces every built-in with
// itself, and no install folder adds nothing, so neither is refused.
func TestCheckInstallPaths_TheBuiltInFolderAsTheInstallPasses(t *testing.T) {
	assert.NoError(t, CheckInstallPaths(workflows.Builtin, workflows.Builtin))
	assert.NoError(t, CheckInstallPaths(workflows.Builtin, nil))
	assert.NoError(t, CheckInstallPaths(workflows.Builtin, fstest.MapFS{}))
}

// A file the check cannot load is an error with its file and line.
func TestCheckInstallPaths_RefusesAFileThatDoesNotLoad(t *testing.T) {
	err := CheckInstallPaths(builtinFiles(), fstest.MapFS{"bad.yaml": {Data: []byte("kind: type\nname: [unclosed\n")}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad.yaml:")
}
