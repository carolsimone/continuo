package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
)

// wireReport is the JSON object `files` prints, as a reader decodes it.
type wireReport struct {
	Files     int            `json:"files"`
	Errors    []string       `json:"errors"`
	Warnings  []string       `json:"warnings"`
	Scenarios []wireScenario `json:"scenarios"`
}

type wireScenario struct {
	Type     string `json:"type"`
	Scenario string `json:"scenario"`
	OK       bool   `json:"ok"`
	Error    string `json:"error"`
}

const builtinDir = "../../workflows"

// builtinYAMLFiles is how many *.yaml and *.yml files the built-in files hold
// under dir ("." for all of them). It walks the files and does not load them,
// so a file that fails to load is still counted and a count taken from a load
// can never hide it.
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

// builtinFileCount is the number of files the built-in files hold: the flow
// and type files and the fixtures of the types and of the flows.
func builtinFileCount(t *testing.T) int {
	t.Helper()
	return builtinYAMLFiles(t, ".")
}

// builtinScenarioCount is the number of scenarios the built-in files play:
// every built-in type through every type scenario, then every built-in flow
// through every flow scenario. A built-in flow or type file holds one flow or
// one type, and every built-in type is listed by a flow.
func builtinScenarioCount(t *testing.T) int {
	t.Helper()
	return builtinYAMLFiles(t, "types")*len(conformance.TypeScenarios()) +
		builtinYAMLFiles(t, "flows")*len(conformance.FlowScenarios())
}

// lineOf is the line, counted from 1, of the first line of text that starts
// with prefix.
func lineOf(t *testing.T, text, prefix string) int {
	t.Helper()
	for i, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, prefix) {
			return i + 1
		}
	}
	require.FailNow(t, "no line starts with "+prefix)
	return 0
}

func runFiles(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func decode(t *testing.T, stdout string) wireReport {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(stdout))
	dec.DisallowUnknownFields()
	var r wireReport
	require.NoError(t, dec.Decode(&r), stdout)
	require.False(t, dec.More(), "stdout holds more than one JSON object")
	return r
}

func TestFiles_BuiltinDirPasses(t *testing.T) {
	code, stdout, stderr := runFiles(t, "files", builtinDir)

	r := decode(t, stdout)
	require.Equal(t, 0, code, "stderr: %s\nstdout: %s", stderr, stdout)
	assert.Empty(t, r.Errors)
	assert.Empty(t, r.Warnings)
	assert.Equal(t, builtinFileCount(t), r.Files)
	require.Len(t, r.Scenarios, builtinScenarioCount(t))
	for _, s := range r.Scenarios {
		assert.True(t, s.OK, "%s %s: %s", s.Type, s.Scenario, s.Error)
		assert.Empty(t, s.Error)
	}
	assert.Equal(t, "rejected-release", r.Scenarios[len(r.Scenarios)-1].Type)
}

// The report names the flow scenario in which the classifier marked the node
// unfixable, and it passes.
func TestFiles_TheReportHoldsTheUnfixableFlowScenario(t *testing.T) {
	_, stdout, _ := runFiles(t, "files", builtinDir)

	var found []wireScenario
	for _, s := range decode(t, stdout).Scenarios {
		if s.Type == "rejected-release" && s.Scenario == "unfixable" {
			found = append(found, s)
		}
	}
	require.Len(t, found, 1)
	assert.True(t, found[0].OK, found[0].Error)
}

func TestFiles_OutputKeysFollowTheDocumentedOrder(t *testing.T) {
	_, stdout, _ := runFiles(t, "files", builtinDir)

	assert.True(t, strings.HasPrefix(stdout, `{"files":`), stdout)
	files := strings.Index(stdout, `"errors":`)
	warnings := strings.Index(stdout, `"warnings":`)
	scenarios := strings.Index(stdout, `"scenarios":`)
	assert.Less(t, files, warnings)
	assert.Less(t, warnings, scenarios)
	assert.Contains(t, stdout, `"type":`)
	assert.Contains(t, stdout, `"scenario":`)
	assert.Contains(t, stdout, `"ok":`)
	assert.Contains(t, stdout, `"error":`)
}

func TestFiles_AnUnknownStepExitsOne(t *testing.T) {
	dir := t.TempDir()
	flow := `kind: flow
version: 1
name: broken
trigger: remediation.requested
types: [only-type]
context: []
across_clusters: []
gate: [no_such_step]
tries: 1
model_may_see: [code, logs]
output:
  on_pass: { action: pull_request, emits: remediation.proposed }
  on_give_up: { action: hand_over }
approver: node_owner
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "flow.yaml"), []byte(flow), 0o600))

	code, stdout, stderr := runFiles(t, "files", dir)

	assert.Equal(t, 1, code, stderr)
	r := decode(t, stdout)
	assert.NotEmpty(t, r.Errors)
	assert.Contains(t, strings.Join(r.Errors, "\n"), "no_such_step")
	assert.NotNil(t, r.Warnings)
	assert.NotNil(t, r.Scenarios)
	assert.Empty(t, r.Scenarios, "the suite is skipped when the files do not validate")
	assert.Equal(t, builtinFileCount(t)+1, r.Files, "the install's one file and every built-in file")
}

func TestFiles_AFileThatDoesNotLoadExitsOneWithEmptyArrays(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte("kind: nonsense\n"), 0o600))

	code, stdout, _ := runFiles(t, "files", dir)

	assert.Equal(t, 1, code)
	assert.Contains(t, stdout, `"warnings":[]`)
	assert.Contains(t, stdout, `"scenarios":[]`)
	assert.NotEmpty(t, decode(t, stdout).Errors)
}

// The built-in files are always loaded, so an install folder with no file
// checks the built-ins alone.
func TestFiles_AnEmptyDirChecksTheBuiltInsAlone(t *testing.T) {
	code, stdout, stderr := runFiles(t, "files", t.TempDir())

	r := decode(t, stdout)
	require.Equal(t, 0, code, "stderr: %s\nstdout: %s", stderr, stdout)
	assert.Empty(t, r.Errors)
	assert.Empty(t, r.Warnings)
	assert.Equal(t, builtinFileCount(t), r.Files)
	require.Len(t, r.Scenarios, builtinScenarioCount(t))
	for _, s := range r.Scenarios {
		assert.True(t, s.OK, "%s %s: %s", s.Type, s.Scenario, s.Error)
	}
}

func TestFiles_NoArgumentsExitsTwo(t *testing.T) {
	code, stdout, stderr := runFiles(t)

	assert.Equal(t, 2, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "usage: remediation-check files <dir>")
}

func TestFiles_BadUsageExitsTwoWithAUsageLine(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent")
	file := filepath.Join(t.TempDir(), "a-file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	for name, args := range map[string][]string{
		"unknown subcommand": {"verify", builtinDir},
		"missing dir arg":    {"files"},
		"extra arguments":    {"files", builtinDir, "more"},
		"dir does not exist": {"files", missing},
		"dir is a file":      {"files", file},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runFiles(t, args...)

			assert.Equal(t, 2, code, stderr)
			assert.Empty(t, stdout)
			assert.Contains(t, stderr, "usage: remediation-check files <dir>")
		})
	}
}

func TestFiles_OutputIsDeterministic(t *testing.T) {
	_, first, _ := runFiles(t, "files", builtinDir)
	_, second, _ := runFiles(t, "files", builtinDir)

	assert.Equal(t, first, second)
}

// copyBuiltin copies the built-in files into a new directory, with the flow
// file's text changed by edit, and returns the directory.
func copyBuiltin(t *testing.T, edit func(flow string) string) string {
	t.Helper()
	return copyBuiltinEditing(t, filepath.Join("flows", "rejected-release.yaml"), edit)
}

// copyBuiltinEditing copies the built-in files into a new directory, with the
// text of the file at rel, relative to the built-in directory, changed by
// edit, and returns the directory.
func copyBuiltinEditing(t *testing.T, file string, edit func(text string) string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, filepath.WalkDir(builtinDir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(builtinDir, p)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(filepath.Clean(p)) //nolint:gosec // G304: a file of the repository's built-in directory.
		if err != nil {
			return err
		}
		if rel == file {
			edited := edit(string(data))
			require.NotEqual(t, string(data), edited, "the edit changes %s", file)
			data = []byte(edited)
		}
		dst := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return err
		}
		return os.WriteFile(dst, data, 0o600) //nolint:gosec // G703: a path under the test's own temporary directory.
	}))
	return dir
}

// A gate is one step that runs the verification: a flow with two gate steps
// would validate each step and then fail every remediation at its gate.
func TestFiles_AGateOfTwoStepsExitsOne(t *testing.T) {
	for name, gate := range map[string]string{
		"two steps":      "gate: [verification_run, one_manifest_kind]",
		"one step twice": "gate: [verification_run, verification_run]",
	} {
		t.Run(name, func(t *testing.T) {
			dir := copyBuiltin(t, func(flow string) string {
				return strings.Replace(flow, "gate: [verification_run]", gate, 1)
			})

			code, stdout, stderr := runFiles(t, "files", dir)

			assert.Equal(t, 1, code, stderr)
			r := decode(t, stdout)
			gateLine := lineOf(t, builtinText(t, "flows/rejected-release.yaml"), "gate:")
			assert.Contains(t, strings.Join(r.Errors, "\n"),
				fmt.Sprintf(`flows/rejected-release.yaml:%d: flow "rejected-release" has 2 gate steps`, gateLine),
				"the refusal names the line of the second gate step")
			assert.Empty(t, r.Scenarios, "the suite is skipped when the files do not validate")
		})
	}
}

// Every heal trigger runs the flow rejected-release. An install file that
// replaces it with a flow that cannot run is refused, as the service refuses it
// at startup, and the built-in flow does not stand in for it.
func TestFiles_AReplacementOfTheTriggerFlowThatCannotRunExitsOne(t *testing.T) {
	for name, tc := range map[string]struct{ from, to, want string }{
		"an unsupported trigger": {
			from: "trigger: remediation.requested", to: "trigger: release.rejected",
			want: `flow "rejected-release" has trigger "release.rejected"`,
		},
		"no gate": {
			from: "gate: [verification_run]", to: "gate: []",
			want: `flow "rejected-release" has no gate`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := copyBuiltin(t, func(flow string) string {
				return strings.Replace(flow, tc.from, tc.to, 1)
			})

			code, stdout, stderr := runFiles(t, "files", dir)

			assert.Equal(t, 1, code, stderr)
			r := decode(t, stdout)
			require.Len(t, r.Errors, 1, "%v", r.Errors)
			assert.Contains(t, r.Errors[0], tc.want)
			assert.Empty(t, r.Scenarios, "the suite is skipped when the files do not validate")
		})
	}
}

// A type runs at least one pre-flight round, so a type that asks for none is
// refused at its agent line, as the service refuses it at startup.
func TestFiles_ATypeWithNoRoundsExitsOne(t *testing.T) {
	dir := copyBuiltinEditing(t, filepath.Join("types", "dbt-compile.yaml"), func(typ string) string {
		return strings.Replace(typ, "rounds: 3", "rounds: 0", 1)
	})

	code, stdout, stderr := runFiles(t, "files", dir)

	assert.Equal(t, 1, code, stderr)
	r := decode(t, stdout)
	require.Len(t, r.Errors, 1, "%v", r.Errors)
	agentLine := lineOf(t, builtinText(t, "types/dbt-compile.yaml"), "agent:")
	assert.Equal(t, fmt.Sprintf(`types/dbt-compile.yaml:%d: type "dbt-compile" has rounds 0, and a type runs at least one pre-flight round, so rounds must be 1 or more`, agentLine), r.Errors[0])
	assert.Empty(t, r.Scenarios, "the suite is skipped when the files do not validate")
}

func TestFiles_ATypeOfOneRoundPasses(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS(builtinDir)))
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })
	const typeFile = "types/dbt-compile.yaml"
	body, err := root.ReadFile(typeFile)
	require.NoError(t, err)
	oneRound := strings.Replace(string(body), "rounds: 3", "rounds: 1", 1)
	require.NotEqual(t, string(body), oneRound)
	require.NoError(t, root.WriteFile(typeFile, []byte(oneRound), 0o600))

	code, stdout, stderr := runFiles(t, "files", dir)

	r := decode(t, stdout)
	require.NotEmpty(t, r.Scenarios)
	for _, s := range r.Scenarios {
		assert.True(t, s.OK, "%s %s: %s", s.Type, s.Scenario, s.Error)
	}
	assert.Equal(t, 0, code, "stderr: %s", stderr)
}

// builtinText is the text of one built-in file, by its path under the built-in
// directory.
func builtinText(t *testing.T, name string) string {
	t.Helper()
	data, err := fs.ReadFile(workflows.Builtin, name)
	require.NoError(t, err)
	return string(data)
}

// writeInstallFile writes an install file named name, which may hold
// directories, under dir.
func writeInstallFile(t *testing.T, dir, name, body string) {
	t.Helper()
	root, err := os.OpenRoot(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })
	if parent := filepath.Dir(name); parent != "." {
		require.NoError(t, root.MkdirAll(parent, 0o750))
	}
	require.NoError(t, root.WriteFile(name, []byte(body), 0o600))
}

// installedType is a type an install adds: it handles the validation failures
// of dbt tests, which no built-in type handles.
const installedType = `kind: type
version: 1
name: my-validation
match: { cluster: single, source: validation, node_types: [dbt-test] }
context: [failing_node_source, dbt_log, failure_precedents]
agent: { prompt: dbt-compile, tools: [], rounds: 2 }
preflight: [content_changed, confidence_not_low]
`

// flowListingTheInstalledType is the built-in flow rejected-release with
// installedType added to the types it lists.
func flowListingTheInstalledType(t *testing.T) string {
	t.Helper()
	const last = "  - cross-service-upstream\n"
	flow := builtinText(t, "flows/rejected-release.yaml")
	require.Contains(t, flow, last)
	return strings.Replace(flow, last, last+"  - my-validation\n", 1)
}

// A type that is new to the install has no built-in fixture to fall back on, so
// every scenario of it fails and says which fixture is missing, while the
// built-in types and flows still run.
func TestFiles_AnInstallTypeWithoutAFixtureFails(t *testing.T) {
	dir := t.TempDir()
	writeInstallFile(t, dir, "my-validation.yaml", installedType)
	writeInstallFile(t, dir, "rejected-release.yaml", flowListingTheInstalledType(t))

	code, stdout, stderr := runFiles(t, "files", dir)

	assert.Equal(t, 1, code, stderr)
	r := decode(t, stdout)
	assert.Empty(t, r.Errors, "the files load and validate, the type only lacks a fixture")
	var own, others []wireScenario
	for _, s := range r.Scenarios {
		if s.Type == "my-validation" {
			own = append(own, s)
		} else {
			others = append(others, s)
		}
	}
	require.Len(t, own, len(conformance.TypeScenarios()), "a type is played through every type scenario")
	for _, s := range own {
		assert.False(t, s.OK, s.Scenario)
		assert.Contains(t, s.Error, "type my-validation has no fixture", s.Scenario)
	}
	require.Len(t, others, builtinScenarioCount(t), "every built-in scenario is still in the report")
	for _, s := range others {
		assert.True(t, s.OK, "%s %s: %s", s.Type, s.Scenario, s.Error)
	}
}

// An install type with the name of a built-in replaces it, and the replacement
// goes through the same validation and scenarios as any file.
func TestFiles_AnInstallReplacementIsChecked(t *testing.T) {
	const typeFile = "types/dbt-compile.yaml"

	t.Run("a replacement naming an unknown step exits one with its file and line", func(t *testing.T) {
		dir := t.TempDir()
		writeInstallFile(t, dir, typeFile, strings.Replace(builtinText(t, typeFile),
			"preflight: [target_file_was_shown, content_changed, confidence_not_low]",
			"preflight: [target_file_was_shown, content_changed, no_such_step]", 1))

		code, stdout, stderr := runFiles(t, "files", dir)

		assert.Equal(t, 1, code, stderr)
		r := decode(t, stdout)
		require.Len(t, r.Errors, 1, "%v", r.Errors)
		assert.Regexp(t, `^types/dbt-compile\.yaml:[0-9]+: unknown step "no_such_step"`, r.Errors[0])
		assert.Empty(t, r.Scenarios, "the suite is skipped when the files do not validate")
	})

	t.Run("a valid replacement passes and is noted in the warnings", func(t *testing.T) {
		dir := t.TempDir()
		writeInstallFile(t, dir, typeFile, strings.Replace(builtinText(t, typeFile), "rounds: 3", "rounds: 2", 1))

		code, stdout, stderr := runFiles(t, "files", dir)

		r := decode(t, stdout)
		require.Equal(t, 0, code, "stderr: %s\nstdout: %s", stderr, stdout)
		assert.Empty(t, r.Errors)
		assert.Equal(t, []string{"type dbt-compile: replaced by " + typeFile}, r.Warnings)
		var replaced int
		for _, s := range r.Scenarios {
			assert.True(t, s.OK, "%s %s: %s", s.Type, s.Scenario, s.Error)
			if s.Type == "dbt-compile" {
				replaced++
			}
		}
		assert.Equal(t, len(conformance.TypeScenarios()), replaced, "the replacement is played through every type scenario, with the built-in fixture")
	})
}

// A fixture an install ships replaces the built-in fixture of its type, so the
// type is played against the install's fixture and the report names what that
// fixture makes fail.
func TestFiles_AnInstallFixtureReplacesTheBuiltInFixtureOfItsType(t *testing.T) {
	const fixtureFile = "fixtures/types/dbt-compile.yaml"
	const shown = "  pass:\n    - target_file: services/svc/models/x.sql\n"
	fixture := builtinText(t, fixtureFile)
	require.Contains(t, fixture, shown)
	dir := t.TempDir()
	// The passing answer now edits a file the model was not shown, which
	// pre-flight refuses.
	writeInstallFile(t, dir, "my-compile-fixture.yaml",
		strings.Replace(fixture, shown, "  pass:\n    - target_file: services/svc/models/other.sql\n", 1))

	code, stdout, stderr := runFiles(t, "files", dir)

	assert.Equal(t, 1, code, stderr)
	r := decode(t, stdout)
	assert.Empty(t, r.Errors)
	require.Len(t, r.Scenarios, builtinScenarioCount(t))
	var failedOfCompile int
	for _, s := range r.Scenarios {
		switch {
		case s.Type != "dbt-compile":
			assert.True(t, s.OK, "%s %s: %s", s.Type, s.Scenario, s.Error)
		case !s.OK:
			failedOfCompile++
		}
	}
	assert.Positive(t, failedOfCompile, "the install's fixture is the one dbt-compile is played against")
}

// withoutDBTValidation is the folder of the install fixtures a rejected-release
// flow that lists no dbt-validation needs: a shared-upstream type fixture and
// the flow fixtures whose built-in failures dbt-validation fixes.
const withoutDBTValidation = "testdata/without-dbt-validation"

// flowWithoutDBTValidation is the built-in flow rejected-release with
// dbt-validation dropped from the types it lists.
func flowWithoutDBTValidation(t *testing.T) string {
	t.Helper()
	const listed = "  - dbt-validation\n"
	flow := builtinText(t, "flows/rejected-release.yaml")
	require.Contains(t, flow, listed)
	return strings.Replace(flow, listed, "", 1)
}

// failedScenarios is the "<type> <scenario>" of each scenario that failed.
func failedScenarios(r wireReport) []string {
	var failed []string
	for _, s := range r.Scenarios {
		if !s.OK {
			failed = append(failed, s.Type+" "+s.Scenario)
		}
	}
	return failed
}

// An install whose rejected-release drops a built-in type cannot remove that
// type's built-in fixture. Nothing plays it, so it is left out of the
// scenarios with a warning. The built-in type and flow fixtures of what the
// flow still lists are played, and the ones whose failures the dropped type
// fixed fail until the install ships its own fixture of the same type, or of
// the same flow and scenario, which replaces the built-in one.
func TestFiles_AnInstallFlowThatDropsABuiltInTypeSkipsItsBuiltInFixture(t *testing.T) {
	const skipped = "fixtures/types/dbt-validation.yaml: skipped, no loaded flow lists type dbt-validation"
	flowFixtures := []string{"two-types-one-service.yaml", "two-services.yaml", "shared-upstream-declines.yaml"}

	for name, tc := range map[string]struct {
		fixtures []string
		failed   []string
	}{
		"with the install's fixtures it passes": {
			fixtures: append([]string{"shared-upstream.yaml"}, flowFixtures...),
		},
		"without the install's flow fixtures the built-in flow fixtures fail": {
			fixtures: []string{"shared-upstream.yaml"},
			failed: []string{
				"rejected-release two_types_one_service",
				"rejected-release two_services",
				"rejected-release shared_upstream_declines",
			},
		},
		"without the install's shared-upstream fixture the built-in one fails": {
			fixtures: flowFixtures,
			failed: []string{
				"shared-upstream preflight_rounds_spent",
				"shared-upstream declines_before_model",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeInstallFile(t, dir, "rejected-release.yaml", flowWithoutDBTValidation(t))
			for _, f := range tc.fixtures {
				body, err := os.ReadFile(filepath.Join(withoutDBTValidation, f)) //nolint:gosec // G304: a file of this test's testdata folder.
				require.NoError(t, err)
				writeInstallFile(t, dir, f, string(body))
			}

			code, stdout, stderr := runFiles(t, "files", dir)

			r := decode(t, stdout)
			assert.Empty(t, r.Errors)
			assert.Contains(t, r.Warnings, "flow rejected-release: replaced by rejected-release.yaml")
			assert.Contains(t, r.Warnings, skipped)
			assert.Equal(t, skipped, r.Warnings[len(r.Warnings)-1], "the skipped fixtures come last")
			for _, s := range r.Scenarios {
				assert.NotEqual(t, "dbt-validation", s.Type, "the dropped type's built-in fixture is not played")
			}
			assert.Equal(t, tc.failed, failedScenarios(r))
			if tc.failed == nil {
				assert.Equal(t, 0, code, "stderr: %s\nstdout: %s", stderr, stdout)
			} else {
				assert.Equal(t, 1, code, stderr)
			}
		})
	}
}

// Only a built-in fixture is left out when nothing plays it. A fixture the
// install ships for a type no flow lists is the install's mistake, and fails,
// even when it sits at the path of the built-in fixture it replaces.
func TestFiles_AnInstallFixtureOfATypeNoFlowListsFails(t *testing.T) {
	compile := builtinText(t, "fixtures/types/dbt-compile.yaml")
	require.Contains(t, compile, "type: dbt-compile\n")

	t.Run("a type no file defines", func(t *testing.T) {
		dir := t.TempDir()
		writeInstallFile(t, dir, "not-listed.yaml", strings.Replace(compile, "type: dbt-compile\n", "type: not-listed\n", 1))

		code, stdout, stderr := runFiles(t, "files", dir)

		assert.Equal(t, 1, code, stderr)
		r := decode(t, stdout)
		assert.Empty(t, r.Errors)
		assert.Empty(t, r.Warnings, "an install's fixture is never skipped")
		assert.Contains(t, r.Scenarios, wireScenario{Type: "not-listed", Error: "not-listed.yaml tests type not-listed, which no flow lists"})
		assert.Equal(t, []string{"not-listed "}, failedScenarios(r))
	})

	t.Run("a built-in type the install's flow drops, at the built-in fixture's path", func(t *testing.T) {
		const path = "fixtures/types/dbt-validation.yaml"
		dir := copyBuiltin(t, func(string) string { return flowWithoutDBTValidation(t) })
		writeInstallFile(t, dir, path, builtinText(t, path)+"# the install's copy\n")

		code, stdout, stderr := runFiles(t, "files", dir)

		assert.Equal(t, 1, code, stderr)
		r := decode(t, stdout)
		assert.Empty(t, r.Errors)
		for _, w := range r.Warnings {
			assert.NotContains(t, w, "skipped", "the install's fixture replaced the built-in one, so none is skipped")
		}
		assert.Contains(t, r.Scenarios, wireScenario{Type: "dbt-validation", Error: path + " tests type dbt-validation, which no flow lists"})
	})
}

// Every heal trigger runs rejected-release, so a flow of another name is noted:
// it is loaded, validated and played, but no remediation follows it.
func TestFiles_AFlowNoHealTriggerRunsIsNoted(t *testing.T) {
	dir := t.TempDir()
	writeInstallFile(t, dir, "my-flow.yaml", strings.Replace(builtinText(t, "flows/rejected-release.yaml"),
		"name: rejected-release\n", "name: my-flow\n", 1))

	code, stdout, stderr := runFiles(t, "files", dir)

	assert.Equal(t, 1, code, "the flow has no fixture of its own: %s", stderr)
	r := decode(t, stdout)
	assert.Empty(t, r.Errors)
	assert.Equal(t, []string{"flow my-flow: no heal trigger runs it"}, r.Warnings)
	var played int
	for _, s := range r.Scenarios {
		if s.Type == "my-flow" {
			played++
			assert.Equal(t, "flow my-flow has no fixture for scenario "+s.Scenario, s.Error)
		}
	}
	assert.Equal(t, len(conformance.FlowScenarios()), played, "the flow is still played")
}

// writeSecondFlow writes the install flow second-flow, the built-in flow
// rejected-release renamed with edit applied, and a copy of every built-in
// flow fixture that plays second-flow.
func writeSecondFlow(t *testing.T, dir string, edit func(flow string) string) {
	t.Helper()
	flow := builtinText(t, "flows/rejected-release.yaml")
	require.Contains(t, flow, "name: rejected-release\n")
	writeInstallFile(t, dir, "second-flow.yaml", edit(strings.Replace(flow, "name: rejected-release\n", "name: second-flow\n", 1)))
	const fixtures = "fixtures/flows/rejected-release"
	entries, err := fs.ReadDir(workflows.Builtin, fixtures)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	for _, e := range entries {
		body := builtinText(t, fixtures+"/"+e.Name())
		require.Contains(t, body, "flow: rejected-release\n")
		writeInstallFile(t, dir, "second-flow-"+e.Name(), strings.Replace(body, "flow: rejected-release\n", "flow: second-flow\n", 1))
	}
}

// An install's second flow is played as itself: each of its scenarios runs a
// remediation that follows it. A second flow that lists one type fails the
// scenarios it cannot pass with fixtures copied from rejected-release, while a
// faithful copy of rejected-release passes them all.
func TestFiles_ASecondFlowIsPlayedAsItself(t *testing.T) {
	t.Run("a flow of one type fails the scenarios it cannot pass", func(t *testing.T) {
		dir := t.TempDir()
		writeSecondFlow(t, dir, func(flow string) string {
			head, rest, found := strings.Cut(flow, "types:\n")
			require.True(t, found)
			for strings.HasPrefix(rest, "  - ") {
				_, rest, _ = strings.Cut(rest, "\n")
			}
			return head + "types:\n  - dbt-compile\n" + rest
		})

		code, stdout, stderr := runFiles(t, "files", dir)

		assert.Equal(t, 1, code, stderr)
		r := decode(t, stdout)
		assert.Empty(t, r.Errors)
		assert.Equal(t, []string{"flow second-flow: no heal trigger runs it"}, r.Warnings)
		failed := failedScenarios(r)
		assert.Contains(t, failed, "second-flow two_types_one_service")
		for _, f := range failed {
			assert.True(t, strings.HasPrefix(f, "second-flow "), "only second-flow's scenarios fail: %s", f)
		}
	})
	t.Run("a faithful copy of rejected-release passes", func(t *testing.T) {
		dir := t.TempDir()
		writeSecondFlow(t, dir, func(flow string) string { return flow })

		code, stdout, stderr := runFiles(t, "files", dir)

		assert.Equal(t, 0, code, stderr)
		r := decode(t, stdout)
		assert.Empty(t, r.Errors)
		assert.Empty(t, failedScenarios(r))
		var played int
		for _, s := range r.Scenarios {
			if s.Type == "second-flow" {
				played++
			}
		}
		assert.Equal(t, len(conformance.FlowScenarios()), played)
	})
}

// An install file at the path of a built-in file that leaves the built-in's
// type loaded would make one path name two files, so it is an error that names
// both definitions, and the scenarios are skipped.
func TestFiles_AnInstallFileAtABuiltInPathThatLeavesItLoadedExitsOne(t *testing.T) {
	dir := t.TempDir()
	body := strings.Replace(builtinText(t, "types/dbt-compile.yaml"), "name: dbt-compile\n", "name: my-compile\n", 1)
	writeInstallFile(t, dir, "types/dbt-compile.yaml", body)

	code, stdout, stderr := runFiles(t, "files", dir)

	assert.Equal(t, 1, code, stderr)
	r := decode(t, stdout)
	assert.Equal(t, []string{fmt.Sprintf(`types/dbt-compile.yaml:%d: type "my-compile" is in a file with the path of the built-in file that defines type "dbt-compile", which stays loaded; give the install file another path`,
		lineOf(t, body, "kind: type"))}, r.Errors)
	assert.Empty(t, r.Scenarios)
}

// A flow fixture whose flow no file defines (a mistyped name) is played by no
// flow, so it fails with a result that names its file.
func TestFiles_AFlowFixtureOfAFlowNoFileDefinesFails(t *testing.T) {
	const fixture = "fixtures/flows/rejected-release/maintenance.yaml"
	body := builtinText(t, fixture)
	require.Contains(t, body, "flow: rejected-release\n")
	dir := t.TempDir()
	writeInstallFile(t, dir, "maintenance.yaml", strings.Replace(body, "flow: rejected-release\n", "flow: rejected-relase\n", 1))

	code, stdout, stderr := runFiles(t, "files", dir)

	assert.Equal(t, 1, code, stderr)
	r := decode(t, stdout)
	assert.Empty(t, r.Errors)
	assert.Equal(t, []string{"rejected-relase maintenance"}, failedScenarios(r))
	assert.Contains(t, r.Scenarios, wireScenario{Type: "rejected-relase", Scenario: "maintenance",
		Error: "maintenance.yaml tests flow rejected-relase, which no file defines"})
}

// Both loaders read every install file, so a file neither can parse is one
// error in the report, not one per loader.
func TestFiles_AFileNeitherLoaderCanParseIsReportedOnce(t *testing.T) {
	dir := t.TempDir()
	writeInstallFile(t, dir, "broken.yaml", "kind: type\nname: [unclosed\n")

	code, stdout, stderr := runFiles(t, "files", dir)

	assert.Equal(t, 1, code, stderr)
	r := decode(t, stdout)
	require.Len(t, r.Errors, 1, "%v", r.Errors)
	assert.Contains(t, r.Errors[0], "broken.yaml")
}

// What the load notes of an install's files (a replacement, a based_on that no
// longer matches) go to the report's warnings, before the validator's own.
func TestFiles_AWarningFromTheLoadReachesTheReport(t *testing.T) {
	builtin, err := workflowfile.Load(workflows.Builtin)
	require.NoError(t, err)
	current := builtin.Types["dbt-compile"].Revision
	dir := t.TempDir()
	writeInstallFile(t, dir, "copy.yaml", "based_on: dbt-compile@000000000000\n"+
		strings.Replace(builtinText(t, "types/dbt-compile.yaml"), "rounds: 3", "rounds: 2", 1))
	writeInstallFile(t, dir, "unlisted.yaml", strings.Replace(installedType, "name: my-validation", "name: unlisted", 1))

	code, stdout, stderr := runFiles(t, "files", dir)

	r := decode(t, stdout)
	require.Equal(t, 0, code, "stderr: %s\nstdout: %s", stderr, stdout)
	assert.Empty(t, r.Errors)
	require.Len(t, r.Warnings, 3, "%v", r.Warnings)
	assert.Equal(t, "type dbt-compile: replaced by copy.yaml", r.Warnings[0])
	assert.Equal(t, "copy.yaml: based on dbt-compile@000000000000, which is now @"+current, r.Warnings[1])
	assert.Regexp(t, `^unlisted\.yaml:[0-9]+: type "unlisted" is listed by no flow$`, r.Warnings[2])
}

// wireStep is one entry of the array `steps` prints, as a reader decodes it.
type wireStep struct {
	Name              string   `json:"name"`
	Kind              string   `json:"kind"`
	Description       string   `json:"description"`
	Needs             []string `json:"needs"`
	Provides          []string `json:"provides"`
	Stages            []string `json:"stages"`
	MayStandAsGateRun bool     `json:"may_stand_as_gate_run"`
	DataClass         []string `json:"data_class"`
}

func decodeSteps(t *testing.T, stdout string) []wireStep {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(stdout))
	dec.DisallowUnknownFields()
	var steps []wireStep
	require.NoError(t, dec.Decode(&steps), stdout)
	require.False(t, dec.More(), "stdout holds more than one JSON value")
	return steps
}

// printedSteps runs `steps` and returns what it printed, after checking that it
// succeeded.
func printedSteps(t *testing.T) []wireStep {
	t.Helper()
	code, stdout, stderr := runFiles(t, "steps")
	require.Equal(t, 0, code, stderr)
	return decodeSteps(t, stdout)
}

func TestSteps_ListsEveryRegisteredStep(t *testing.T) {
	var want []string
	for _, m := range runner.NewRegistry(runner.Collaborators{}).All() {
		want = append(want, string(m.Name))
	}
	require.NotEmpty(t, want)

	var got []string
	for _, s := range printedSteps(t) {
		got = append(got, s.Name)
	}

	assert.Equal(t, want, got, "every registered step, sorted by name")
	assert.True(t, slices.IsSorted(got))
}

func TestSteps_EveryEntryDescribesItself(t *testing.T) {
	kinds := []string{string(step.KindContext), string(step.KindPrompt), string(step.KindCheck), string(step.KindApprover), string(step.KindOutput)}
	stages := []string{
		string(workflow.StageContext), string(workflow.StagePrompt), string(workflow.StagePreflight),
		string(workflow.StageCrossCluster), string(workflow.StageGate), string(workflow.StageApprover), string(workflow.StageOutput),
	}
	seen := map[string]bool{}
	for _, s := range printedSteps(t) {
		seen[s.Kind] = true
		assert.NotEmpty(t, s.Name)
		assert.NotEmpty(t, strings.TrimSpace(s.Description), s.Name)
		assert.Contains(t, kinds, s.Kind, s.Name)
		if s.Kind == string(step.KindCheck) {
			assert.NotEmpty(t, s.Stages, "%s: a check says where it may stand", s.Name)
		}
		for _, stage := range s.Stages {
			assert.Contains(t, stages, stage, s.Name)
		}
	}
	for _, kind := range kinds {
		assert.True(t, seen[kind], "the catalog holds a step of kind %s", kind)
	}
}

// data_class is read from the registry's evidence classes: the classes of the
// keys a step provides and, for a prompt, also of the keys it reads.
func TestSteps_DataClassFollowsTheEvidenceClasses(t *testing.T) {
	reg := runner.NewRegistry(runner.Collaborators{})
	classesOf := func(keys ...[]remediation.EvidenceKey) []string {
		held := map[workflow.DataClass]bool{}
		for _, ks := range keys {
			for _, k := range ks {
				if c, ok := reg.EvidenceClass(k); ok {
					held[c] = true
				}
			}
		}
		out := []string{}
		for _, c := range workflow.DataClasses() {
			if held[c] {
				out = append(out, string(c))
			}
		}
		return out
	}

	steps := printedSteps(t)
	byName := map[string]wireStep{}
	for _, s := range steps {
		byName[s.Name] = s
	}
	var readsMoreThanItProvides int
	for _, m := range reg.All() {
		got := byName[string(m.Name)]
		want := classesOf(m.Provides)
		if m.Kind == step.KindPrompt {
			want = classesOf(m.Provides, m.Needs)
		} else if len(classesOf(m.Needs)) > len(want) {
			readsMoreThanItProvides++
		}
		assert.Equal(t, want, got.DataClass, m.Name)
	}
	assert.Positive(t, readsMoreThanItProvides, "a step that is no prompt reads a class it does not provide, and that class stays out of its data_class")

	// A context step that provides logs, one that provides code, and a prompt
	// whose reads add a class its own evidence lacks.
	assert.Equal(t, []string{"logs"}, byName["dbt_log"].DataClass)
	assert.Equal(t, []string{"code"}, byName["candidate_sql"].DataClass)
	assert.Empty(t, byName["dbt-compile"].Provides)
	assert.Equal(t, []string{"code", "logs"}, byName["dbt-compile"].DataClass)
	assert.Empty(t, byName["content_changed"].DataClass, "a check provides no evidence")
}

func TestSteps_NeverNull(t *testing.T) {
	_, stdout, _ := runFiles(t, "steps")

	var raw []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(stdout), &raw), stdout)
	require.NotEmpty(t, raw)
	empty := map[string]bool{}
	for _, entry := range raw {
		for _, field := range []string{"needs", "provides", "stages", "data_class"} {
			value := string(entry[field])
			assert.NotEqual(t, "null", value, "%s of %s", field, entry["name"])
			if value == "[]" {
				empty[field] = true
			}
		}
	}
	for _, field := range []string{"needs", "provides", "stages", "data_class"} {
		assert.True(t, empty[field], "some step has no %s, and prints []", field)
	}
}

func TestSteps_FieldsFollowTheDocumentedOrder(t *testing.T) {
	_, stdout, _ := runFiles(t, "steps")

	dec := json.NewDecoder(strings.NewReader(stdout))
	tok, err := dec.Token()
	require.NoError(t, err)
	require.Equal(t, json.Delim('['), tok, "stdout is one JSON array")
	tok, err = dec.Token()
	require.NoError(t, err)
	require.Equal(t, json.Delim('{'), tok)
	var keys []string
	for dec.More() {
		key, err := dec.Token()
		require.NoError(t, err)
		keys = append(keys, key.(string))
		var skipped json.RawMessage
		require.NoError(t, dec.Decode(&skipped))
	}
	assert.Equal(t, []string{"name", "kind", "description", "needs", "provides", "stages", "may_stand_as_gate_run", "data_class"}, keys)
}

func TestSteps_OutputIsDeterministic(t *testing.T) {
	code, first, _ := runFiles(t, "steps")
	require.Equal(t, 0, code)
	_, second, _ := runFiles(t, "steps")

	assert.Equal(t, first, second)
}

func TestSteps_TakesNoArguments(t *testing.T) {
	code, stdout, stderr := runFiles(t, "steps", "extra")

	assert.Equal(t, 2, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "usage: remediation-check")
	assert.Contains(t, stderr, "remediation-check steps")
}

// Each subcommand says what it is for, what it takes, what it prints and how
// it exits, whether it is asked through `help`, `-h` or `--help`, and the
// overview holds each of those blocks.
func TestHelp_EachSubcommandStatesItsPurposeArgumentsOutputAndExitCodes(t *testing.T) {
	askedFor := func(args ...string) string {
		t.Helper()
		code, stdout, stderr := runFiles(t, args...)
		require.Equal(t, 0, code, "%v: %s", args, stderr)
		assert.Empty(t, stderr, "%v: help goes to stdout alone", args)
		require.NotEmpty(t, stdout, "%v", args)
		return stdout
	}
	sections := []string{"Purpose", "Arguments", "Stdout", "Exit codes"}

	blocks := map[string]string{}
	for sub, ways := range map[string][][]string{
		"files": {{"help", "files"}, {"files", "-h"}, {"files", "--help"}},
		"steps": {{"help", "steps"}, {"steps", "-h"}, {"steps", "--help"}},
	} {
		block := askedFor(ways[0]...)
		blocks[sub] = block
		for _, way := range ways[1:] {
			assert.Equal(t, block, askedFor(way...), "%v prints what %v prints", way, ways[0])
		}
		for _, section := range sections {
			assert.Contains(t, block, "\n"+section+"\n", "%s help has a %s section", sub, section)
		}
		assert.True(t, strings.HasPrefix(block, "remediation-check "+sub), block)
	}

	files, steps := blocks["files"], blocks["steps"]
	assert.Contains(t, files, "<dir>")
	assert.Contains(t, files, `"scenarios"`)
	assert.Contains(t, files, "distinct files")
	assert.Contains(t, files, "built-in files included")
	for _, code := range []string{"0  ", "1  ", "2  "} {
		assert.Contains(t, files, "\n  "+code, "files help lists exit code %s", strings.TrimSpace(code))
	}
	assert.Contains(t, steps, "None.")
	assert.Contains(t, steps, `"may_stand_as_gate_run"`)
	assert.Contains(t, steps, `"data_class"`)
	assert.Contains(t, steps, "sorted by name")
	for _, code := range []string{"0  ", "2  "} {
		assert.Contains(t, steps, "\n  "+code, "steps help lists exit code %s", strings.TrimSpace(code))
	}
	assert.NotContains(t, files, "remediation-check steps")
	assert.NotContains(t, steps, "remediation-check files")

	for _, way := range [][]string{{"help"}, {"-h"}, {"--help"}} {
		overview := askedFor(way...)
		assert.Contains(t, overview, files, "%v: the overview holds the files block", way)
		assert.Contains(t, overview, steps, "%v: the overview holds the steps block", way)
	}
}

func TestHelp_AMisusedHelpIsAUsageError(t *testing.T) {
	for name, args := range map[string][]string{
		"an unknown subcommand": {"help", "verify"},
		"two subcommands":       {"help", "files", "steps"},
		"an option and a name":  {"--help", "files"},
		"help of steps, extra":  {"steps", "--help", "more"},
		"help of files, extra":  {"files", "-h", "more"},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runFiles(t, args...)

			assert.Equal(t, 2, code, stderr)
			assert.Empty(t, stdout)
			assert.Contains(t, stderr, "usage: remediation-check files <dir>")
		})
	}
}
