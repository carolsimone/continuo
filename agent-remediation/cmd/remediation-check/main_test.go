package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	// 1 flow, 10 types, 10 type fixtures and 7 flow fixtures.
	assert.Equal(t, 28, r.Files)
	// 10 types x 7 type scenarios, then 1 flow x 7 flow scenarios.
	require.Len(t, r.Scenarios, 77)
	for _, s := range r.Scenarios {
		assert.True(t, s.OK, "%s %s: %s", s.Type, s.Scenario, s.Error)
		assert.Empty(t, s.Error)
	}
	assert.Equal(t, "rejected-release", r.Scenarios[len(r.Scenarios)-1].Type)
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
output:
  on_pass: { action: pull_request, emits: remediation.proposed }
  on_give_up: { action: hand_over }
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
	assert.Equal(t, 1, r.Files)
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

func TestFiles_AnEmptyDirExitsOne(t *testing.T) {
	code, stdout, _ := runFiles(t, "files", t.TempDir())

	assert.Equal(t, 1, code)
	r := decode(t, stdout)
	assert.NotEmpty(t, r.Errors, "a directory with no flow is not a passing gate")
	assert.Zero(t, r.Files)
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
			assert.Contains(t, strings.Join(r.Errors, "\n"), `flows/rejected-release.yaml:22: flow "rejected-release" has 2 gate steps`,
				"the refusal names the line of the second gate step")
			assert.Empty(t, r.Scenarios, "the suite is skipped when the files do not validate")
		})
	}
}

// Every heal trigger runs the flow rejected-release, so files that do not
// define it are refused, as the service refuses them at startup.
func TestFiles_FilesWithoutTheTriggerFlowExitOne(t *testing.T) {
	dir := copyBuiltin(t, func(flow string) string {
		return strings.Replace(flow, "\nname: rejected-release\n", "\nname: another-flow\n", 1)
	})

	code, stdout, stderr := runFiles(t, "files", dir)

	assert.Equal(t, 1, code, stderr)
	r := decode(t, stdout)
	require.Len(t, r.Errors, 1, "%v", r.Errors)
	assert.Contains(t, r.Errors[0], `"rejected-release"`)
	assert.Empty(t, r.Scenarios)
}

// The runner runs one pre-flight round per try, so a type that asks for two is
// refused at its agent line, as the service refuses it at startup.
func TestFiles_ATypeWithTwoRoundsExitsOne(t *testing.T) {
	dir := copyBuiltinEditing(t, filepath.Join("types", "dbt-compile.yaml"), func(typ string) string {
		return strings.Replace(typ, "rounds: 1", "rounds: 2", 1)
	})

	code, stdout, stderr := runFiles(t, "files", dir)

	assert.Equal(t, 1, code, stderr)
	r := decode(t, stdout)
	require.Len(t, r.Errors, 1, "%v", r.Errors)
	assert.Equal(t, `types/dbt-compile.yaml:8: type "dbt-compile" has rounds 2, and the runner runs one pre-flight round per try, so rounds must be 1`, r.Errors[0])
	assert.Empty(t, r.Scenarios, "the suite is skipped when the files do not validate")
}
