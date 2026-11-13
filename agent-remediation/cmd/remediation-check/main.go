// remediation-check checks the workflow files of agent-remediation, in the
// image beside the service binary and in CI.
//
//	remediation-check files <dir>
//
// files loads the flow, type and fixture files under <dir>, validates them
// against the steps the runner registers, checks that they define the flow
// every heal trigger runs, and runs the conformance suite over them: every type a flow lists through every type scenario, and every flow
// through every flow scenario, in worlds of scripted fakes over the memory
// store. When the files do not load or do not validate it reports the errors
// and skips the suite.
//
// stdout carries one JSON object and nothing else:
//
//	{"files": int, "errors": [string], "warnings": [string],
//	 "scenarios": [{"type": string, "scenario": string, "ok": bool, "error": string}]}
//
// files counts the distinct files that hold a flow, a type or a fixture. A
// flow scenario names its flow in "type". Human text goes to stderr.
//
// Exit codes: 0 no error and every scenario passed, 1 an error or a failed
// scenario, 2 usage (no arguments, an unknown subcommand, a missing or extra
// argument, a directory that does not exist).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/adapters/scripted"
	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
)

const usage = "usage: remediation-check files <dir>"

// Exit codes.
const (
	exitOK    = 0
	exitFail  = 1
	exitUsage = 2
)

// report is the JSON object `files` prints. Its fields are printed in this
// order, and the slices are never nil.
type report struct {
	Files     int              `json:"files"`
	Errors    []string         `json:"errors"`
	Warnings  []string         `json:"warnings"`
	Scenarios []scenarioResult `json:"scenarios"`
}

type scenarioResult struct {
	Type     string `json:"type"`
	Scenario string `json:"scenario"`
	OK       bool   `json:"ok"`
	Error    string `json:"error"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes the command in args, writes the JSON report to stdout and human
// text to stderr, and returns the exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 || args[0] != "files" {
		_, _ = fmt.Fprintln(stderr, usage)
		return exitUsage
	}
	dir := filepath.Clean(args[1])
	if info, err := os.Stat(dir); err != nil || !info.IsDir() { //nolint:gosec // G703: the operator names the directory to check on the command line.
		_, _ = fmt.Fprintf(stderr, "%s is not a directory\n%s\n", dir, usage)
		return exitUsage
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	rep := checkFiles(context.Background(), dir)
	if err := json.NewEncoder(stdout).Encode(rep); err != nil {
		logger.Error("write the report", "error", err)
		return exitFail
	}

	failed := 0
	for _, s := range rep.Scenarios {
		if !s.OK {
			failed++
		}
	}
	logger.Info("checked", "dir", dir, "files", rep.Files, "errors", len(rep.Errors),
		"warnings", len(rep.Warnings), "scenarios", len(rep.Scenarios), "failed_scenarios", failed)
	if len(rep.Errors) > 0 || failed > 0 {
		return exitFail
	}
	return exitOK
}

// checkFiles loads, validates and runs the conformance suite over the files
// under dir.
func checkFiles(ctx context.Context, dir string) report {
	rep := report{Errors: []string{}, Warnings: []string{}, Scenarios: []scenarioResult{}}
	fsys := os.DirFS(dir)

	set, loadErr := workflowfile.Load(fsys)
	fixtures, fixtureErr := workflowfile.LoadFixtures(fsys)
	rep.Files = countFiles(set, fixtures)
	rep.Errors = append(rep.Errors, messages(loadErr)...)
	rep.Errors = append(rep.Errors, messages(fixtureErr)...)
	if len(rep.Errors) > 0 {
		return rep
	}

	if len(set.Flows) == 0 {
		rep.Errors = append(rep.Errors, "no flow file under "+dir)
		return rep
	}
	warnings, err := runner.Validate(set, runner.NewRegistry(runner.Collaborators{}))
	rep.Warnings = append(rep.Warnings, warnings...)
	rep.Errors = append(rep.Errors, messages(err)...)
	if _, err := runner.TriggerFlow(set); err != nil {
		rep.Errors = append(rep.Errors, err.Error())
	}
	if len(rep.Errors) > 0 {
		return rep
	}

	locator := repofs.NewLocator(slog.New(slog.DiscardHandler))
	suite := conformance.Suite{
		Files:     set,
		Fixtures:  fixtures,
		Locator:   locator,
		Inspector: locator,
		NewWorld:  scripted.Worlds(memory.NewStore),
	}
	results := suite.Run(ctx)
	flows := make([]string, 0, len(set.Flows))
	for name := range set.Flows {
		flows = append(flows, name)
	}
	sort.Strings(flows)
	for _, name := range flows {
		results = append(results, suite.RunFlowScenarios(ctx, name)...)
	}
	for _, r := range results {
		s := scenarioResult{Type: r.Type, Scenario: string(r.Scenario), OK: r.Err == nil}
		if r.Err != nil {
			s.Error = r.Err.Error()
		}
		rep.Scenarios = append(rep.Scenarios, s)
	}
	return rep
}

// countFiles is the number of distinct files that hold a loaded flow, type or
// fixture.
func countFiles(set workflow.Set, fixtures []conformance.Fixture) int {
	seen := map[string]bool{}
	for _, f := range set.Flows {
		seen[f.Pos.File] = true
	}
	for _, t := range set.Types {
		seen[t.Pos.File] = true
	}
	for _, f := range fixtures {
		seen[f.File] = true
	}
	return len(seen)
}

// messages splits a joined error into one message per error it joins.
func messages(err error) []string {
	if err == nil {
		return nil
	}
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		var out []string
		for _, e := range joined.Unwrap() {
			out = append(out, messages(e)...)
		}
		return out
	}
	return []string{err.Error()}
}
