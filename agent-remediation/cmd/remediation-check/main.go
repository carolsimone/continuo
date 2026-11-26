// remediation-check checks the workflow files of agent-remediation and lists
// the steps they may name. It ships in the image beside the component binary, for
// an install to check its own files, and the repository's tests run it on the
// built-in files.
//
//	remediation-check files <dir>
//	remediation-check steps
//	remediation-check help [files|steps]
//
// files loads the files the component would load with <dir> as its install
// folder: the built-in flow, type and fixture files, then the ones under <dir>
// over them (an install file with the kind and name of a built-in replaces it
// whole, any other file is added, and one at the path of a built-in file whose
// flow, type or fixture stays loaded, of whatever kind the install file is,
// is an error). It validates them against the steps
// the runner registers, checks that they define the flow every heal trigger
// runs, and runs the conformance suite over them: every type a flow lists
// through every type scenario, in rejected-release when it lists the type, else
// in the first flow by name that lists it, and every flow through every flow
// scenario, each remediation following the flow it plays, in worlds of scripted
// fakes over the memory store. A built-in type fixture of a type that no loaded
// flow lists is left out of the suite, with a warning, while an install's
// fixture that nothing plays (one of a type no flow lists, or of a flow no file
// defines) fails. When the files do not load or do not validate it reports the
// errors, each message once, and skips the suite.
//
// files prints one JSON object to stdout and nothing else:
//
//	{"files": int, "errors": [string], "warnings": [string],
//	 "scenarios": [{"type": string, "scenario": string, "ok": bool, "error": string}]}
//
// "files" counts the distinct files that hold a flow, a type or a fixture of
// the merged set, the built-in files included. "warnings" holds the notes of
// the load (an install file that replaces a built-in, a based_on whose built-in
// has changed or does not exist), the validator's warnings, a note for each
// flow no heal trigger runs, and then a note for each built-in fixture left
// out of the suite. A flow scenario names its flow in "type". Human text goes
// to stderr.
//
// steps prints one JSON array to stdout and nothing else, one object per step
// the runner registers, sorted by name:
//
//	{"name": string, "kind": string, "description": string,
//	 "needs": [string], "provides": [string], "stages": [string],
//	 "may_stand_as_gate_run": bool, "data_class": [string]}
//
// "needs" and "provides" are the evidence keys the step reads and writes,
// "stages" the stages it declares it may stand at, and "data_class" the data
// classes (code, logs, rows) of the evidence it puts in front of the model: the
// keys it provides and, for a prompt, the keys it reads. Every array is [] when
// empty. steps takes no argument.
//
// help, -h and --help print the overview of both subcommands; help <sub>,
// <sub> -h and <sub> --help print one subcommand's block. Help goes to stdout.
//
// Exit codes: 0 files found no error and every scenario passed, steps printed
// the catalog, or help was printed; 1 files found an error or a scenario
// failed; 2 usage (no arguments, an unknown subcommand, a missing or extra
// argument, a directory that does not exist), with the usage on stderr and
// nothing on stdout.
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
	"slices"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/adapters/scripted"
	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
)

const usage = `usage: remediation-check files <dir>
       remediation-check steps
       remediation-check help [files|steps]`

// filesHelp is the help of the files subcommand.
const filesHelp = `remediation-check files <dir>

Purpose
  Checks the workflow files an install gives agent-remediation. It loads the
  files the component loads with <dir> as its install folder: the built-in flow,
  type and fixture files, then the ones under <dir> over them. An install file
  with the kind and name of a built-in replaces it whole, and any other file is
  added; one at the path of a built-in file whose flow, type or fixture stays
  loaded, of whatever kind the install file is, is an error. It validates the
  result against the steps the runner registers,
  checks that it defines the flow every heal trigger runs (rejected-release),
  and plays every type a flow lists (in rejected-release when it lists the
  type, else in the first flow by name that lists it) and every flow, as
  itself, through the conformance scenarios, in worlds of scripted fakes over
  the memory store. A built-in type fixture of a
  type that no loaded flow lists is not played, and a warning says so. An
  install fixture that nothing plays fails: one of a type no flow lists, or of
  a flow no file defines. When the files do not load or do not validate it
  reports the errors and skips the scenarios.

Arguments
  <dir>  The folder of the install's files: *.yaml and *.yml files of kind flow,
         type or fixture, in any layout. Files and directories whose name starts
         with a dot are skipped. A folder with no file checks the built-ins
         alone. It must be an existing directory.

Stdout
  One JSON object and nothing else:
    {"files": int, "errors": [string], "warnings": [string],
     "scenarios": [{"type": string, "scenario": string, "ok": bool, "error": string}]}
  files is the number of distinct files that hold a flow, a type or a fixture of
  the merged set, built-in files included. errors holds each message once.
  warnings holds the notes of the load (an install file that replaces a
  built-in, a based_on whose built-in has changed or does not exist), the
  validator's warnings, a note for each flow no heal trigger runs, then a note
  for each built-in fixture that was not played. A flow scenario names its flow
  in "type". A message names a file by its path under <dir>, and a path under
  flows/, types/ or fixtures/ that <dir> does not hold is a built-in file.
  Human text goes to stderr.

Exit codes
  0  no error, and every scenario passed
  1  an error, or a scenario failed
  2  usage: a missing or extra argument, or a <dir> that is not a directory
`

// stepsHelp is the help of the steps subcommand.
const stepsHelp = `remediation-check steps

Purpose
  Lists the steps a flow or type file may name: every step the runner registers,
  sorted by name, with the evidence each one reads and provides, where it may
  stand and which data classes it puts in front of the model.

Arguments
  None.

Stdout
  One JSON array and nothing else, with one object per step:
    {"name": string, "kind": string, "description": string,
     "needs": [string], "provides": [string], "stages": [string],
     "may_stand_as_gate_run": bool, "data_class": [string]}
  kind is context, prompt, check, approver or output. needs and provides are the
  evidence keys the step reads and writes. stages are the stages the step
  declares it may stand at: a check always declares them, a step of another
  kind may declare none, and its kind says where it runs.
  may_stand_as_gate_run is true for a check that may be a flow's gate.
  data_class holds the data classes (code, logs, rows) of the evidence the step
  puts in front of the model, in that order: the classes of the keys it provides
  and, for a prompt, of the keys it reads too. A flow's model_may_see must list
  the class of each key that a context step of the flow, or of a type it
  lists, provides, and of each key that such a type's prompt reads. No other
  class shown here is held against it. Every array is [] when empty.

Exit codes
  0  the catalog was printed
  2  usage: an argument was given
`

// subcommandHelp is the help block of each subcommand, by name.
var subcommandHelp = map[string]string{"files": filesHelp, "steps": stepsHelp}

// overviewHelp is the help of the whole command: what it is, then the block of
// every subcommand.
const overviewHelp = `remediation-check checks the workflow files of agent-remediation and lists the
steps they may name.

Usage:
  remediation-check files <dir>
  remediation-check steps
  remediation-check help [files|steps]

help, -h and --help print this overview. "help <subcommand>", "<subcommand> -h"
and "<subcommand> --help" print the block of one subcommand. Help goes to
stdout and exits 0.

` + filesHelp + `
` + stepsHelp

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

// stepEntry is one element of the JSON array `steps` prints. Its fields are
// printed in this order, and the slices are never nil.
type stepEntry struct {
	Name              string   `json:"name"`
	Kind              string   `json:"kind"`
	Description       string   `json:"description"`
	Needs             []string `json:"needs"`
	Provides          []string `json:"provides"`
	Stages            []string `json:"stages"`
	MayStandAsGateRun bool     `json:"may_stand_as_gate_run"`
	DataClass         []string `json:"data_class"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes the command in args, writes the JSON a subcommand prints (or the
// help it was asked for) to stdout and human text to stderr, and returns the
// exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if text := helpText(args); text != "" {
		if _, err := io.WriteString(stdout, text); err != nil {
			slog.New(slog.NewTextHandler(stderr, nil)).Error("write the help", "error", err)
			return exitFail
		}
		return exitOK
	}
	switch {
	case len(args) == 2 && args[0] == "files":
		return filesCommand(args[1], stdout, stderr)
	case len(args) == 1 && args[0] == "steps":
		return stepsCommand(stdout, stderr)
	}
	_, _ = fmt.Fprintln(stderr, usage)
	return exitUsage
}

// helpText is the help args ask for, or "" when they ask for none. "help", "-h"
// or "--help" alone ask for the overview, and "help <sub>", "<sub> -h" or
// "<sub> --help" for the block of one subcommand. A "help" of a name that is no
// subcommand asks for nothing, so run reports it as a usage error.
func helpText(args []string) string {
	switch {
	case len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help"):
		return overviewHelp
	case len(args) == 2 && args[0] == "help":
		return subcommandHelp[args[1]]
	case len(args) == 2 && (args[1] == "-h" || args[1] == "--help"):
		return subcommandHelp[args[0]]
	}
	return ""
}

// filesCommand checks the install files under dir, prints the report and
// returns the exit code.
func filesCommand(dir string, stdout, stderr io.Writer) int {
	dir = filepath.Clean(dir)
	if info, err := os.Stat(dir); err != nil || !info.IsDir() { //nolint:gosec // G703: the operator names the directory to check on the command line.
		_, _ = fmt.Fprintf(stderr, "%s is not a directory\n%s\n", dir, usage)
		return exitUsage
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))

	rep := checkFiles(context.Background(), dir)
	if err := writeJSON(stdout, rep); err != nil {
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

// stepsCommand prints the catalog of steps and returns the exit code.
func stepsCommand(stdout, stderr io.Writer) int {
	logger := slog.New(slog.NewTextHandler(stderr, nil))
	steps := listSteps()
	if err := writeJSON(stdout, steps); err != nil {
		logger.Error("write the step catalog", "error", err)
		return exitFail
	}
	logger.Info("listed", "steps", len(steps))
	return exitOK
}

// writeJSON writes v to w as one line of JSON, with the characters <, > and &
// left as they are so a message that quotes a command reads as written.
func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

// checkFiles loads, validates and runs the conformance suite over the files
// the component would load with dir as its install folder.
func checkFiles(ctx context.Context, dir string) report {
	rep := report{Errors: []string{}, Warnings: []string{}, Scenarios: []scenarioResult{}}
	fsys := os.DirFS(dir)

	set, loaded, loadErr := workflowfile.LoadInstall(workflows.Builtin, fsys)
	builtinFixtures, installFixtures, fixtureErr := workflowfile.LoadInstallFixtures(workflows.Builtin, fsys)
	rep.Files = countFiles(set, builtinFixtures, installFixtures)
	rep.addErrors(messages(loadErr)...)
	rep.addErrors(messages(fixtureErr)...)
	if len(rep.Errors) == 0 {
		rep.addErrors(messages(workflowfile.CheckInstallPaths(workflows.Builtin, fsys))...)
	}
	if len(rep.Errors) > 0 {
		return rep
	}

	rep.Warnings = append(rep.Warnings, loaded.Notes...)
	warnings, err := runner.Validate(set, runner.NewRegistry(runner.Collaborators{}))
	rep.Warnings = append(rep.Warnings, warnings...)
	rep.Warnings = append(rep.Warnings, runner.FlowsNoTriggerRuns(set)...)
	rep.addErrors(messages(err)...)
	if _, err := runner.TriggerFlow(set); err != nil {
		rep.addErrors(err.Error())
	}
	if len(rep.Errors) > 0 {
		return rep
	}

	fixtures, skipped := builtinFixturesToPlay(set, builtinFixtures)
	rep.Warnings = append(rep.Warnings, skipped...)
	locator := repofs.NewLocator(slog.New(slog.DiscardHandler))
	suite := conformance.Suite{
		Files:           set,
		Fixtures:        append(fixtures, installFixtures...),
		Locator:         locator,
		Inspector:       locator,
		ModelReferences: repofs.ModelReferenceFinder{},
		NewWorld:        scripted.Worlds(memory.NewStore),
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

// addErrors appends each message the report does not hold yet, in order: the
// flow and type loader and the fixture loader each read every install file, so
// a file neither can parse is reported by both with the same message.
func (r *report) addErrors(msgs ...string) {
	for _, m := range msgs {
		if !slices.Contains(r.Errors, m) {
			r.Errors = append(r.Errors, m)
		}
	}
}

// builtinFixturesToPlay is the built-in fixtures the suite plays over set, and
// a warning for each one it leaves out. A built-in type fixture of a type that
// no loaded flow lists is left out: an install whose flow drops a built-in
// type cannot remove that type's built-in fixture, and nothing would play it.
// Every other built-in fixture is played. An install's own fixture is never
// left out, so the suite fails one that tests a type no flow lists.
func builtinFixturesToPlay(set workflow.Set, builtin []conformance.Fixture) (play []conformance.Fixture, warnings []string) {
	listed := map[string]bool{}
	for _, f := range set.Flows {
		for _, t := range f.Types {
			listed[t] = true
		}
	}
	for _, f := range builtin {
		if f.Type != "" && !listed[f.Type] {
			warnings = append(warnings, fmt.Sprintf("%s: skipped, no loaded flow lists type %s", f.File, f.Type))
			continue
		}
		play = append(play, f)
	}
	return play, warnings
}

// countFiles is the number of distinct files that hold a loaded flow, type or
// fixture. When the report holds no error, an install file and a built-in file
// never share a path while both hold something loaded, whatever its kind:
// workflowfile.CheckInstallPaths refuses that.
func countFiles(set workflow.Set, fixtures ...[]conformance.Fixture) int {
	seen := map[string]bool{}
	for _, f := range set.Flows {
		seen[f.Pos.File] = true
	}
	for _, t := range set.Types {
		seen[t.Pos.File] = true
	}
	for _, list := range fixtures {
		for _, f := range list {
			seen[f.File] = true
		}
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

// listSteps describes every step the runner registers, sorted by name. A
// registry built from no collaborators describes its steps, since constructing
// a step never calls one.
func listSteps() []stepEntry {
	reg := runner.NewRegistry(runner.Collaborators{})
	metas := reg.All()
	steps := make([]stepEntry, 0, len(metas))
	for _, m := range metas {
		shown := append([]remediation.EvidenceKey{}, m.Provides...)
		if m.Kind == step.KindPrompt {
			shown = append(shown, m.Needs...)
		}
		steps = append(steps, stepEntry{
			Name:              string(m.Name),
			Kind:              string(m.Kind),
			Description:       m.Description,
			Needs:             stringsOf(m.Needs),
			Provides:          stringsOf(m.Provides),
			Stages:            stringsOf(m.Stages),
			MayStandAsGateRun: m.MayStandAsGateRun,
			DataClass:         dataClasses(reg, shown),
		})
	}
	return steps
}

// dataClasses is the data classes of the evidence keys, each once, in the order
// workflow.DataClasses lists them. A key the registry knows no class of adds
// none.
func dataClasses(reg *runner.Registry, keys []remediation.EvidenceKey) []string {
	held := map[workflow.DataClass]bool{}
	for _, k := range keys {
		if class, ok := reg.EvidenceClass(k); ok {
			held[class] = true
		}
	}
	classes := []string{}
	for _, class := range workflow.DataClasses() {
		if held[class] {
			classes = append(classes, string(class))
		}
	}
	return classes
}

// stringsOf converts a list of names to strings. The result is never nil.
func stringsOf[T ~string](names []T) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = string(n)
	}
	return out
}
