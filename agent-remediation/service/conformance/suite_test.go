package conformance_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/adapters/scripted"
	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/serialization"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
	"github.com/carolsimone/continuo/pkg/sanitize"
)

const flowName = "rejected-release"

var ctx = context.Background()

// builtinSuite is the suite over the built-in files and fixtures, with the
// real repofs contract locator and inspector and worlds over the memory store.
func builtinSuite(t *testing.T) conformance.Suite {
	t.Helper()
	files, err := workflowfile.Load(workflows.Builtin)
	require.NoError(t, err)
	fixtures, err := workflowfile.LoadFixtures(workflows.Builtin)
	require.NoError(t, err)
	locator := repofs.NewLocator(slog.New(slog.DiscardHandler))
	return conformance.Suite{
		Files:           files,
		Fixtures:        fixtures,
		Locator:         locator,
		Inspector:       locator,
		ModelReferences: repofs.ModelReferenceFinder{},
		NewWorld:        scripted.Worlds(memory.NewStore),
	}
}

// onlyTypes narrows the suite's flow to the named types, in that order.
func onlyTypes(s conformance.Suite, names ...string) conformance.Suite {
	flow := s.Files.Flows[flowName]
	flow.Types = names
	files := workflow.Set{Flows: map[string]workflow.Flow{flowName: flow}, Types: s.Files.Types}
	s.Files = files
	return s
}

// withoutFixture is the suite's fixtures without the one testing typeName.
func withoutFixture(s conformance.Suite, typeName string) conformance.Suite {
	s.Fixtures = slices.DeleteFunc(slices.Clone(s.Fixtures), func(f conformance.Fixture) bool { return f.Type == typeName })
	return s
}

// builtinTypeFiles is how many type files the built-in files hold. It walks the
// files and does not load them, so a type the loader dropped still counts.
func builtinTypeFiles(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, fs.WalkDir(workflows.Builtin, "types", func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && (path.Ext(p) == ".yaml" || path.Ext(p) == ".yml") {
			n++
		}
		return err
	}))
	return n
}

// The built-in flow lists every built-in type, and each is played through every
// type scenario.
func TestSuite_BuiltinFilesPass(t *testing.T) {
	s := builtinSuite(t)
	warnings, err := runner.Validate(s.Files, runner.NewRegistry(runner.Collaborators{}))
	require.NoError(t, err)
	require.Empty(t, warnings)

	results := s.Run(ctx)
	flow := s.Files.Flows[flowName]
	require.Len(t, flow.Types, builtinTypeFiles(t), "the flow lists every built-in type file's type")
	require.Len(t, results, len(flow.Types)*len(conformance.TypeScenarios()))
	for _, r := range results {
		assert.NoError(t, r.Err, "type %s, scenario %s", r.Type, r.Scenario)
	}

	flowResults := s.RunFlowScenarios(ctx, flowName)
	require.Len(t, flowResults, len(conformance.FlowScenarios()))
	for _, r := range flowResults {
		assert.Equal(t, flowName, r.Type)
		assert.NoError(t, r.Err, "flow %s, scenario %s", r.Type, r.Scenario)
	}
}

func TestSuite_ATypeWithoutAFixtureFails(t *testing.T) {
	s := withoutFixture(onlyTypes(builtinSuite(t), "dbt-compile", "seed-build"), "seed-build")

	results := s.Run(ctx)

	var listed []conformance.Result
	for _, r := range results {
		switch r.Type {
		case "dbt-compile":
			listed = append(listed, r)
			assert.NoError(t, r.Err, "scenario %s", r.Scenario)
		case "seed-build":
			listed = append(listed, r)
			require.Error(t, r.Err, "scenario %s", r.Scenario)
			assert.Contains(t, r.Err.Error(), "no fixture")
		}
	}
	assert.Len(t, listed, 2*len(conformance.TypeScenarios()), "one result per listed type and scenario")
}

func TestSuite_AFixtureForATypeNoFlowListsFails(t *testing.T) {
	s := onlyTypes(builtinSuite(t), "dbt-compile")

	results := s.Run(ctx)

	var unlisted []conformance.Result
	for _, r := range results {
		if r.Type != "dbt-compile" {
			unlisted = append(unlisted, r)
		}
	}
	require.NotEmpty(t, unlisted)
	for _, r := range unlisted {
		require.Error(t, r.Err)
		assert.Contains(t, r.Err.Error(), "no flow lists")
	}
}

// A flow fixture is played only by the flow it names, so one that names a flow
// no file defines (a mistyped name) is played by nothing and fails with one
// result that names its file. Every fixture of a loaded flow is left to
// RunFlowScenarios.
func TestSuite_AFixtureForAFlowNoFileDefinesFails(t *testing.T) {
	s := onlyTypes(builtinSuite(t), "dbt-compile")
	var mistyped conformance.Fixture
	for _, f := range s.Fixtures {
		if f.Flow == flowName && f.Scenario == conformance.FlowMaintenance {
			mistyped = f
		}
	}
	require.NotEmpty(t, mistyped.File, "the built-in maintenance fixture")
	mistyped.Flow = "rejected-relase"
	mistyped.File = "my-maintenance.yaml"
	s.Fixtures = append(slices.Clone(s.Fixtures), mistyped)

	results := s.Run(ctx)

	var unplayed []conformance.Result
	for _, r := range results {
		if r.Type == "rejected-relase" {
			unplayed = append(unplayed, r)
		}
		if r.Type == flowName {
			assert.Fail(t, "Run reported a fixture of the loaded flow", "%s: %v", r.Scenario, r.Err)
		}
	}
	require.Len(t, unplayed, 1)
	assert.Equal(t, conformance.FlowMaintenance, unplayed[0].Scenario)
	require.Error(t, unplayed[0].Err)
	assert.Equal(t, "my-maintenance.yaml tests flow rejected-relase, which no file defines", unplayed[0].Err.Error())
}

// withSecondFlow is the suite with one more flow, named name: a copy of the
// built-in flow that lists types, in that order and with tries tries, and every
// built-in flow fixture copied to play it. The built-in flow is left as it is.
func withSecondFlow(s conformance.Suite, name string, tries int, types ...string) conformance.Suite {
	flow := s.Files.Flows[flowName]
	flow.Name = name
	flow.Types = types
	flow.Tries = tries
	flows := maps.Clone(s.Files.Flows)
	flows[name] = flow
	s.Files = workflow.Set{Flows: flows, Types: s.Files.Types}
	fixtures := slices.Clone(s.Fixtures)
	for _, f := range s.Fixtures {
		if f.Flow == flowName {
			f.Flow = name
			f.File = name + "/" + f.File
			fixtures = append(fixtures, f)
		}
	}
	s.Fixtures = fixtures
	return s
}

// A flow scenario runs the flow it is played for: the remediation of each of
// its worlds follows that flow, so a second flow that lists one type fails the
// scenario of two types with the same fixtures the built-in flow passes.
func TestSuite_AFlowScenarioRunsTheNamedFlow(t *testing.T) {
	s := withSecondFlow(builtinSuite(t), "second-flow", 3, "dbt-compile")
	worlds := recordWorlds(&s)

	second := map[conformance.Scenario]error{}
	for _, r := range s.RunFlowScenarios(ctx, "second-flow") {
		assert.Equal(t, "second-flow", r.Type)
		second[r.Scenario] = r.Err
	}
	require.Len(t, second, len(conformance.FlowScenarios()))
	require.Error(t, second[conformance.FlowTwoTypesOneProject], "a flow that lists only dbt-compile cannot fix with two types")
	assert.Contains(t, second[conformance.FlowTwoTypesOneProject].Error(), "want two")
	require.NotEmpty(t, *worlds)
	for _, w := range *worlds {
		for _, rem := range w.Remediations() {
			assert.Equal(t, "second-flow", rem.Flow, "every remediation of a second-flow scenario follows second-flow")
		}
	}

	*worlds = nil
	for _, r := range s.RunFlowScenarios(ctx, flowName) {
		assert.NoError(t, r.Err, "flow %s, scenario %s", r.Type, r.Scenario)
	}
	require.NotEmpty(t, *worlds)
	for _, w := range *worlds {
		for _, rem := range w.Remediations() {
			assert.Equal(t, flowName, rem.Flow)
		}
	}
}

// A type the flow heal triggers run does not list is played in the first flow,
// by name, that lists it: a type that only a second flow lists is fixed there,
// with that flow's tries, and not in the built-in flow, which would hand its
// cluster over as no type.
func TestSuite_ATypeOnlyASecondFlowListsRunsInThatFlow(t *testing.T) {
	s := withSecondFlow(onlyTypes(builtinSuite(t), "dbt-parse"), "z-second-flow", 2, "dbt-compile")
	s.Fixtures = slices.DeleteFunc(s.Fixtures, func(f conformance.Fixture) bool {
		return f.Type != "" && f.Type != "dbt-parse" && f.Type != "dbt-compile"
	})
	worlds := recordWorlds(&s)

	results := s.Run(ctx)

	var compile []conformance.Result
	for _, r := range results {
		assert.NoError(t, r.Err, "type %s, scenario %s", r.Type, r.Scenario)
		if r.Type == "dbt-compile" {
			compile = append(compile, r)
		}
	}
	assert.Len(t, compile, len(conformance.TypeScenarios()), "dbt-compile is played through every type scenario")
	seen := 0
	for _, w := range *worlds {
		for _, rem := range w.Remediations() {
			want := flowName
			if w.typ == "dbt-compile" {
				want = "z-second-flow"
			}
			assert.Equal(t, want, rem.Flow, "a remediation of a %s scenario", w.typ)
			seen++
		}
	}
	assert.Positive(t, seen)
}

// A type that the flow heal triggers run lists is played in that flow, with
// its tries, even when a flow whose name sorts first lists the type too.
func TestSuite_ATypeListedByTheProductionFlowIsPlayedThere(t *testing.T) {
	s := withSecondFlow(onlyTypes(builtinSuite(t), "dbt-compile"), "a-experimental", 2, "dbt-compile")
	s.Fixtures = slices.DeleteFunc(s.Fixtures, func(f conformance.Fixture) bool {
		return f.Type != "" && f.Type != "dbt-compile"
	})
	production := s.Files.Flows[runner.TriggerFlowName]
	require.Equal(t, 3, production.Tries)
	require.Less(t, "a-experimental", runner.TriggerFlowName, "the other flow sorts first")
	worlds := recordWorlds(&s)

	results := s.Run(ctx)

	require.Len(t, results, len(conformance.TypeScenarios()), "dbt-compile is played once, in one flow")
	for _, r := range results {
		assert.Equal(t, "dbt-compile", r.Type)
		assert.NoError(t, r.Err, "scenario %s", r.Scenario)
	}
	mostTries := 0
	for _, w := range *worlds {
		for _, rem := range w.Remediations() {
			assert.Equal(t, runner.TriggerFlowName, rem.Flow, "a remediation of a dbt-compile scenario")
			mostTries = max(mostTries, len(w.Tries(rem.ID)))
		}
	}
	assert.Equal(t, production.Tries, mostTries, "the scenario whose tries are all spent makes the production flow's tries")
}

func TestSuite_AFlowScenarioWithoutAFixtureFails(t *testing.T) {
	s := builtinSuite(t)
	s.Fixtures = slices.DeleteFunc(slices.Clone(s.Fixtures), func(f conformance.Fixture) bool {
		return f.Scenario == conformance.FlowMaintenance
	})

	results := s.RunFlowScenarios(ctx, flowName)

	require.Len(t, results, len(conformance.FlowScenarios()))
	for _, r := range results {
		if r.Scenario == conformance.FlowMaintenance {
			require.Error(t, r.Err)
			assert.Contains(t, r.Err.Error(), "no fixture")
			continue
		}
		assert.NoError(t, r.Err, "scenario %s", r.Scenario)
	}
}

// The unfixable scenario plays a failure the classifier marked unfixable: a
// fixture whose node carries no such mark does not test it, so the scenario
// fails, and so do the hand-over expectations the run could not meet.
func TestSuite_TheUnfixableScenarioFailsWhenTheNodeIsNotMarkedUnfixable(t *testing.T) {
	s := builtinSuite(t)
	s.Fixtures = slices.Clone(s.Fixtures)
	for i, f := range s.Fixtures {
		if f.Scenario == conformance.FlowUnfixable {
			trigger := f.Trigger
			trigger.Nodes = slices.Clone(f.Trigger.Nodes)
			for j := range trigger.Nodes {
				trigger.Nodes[j].Unfixable = nil
			}
			s.Fixtures[i].Trigger = trigger
		}
	}

	for _, r := range s.RunFlowScenarios(ctx, flowName) {
		if r.Scenario != conformance.FlowUnfixable {
			assert.NoError(t, r.Err, "scenario %s", r.Scenario)
			continue
		}
		require.Error(t, r.Err)
		assert.Contains(t, r.Err.Error(), "node model.sales.orders of the trigger is not marked unfixable")
	}
}

// A fixture whose passing answer edits a file the model was never shown is
// refused by pre-flight, so every draft the scenario scripted to pass asks for
// a revision instead, and each scenario that expects a fix fails, most on
// the call its script holds no answer for: the suite judges what the files do,
// not only that they run, and never answers a call twice.
func TestSuite_AnAnswerPreflightRefusesFailsTheScenariosThatExpectAFix(t *testing.T) {
	s := onlyTypes(builtinSuite(t), "dbt-compile")
	for i, f := range s.Fixtures {
		if f.Type == "dbt-compile" {
			s.Fixtures = slices.Clone(s.Fixtures)
			s.Fixtures[i].Answers.Pass = f.Answers.Fail
		}
	}

	results := s.Run(ctx)

	errs := map[conformance.Scenario]error{}
	for _, r := range results {
		if r.Type == "dbt-compile" {
			errs[r.Scenario] = r.Err
		}
	}
	const unscripted = `model answers for calls about "svc", and this is call `
	for sc, want := range map[conformance.Scenario]string{
		conformance.FirstDraftPasses:        "the script holds 1 " + unscripted + "2",
		conformance.PreflightRevisionPasses: "the script holds 2 " + unscripted + "3",
		conformance.GateFailsNextTryLearns:  "the script holds 2 " + unscripted + "3",
		// Three refused answers spend the first try's three drafts.
		conformance.TriesSpent:    "the tries are [failed], want [failed failed failed escalated]",
		conformance.ModelDeclines: "the script holds 2 " + unscripted + "3",
		conformance.Redelivery:    "the script holds 2 " + unscripted + "3",
	} {
		require.Error(t, errs[sc], "scenario %s", sc)
		assert.Contains(t, errs[sc].Error(), want, "scenario %s", sc)
	}
	assert.NoError(t, errs[conformance.DraftsSpent])
	assert.NoError(t, errs[conformance.DeclinesBeforeModel])
}

// A fixture with no answer for a draft its scenario plays fails that scenario
// with the list it lacks, before anything runs.
func TestSuite_AScenarioWhoseDraftHasNoAnswersFails(t *testing.T) {
	s := onlyTypes(builtinSuite(t), "dbt-compile")
	for i, f := range s.Fixtures {
		if f.Type == "dbt-compile" {
			s.Fixtures = slices.Clone(s.Fixtures)
			s.Fixtures[i].Answers.Decline = nil
		}
	}

	errs := map[conformance.Scenario]error{}
	for _, r := range s.Run(ctx) {
		if r.Type == "dbt-compile" {
			errs[r.Scenario] = r.Err
		}
	}

	require.Error(t, errs[conformance.ModelDeclines])
	assert.Contains(t, errs[conformance.ModelDeclines].Error(), "has no answers.decline for draft 1")
	for _, sc := range conformance.TypeScenarios() {
		if sc != conformance.ModelDeclines {
			assert.NoError(t, errs[sc], "scenario %s", sc)
		}
	}
}

// extraChecks is a world that also reports release checks of projects no
// edit touches, in the order given.
type extraChecks struct {
	conformance.World
	projects []string
}

func (w extraChecks) Checks() []ports.ReleaseCheckRequest {
	runs := w.World.Checks()
	for _, s := range w.projects {
		runs = append(runs, ports.ReleaseCheckRequest{CheckID: "extra-" + s, Project: s})
	}
	return runs
}

// withExtraChecks makes every world of the suite report a release check of each project.
func withExtraChecks(s conformance.Suite, projects ...string) conformance.Suite {
	base := s.NewWorld
	s.NewWorld = func(f conformance.Fixture, sc conformance.Script) (conformance.World, error) {
		w, err := base(f, sc)
		if err != nil {
			return nil, err
		}
		return extraChecks{World: w, projects: projects}, nil
	}
	return s
}

// A failure names the projects and the nodes it is about in sorted order, so
// remediation-check prints the same text on every run whatever order a map or
// the world hands them over in.
func TestSuite_AFailureNamesItsProjectsAndNodesInSortedOrder(t *testing.T) {
	base := onlyTypes(builtinSuite(t), "dbt-compile")
	const (
		runsText = "project alpha had 1 release checks and no edit; project zeta had 1 release checks and no edit"
		gateText = `does not name alpha.node's error "alpha broke"; try 1's verify error`
	)

	var texts []string
	for i := range 10 {
		// Each pass hands the projects and the gate errors over in the other
		// order: as runs, and as the order the map was filled in.
		order := []string{"zeta", "alpha"}
		if i%2 == 1 {
			order = []string{"alpha", "zeta"}
		}
		s := base
		s.Fixtures = slices.Clone(base.Fixtures)
		for j, f := range s.Fixtures {
			if f.Type == "dbt-compile" {
				s.Fixtures[j].GateErrors = maps.Clone(f.GateErrors)
				for _, project := range order {
					s.Fixtures[j].GateErrors[project+".node"] = project + " broke"
				}
			}
		}
		errs := map[conformance.Scenario]error{}
		for _, r := range withExtraChecks(s, order...).Run(ctx) {
			if r.Type == "dbt-compile" {
				errs[r.Scenario] = r.Err
			}
		}
		require.Error(t, errs[conformance.FirstDraftPasses])
		assert.Contains(t, errs[conformance.FirstDraftPasses].Error(), runsText)
		require.Error(t, errs[conformance.GateFailsNextTryLearns])
		gate := errs[conformance.GateFailsNextTryLearns].Error()
		assert.Contains(t, gate, gateText)
		assert.Less(t, strings.Index(gate, "alpha.node"), strings.Index(gate, "zeta.node"))
		texts = append(texts, errs[conformance.FirstDraftPasses].Error()+"\n"+gate)
	}
	for _, text := range texts[1:] {
		assert.Equal(t, texts[0], text)
	}
}

// withDrafts is the suite with every type allowing drafts drafts
// and the flow making tries tries.
func withDrafts(s conformance.Suite, drafts, tries int) conformance.Suite {
	types := make(map[string]workflow.Type, len(s.Files.Types))
	for name, typ := range s.Files.Types {
		typ.Agent.Drafts = drafts
		types[name] = typ
	}
	flow := s.Files.Flows[flowName]
	flow.Tries = tries
	s.Files = workflow.Set{Flows: map[string]workflow.Flow{flowName: flow}, Types: types}
	return s
}

// Every scenario runs for every type whatever its drafts and its flow's
// tries: a type of one draft has its refused draft handed over where a type
// that revises passes its revision, and a flow of one try hands over the
// try whose release check failed.
func TestSuite_EveryScenarioHoldsForTypesOfOneAndOfTwoDrafts(t *testing.T) {
	for _, tc := range []struct{ drafts, tries int }{{1, 3}, {2, 3}, {1, 1}} {
		t.Run(fmt.Sprintf("drafts %d tries %d", tc.drafts, tc.tries), func(t *testing.T) {
			s := withDrafts(builtinSuite(t), tc.drafts, tc.tries)
			_, err := runner.Validate(s.Files, runner.NewRegistry(runner.Collaborators{}))
			require.NoError(t, err)

			results := s.Run(ctx)
			require.Len(t, results, len(s.Files.Flows[flowName].Types)*len(conformance.TypeScenarios()))
			for _, r := range results {
				assert.NoError(t, r.Err, "type %s, scenario %s", r.Type, r.Scenario)
			}
			for _, r := range s.RunFlowScenarios(ctx, flowName) {
				assert.NoError(t, r.Err, "flow %s, scenario %s", r.Type, r.Scenario)
			}
		})
	}
}

// The values the cleaner test plants: an email address and a password, in a
// line of text that every file and log of every fixture carries.
const (
	plantedEmail    = "alice@example.com"
	plantedPassword = "hunter2"
	plantedText     = "owner " + plantedEmail + " password=" + plantedPassword
)

// commentLine is text as one comment line of a file of the kind path's
// extension names, newline-terminated.
func commentLine(path, text string) string {
	switch filepath.Ext(path) {
	case ".sql":
		return "-- " + text + "\n"
	case ".yml", ".yaml", ".py":
		return "# " + text + "\n"
	}
	return text + "\n"
}

// plantValues is a Suite.Mutate that puts plantedText in every log a fixture
// holds, in every file of its repository and in the source of every dbt node of
// its code bundle, as a comment line where the file takes one, and puts the line a model writes when it copies the planted line
// from the cleaned file it was shown (the placeholders the Redactor issues for
// the two values) at the end of every answer. A fixture's answers are scripted,
// so the answer is where the planted values come back as placeholders for the
// runner to restore, and an answer that left a file as it was still does.
func plantValues(f *conformance.Fixture) {
	for uri, text := range f.Objects {
		if strings.HasSuffix(uri, ".log") {
			f.Objects[uri] = text + plantedText + "\n"
		}
	}
	for path, text := range f.RepoFiles {
		f.RepoFiles[path] = text + commentLine(path, plantedText)
	}
	for node, src := range f.Bundle {
		if src.Runtime == "dbt" {
			src.RawCode += commentLine("x.sql", plantedText)
			f.Bundle[node] = src
		}
	}
	cleaned := sanitize.NewRedactor().Clean(plantedText)
	plant := func(answers []ports.ProposeResult) {
		for i := range answers {
			a := &answers[i]
			if a.ProposedSQL != "" {
				a.ProposedSQL += commentLine("x.sql", cleaned)
			}
			if a.ProposedContent != "" {
				a.ProposedContent += commentLine(a.TargetFile, cleaned)
			}
			for j := range a.Files {
				a.Files[j].Content += commentLine(a.Files[j].Path, cleaned)
			}
		}
	}
	plantInAnswers := func(a *conformance.Answers) {
		for _, list := range [][]ports.ProposeResult{a.Pass, a.Fail, a.Decline, a.Revision.Pass, a.Revision.Fail, a.Revision.Decline} {
			plant(list)
		}
	}
	plantInAnswers(&f.Answers)
	for node, a := range f.MemberAnswers {
		plantInAnswers(&a)
		f.MemberAnswers[node] = a
	}
}

// recordedWorld is a world a scenario ran in, with the type of the fixture it
// was built from.
type recordedWorld struct {
	typ string
	conformance.World
}

// recordWorlds makes every world of the suite one the returned list holds, in
// the order the worlds are built.
func recordWorlds(s *conformance.Suite) *[]recordedWorld {
	var worlds []recordedWorld
	base := s.NewWorld
	s.NewWorld = func(f conformance.Fixture, sc conformance.Script) (conformance.World, error) {
		w, err := base(f, sc)
		if err == nil {
			worlds = append(worlds, recordedWorld{typ: f.Type, World: w})
		}
		return w, err
	}
	return &worlds
}

// checkedDrafts is the drafts the first remediation of the world checked and
// passed: the draft each cluster's passing pre-flight pass stored.
func checkedDrafts(t *testing.T, w conformance.World) []remediation.Draft {
	t.Helper()
	rems := w.Remediations()
	require.Len(t, rems, 1)
	var drafts []remediation.Draft
	for _, row := range w.Steps(rems[0].ID) {
		if row.Step != remediation.StepPreflight || row.Status != remediation.StepDone || len(row.Findings) > 0 {
			continue
		}
		rec, err := serialization.DecodeRecord[serialization.PreflightRecord](row.Output)
		require.NoError(t, err)
		drafts = append(drafts, rec.Draft)
	}
	return drafts
}

// Whatever the model is shown is cleaned by the runner: a fixture whose logs,
// files and answers all hold an email address and a password reaches the model
// with a placeholder for each, in every request of every scenario, while the
// draft that pre-flight and the gate check holds the values again, where the
// file the draft edits had them.
func TestSuite_EveryModelRequestIsCleaned(t *testing.T) {
	s := builtinSuite(t)
	s.Mutate = plantValues
	worlds := recordWorlds(&s)

	results := s.Run(ctx)
	results = append(results, s.RunFlowScenarios(ctx, flowName)...)

	cleaned := sanitize.NewRedactor().Clean(plantedText)
	shown := map[string]bool{}
	for _, r := range results {
		assert.NoError(t, r.Err, "type %s, scenario %s", r.Type, r.Scenario)
		for i, req := range r.Requests {
			wire, err := json.Marshal(req)
			require.NoError(t, err)
			assert.NotContains(t, string(wire), plantedEmail, "type %s, scenario %s, request %d", r.Type, r.Scenario, i+1)
			assert.NotContains(t, string(wire), plantedPassword, "type %s, scenario %s, request %d", r.Type, r.Scenario, i+1)
			if strings.Contains(req.User, cleaned) {
				shown[r.Type] = true
			}
		}
	}

	// The first scenario of a type, first_draft_passes, builds the first world
	// of the type's fixture.
	seen := map[string]bool{}
	for _, rw := range *worlds {
		if rw.typ == "" || seen[rw.typ] {
			continue
		}
		seen[rw.typ] = true
		t.Run(rw.typ, func(t *testing.T) {
			assert.True(t, shown[rw.typ], "a request of type %s shows the planted line as %q, so the test cleans something", rw.typ, cleaned)
			drafts := checkedDrafts(t, rw.World)
			require.NotEmpty(t, drafts)
			for _, d := range drafts {
				require.NotEmpty(t, d.Edits)
				for _, e := range d.Edits {
					assert.Contains(t, e.Original, plantedEmail, "the file %s as read holds the planted line", e.Path)
					assert.Contains(t, e.Content, plantedEmail, "the checked edit of %s holds the email address again", e.Path)
					assert.Contains(t, e.Content, plantedPassword, "the checked edit of %s holds the password again", e.Path)
					assert.NotContains(t, e.Content, "[redacted", "the checked edit of %s holds no placeholder", e.Path)
				}
			}
		})
	}
	assert.NotEmpty(t, seen)
}

// A Mutate changes a copy of each fixture: the suite's loaded fixtures hold
// what they held after the scenarios ran on the changed ones, however often
// the suite is run.
func TestSuite_MutateChangesACopyOfEveryFixture(t *testing.T) {
	s := builtinSuite(t)
	s.Mutate = plantValues

	s.Run(ctx)
	s.RunFlowScenarios(ctx, flowName)

	for _, f := range s.Fixtures {
		for path, text := range f.RepoFiles {
			assert.NotContains(t, text, plantedEmail, "%s: %s", f.File, path)
		}
		for uri, text := range f.Objects {
			assert.NotContains(t, text, plantedEmail, "%s: %s", f.File, uri)
		}
		for node, src := range f.Bundle {
			assert.NotContains(t, src.RawCode, plantedEmail, "%s: %s", f.File, node)
		}
		for _, answers := range [][]ports.ProposeResult{f.Answers.Pass, f.Answers.Fail, f.Answers.Decline} {
			for _, a := range answers {
				assert.NotContains(t, a.ProposedSQL+a.ProposedContent, "[redacted", f.File)
			}
		}
	}
}

// A result carries the requests its scenario made, in call order: the one
// request of a first draft that passes, a request for each model call of a
// scenario that revises, and none of a scenario that declines before the model
// is called.
func TestSuite_AResultCarriesTheModelRequestsOfItsScenario(t *testing.T) {
	s := onlyTypes(builtinSuite(t), "dbt-compile")
	worlds := recordWorlds(&s)

	results := s.Run(ctx)

	byScenario := map[conformance.Scenario]conformance.Result{}
	for _, r := range results {
		if r.Type != "dbt-compile" {
			continue
		}
		require.NoError(t, r.Err, "scenario %s", r.Scenario)
		byScenario[r.Scenario] = r
	}
	first := byScenario[conformance.FirstDraftPasses]
	require.Len(t, first.Requests, 1)
	assert.Contains(t, first.Requests[0].User, "unexpected ')'")
	assert.Empty(t, byScenario[conformance.DeclinesBeforeModel].Requests)

	revised := byScenario[conformance.PreflightRevisionPasses].Requests
	require.Len(t, revised, 2)
	assert.NotEqual(t, revised[0], revised[1], "the revision is a request of its own")
	assert.Contains(t, revised[1].User, revised[0].User, "the revision is built on the draft's first request")

	// The requests are the ones the model received: the first world is the one
	// first_draft_passes ran in.
	calls := (*worlds)[0].ModelCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, calls[0].Request, first.Requests[0])
}
