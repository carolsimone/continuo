package conformance_test

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/adapters/scripted"
	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/workflows"
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
		Files:     files,
		Fixtures:  fixtures,
		Locator:   locator,
		Inspector: locator,
		NewWorld:  scripted.Worlds(memory.NewStore),
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

func TestSuite_BuiltinFilesPass(t *testing.T) {
	s := builtinSuite(t)
	warnings, err := runner.Validate(s.Files, runner.NewRegistry(runner.Collaborators{}))
	require.NoError(t, err)
	require.Empty(t, warnings)

	results := s.Run(ctx)
	flow := s.Files.Flows[flowName]
	require.Len(t, flow.Types, 10)
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

// A fixture whose passing answer edits a file the model was never shown is
// refused by pre-flight, so the scenarios that expect a proposal fail: the
// suite judges what the files do, not only that they run.
func TestSuite_AnAnswerPreflightRefusesFailsTheScenariosThatExpectAProposal(t *testing.T) {
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
	const noEvent = "0 remediation.proposed events were enqueued, want 1"
	for sc, want := range map[conformance.Scenario][]string{
		conformance.FirstDraftPasses:    {"the attempts are [skipped], want [proposed]", noEvent},
		conformance.GateFailsThenPasses: {"the attempts are [skipped], want [failed proposed]", noEvent},
		conformance.TriesSpent:          {"the attempts are [skipped], want [failed failed failed escalated]", "attempt 1 carries no verify error"},
		conformance.Redelivery:          {"the attempts are [skipped], want [proposed]", noEvent},
	} {
		require.Error(t, errs[sc], "scenario %s", sc)
		for _, w := range want {
			assert.Contains(t, errs[sc].Error(), w, "scenario %s", sc)
		}
	}
	assert.NoError(t, errs[conformance.PreflightFails])
	assert.NoError(t, errs[conformance.DeclinesBeforeModel])
	assert.NoError(t, errs[conformance.ModelDeclines])
}

// extraRuns is a world that also reports verification runs of services no
// edit touches, in the order given.
type extraRuns struct {
	conformance.World
	services []string
}

func (w extraRuns) Runs() []ports.VerificationRequest {
	runs := w.World.Runs()
	for _, s := range w.services {
		runs = append(runs, ports.VerificationRequest{RunID: "extra-" + s, Service: s})
	}
	return runs
}

// withExtraRuns makes every world of the suite report a run of each service.
func withExtraRuns(s conformance.Suite, services ...string) conformance.Suite {
	base := s.NewWorld
	s.NewWorld = func(f conformance.Fixture, sc conformance.Script) (conformance.World, error) {
		w, err := base(f, sc)
		if err != nil {
			return nil, err
		}
		return extraRuns{World: w, services: services}, nil
	}
	return s
}

// A failure names the services and the nodes it is about in sorted order, so
// remediation-check prints the same text on every run whatever order a map or
// the world hands them over in.
func TestSuite_AFailureNamesItsServicesAndNodesInSortedOrder(t *testing.T) {
	base := onlyTypes(builtinSuite(t), "dbt-compile")
	const (
		runsText = "service alpha had 1 verification runs and no edit; service zeta had 1 verification runs and no edit"
		gateText = `does not name alpha.node's error "alpha broke"; attempt 1's verify error`
	)

	var texts []string
	for i := range 10 {
		// Each pass hands the services and the gate errors over in the other
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
				for _, service := range order {
					s.Fixtures[j].GateErrors[service+".node"] = service + " broke"
				}
			}
		}
		errs := map[conformance.Scenario]error{}
		for _, r := range withExtraRuns(s, order...).Run(ctx) {
			if r.Type == "dbt-compile" {
				errs[r.Scenario] = r.Err
			}
		}
		require.Error(t, errs[conformance.FirstDraftPasses])
		assert.Contains(t, errs[conformance.FirstDraftPasses].Error(), runsText)
		require.Error(t, errs[conformance.GateFailsThenPasses])
		gate := errs[conformance.GateFailsThenPasses].Error()
		assert.Contains(t, gate, gateText)
		assert.Less(t, strings.Index(gate, "alpha.node"), strings.Index(gate, "zeta.node"))
		texts = append(texts, errs[conformance.FirstDraftPasses].Error()+"\n"+gate)
	}
	for _, text := range texts[1:] {
		assert.Equal(t, texts[0], text)
	}
}
