package conformance_test

import (
	"context"
	"log/slog"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/adapters/scripted"
	"github.com/carolsimone/continuo/agent-remediation/adapters/workflowfile"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
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

	failed := map[conformance.Scenario]bool{}
	for _, r := range results {
		if r.Type == "dbt-compile" {
			failed[r.Scenario] = r.Err != nil
		}
	}
	assert.True(t, failed[conformance.FirstDraftPasses])
	assert.True(t, failed[conformance.GateFailsThenPasses])
	assert.True(t, failed[conformance.Redelivery])
	assert.False(t, failed[conformance.PreflightFails])
	assert.False(t, failed[conformance.DeclinesBeforeModel])
}
