package runner

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/streams"
)

const (
	flowFile = "flows/f.yaml"
	typeFile = "types/t.yaml"
)

type stubStep struct{ meta step.Meta }

func (s stubStep) Meta() step.Meta { return s.meta }

// stubFor returns a stub that implements the interface of m's Kind.
func stubFor(m step.Meta) described {
	base := stubStep{m}
	switch m.Kind {
	case step.KindContext:
		return stubContext{base}
	case step.KindPrompt:
		return stubPrompt{base}
	case step.KindCheck:
		return stubCheck{base}
	case step.KindOutput:
		return stubPass{base}
	default:
		return base
	}
}

type stubContext struct{ stubStep }

func (stubContext) Gather(context.Context, step.ContextInput) (step.ContextOutput, error) {
	return step.ContextOutput{}, nil
}

type stubPrompt struct{ stubStep }

func (stubPrompt) Propose(context.Context, step.PromptInput, step.Model) (step.PromptOutput, error) {
	return step.PromptOutput{}, nil
}

type stubCheck struct{ stubStep }

func (stubCheck) Check(context.Context, step.CheckInput) (step.CheckOutput, error) {
	return step.CheckOutput{}, nil
}

type stubPass struct{ stubStep }

func (stubPass) Pass(context.Context, uow.UnitOfWork, step.OutputInput, remediation.Verified) error {
	return nil
}

// stubRegistry holds one stub step per kind and stage the checks care about and
// none of the real steps, so each test states the evidence a step reads and
// provides itself.
func stubRegistry() *Registry {
	r := &Registry{
		steps:    map[workflow.StepName]entry{},
		evidence: map[remediation.EvidenceKey]func() any{},
	}
	for _, m := range []step.Meta{
		{Name: "dbt_log", Kind: step.KindContext, Provides: []remediation.EvidenceKey{remediation.KeyDBTLog}},
		{Name: "failing_node_source", Kind: step.KindContext, Provides: []remediation.EvidenceKey{remediation.KeySourceFiles}},
		{Name: "reads_dbt_log", Kind: step.KindContext, Needs: []remediation.EvidenceKey{remediation.KeyDBTLog}},
		{Name: "reads_the_draft", Kind: step.KindContext, Needs: []remediation.EvidenceKey{remediation.KeyDraft}},
		{Name: "dbt-compile", Kind: step.KindPrompt, Needs: []remediation.EvidenceKey{remediation.KeySourceFiles}},
		{Name: "dbt-parse", Kind: step.KindPrompt},
		{Name: "target_file_was_shown", Kind: step.KindCheck, Stages: []workflow.Stage{workflow.StagePreflight}, Needs: []remediation.EvidenceKey{remediation.KeyDraft}},
		{Name: "needs_a_precedent", Kind: step.KindCheck, Stages: []workflow.Stage{workflow.StagePreflight}, Needs: []remediation.EvidenceKey{remediation.KeyFailurePrecedents}},
		{Name: "edits_map_to_a_service", Kind: step.KindCheck, Stages: []workflow.Stage{workflow.StageCrossCluster}},
		{Name: "verification_run", Kind: step.KindCheck, Stages: []workflow.Stage{workflow.StageGate}, MayStandAsGateRun: true},
		{Name: "a_gate_without_a_run", Kind: step.KindCheck, Stages: []workflow.Stage{workflow.StageGate}},
		{Name: "open_pr", Kind: step.KindOutput, Stages: []workflow.Stage{workflow.StageOutput}},
	} {
		m.Description = string(m.Name)
		r.register(stubFor(m))
	}
	return r
}

func ref(file string, line int, name string) workflow.StepRef {
	return workflow.StepRef{Name: workflow.StepName(name), Pos: workflow.Pos{File: file, Line: line}}
}

func pos(file string, line int) workflow.Pos { return workflow.Pos{File: file, Line: line} }

func validFlow() workflow.Flow {
	return workflow.Flow{
		Version:        1,
		Name:           "f",
		Trigger:        "remediation.requested",
		Types:          []string{"t"},
		TypesPos:       pos(flowFile, 5),
		Context:        []workflow.StepRef{ref(flowFile, 6, "failing_node_source")},
		AcrossClusters: []workflow.StepRef{ref(flowFile, 7, "edits_map_to_a_service")},
		Gate:           []workflow.StepRef{ref(flowFile, 8, "verification_run")},
		Tries:          3,
		OnPass:         workflow.Output{Action: workflow.OutputPullRequest, Emits: "remediation.proposed", Pos: pos(flowFile, 11)},
		OnGiveUp:       workflow.Output{Action: workflow.OutputHandOver, Pos: pos(flowFile, 12)},
		File:           flowFile,
		Pos:            pos(flowFile, 1),
	}
}

func validType() workflow.Type {
	return workflow.Type{
		Version:   1,
		Name:      "t",
		Match:     workflow.Match{Cluster: workflow.ClusterSingle, Source: "compile", Pos: pos(typeFile, 4)},
		Context:   []workflow.StepRef{ref(typeFile, 5, "dbt_log")},
		Agent:     workflow.Agent{Prompt: ref(typeFile, 6, "dbt-compile"), Rounds: 2, Pos: pos(typeFile, 6)},
		Preflight: []workflow.StepRef{ref(typeFile, 7, "target_file_was_shown")},
		File:      typeFile,
		Pos:       pos(typeFile, 1),
	}
}

func setOf(f workflow.Flow, types ...workflow.Type) workflow.Set {
	s := workflow.Set{Flows: map[string]workflow.Flow{f.Name: f}, Types: map[string]workflow.Type{}}
	for _, t := range types {
		s.Types[t.Name] = t
	}
	return s
}

// refuses runs Validate on the valid set after change and requires an error
// holding every one of wants.
func refuses(t *testing.T, change func(*workflow.Flow, *workflow.Type), wants ...string) {
	t.Helper()
	f, ty := validFlow(), validType()
	change(&f, &ty)
	_, err := Validate(setOf(f, ty), stubRegistry())
	require.Error(t, err)
	for _, w := range wants {
		assert.Contains(t, err.Error(), w)
	}
}

func TestValidate_AcceptsAValidFlowAndType(t *testing.T) {
	warnings, err := Validate(setOf(validFlow(), validType()), stubRegistry())
	require.NoError(t, err)
	assert.Empty(t, warnings)
}

func TestValidate_RefusesAnUnknownStep(t *testing.T) {
	refuses(t, func(f *workflow.Flow, _ *workflow.Type) {
		f.Gate = []workflow.StepRef{ref(flowFile, 8, "verification_run"), ref(flowFile, 9, "no_such_step")}
	}, `flows/f.yaml:9`, `no_such_step`)
}

func TestValidate_RefusesAStepInAStageItIsNotAllowedIn(t *testing.T) {
	t.Run("a gate step under preflight", func(t *testing.T) {
		refuses(t, func(_ *workflow.Flow, ty *workflow.Type) {
			ty.Preflight = append(ty.Preflight, ref(typeFile, 8, "verification_run"))
		}, `types/t.yaml:8`, `verification_run`, `preflight`)
	})
	t.Run("a context step as the prompt", func(t *testing.T) {
		refuses(t, func(_ *workflow.Flow, ty *workflow.Type) {
			ty.Agent.Prompt = ref(typeFile, 6, "dbt_log")
		}, `types/t.yaml:6`, `dbt_log`, `prompt`)
	})
	t.Run("a prompt step in a context list", func(t *testing.T) {
		refuses(t, func(_ *workflow.Flow, ty *workflow.Type) {
			ty.Context = append(ty.Context, ref(typeFile, 9, "dbt-parse"))
		}, `types/t.yaml:9`, `dbt-parse`, `context`)
	})
	t.Run("a preflight check in the gate", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) {
			f.Gate = append(f.Gate, ref(flowFile, 9, "target_file_was_shown"))
		}, `flows/f.yaml:9`, `target_file_was_shown`, `gate`)
	})
	t.Run("a cross-cluster check in preflight", func(t *testing.T) {
		refuses(t, func(_ *workflow.Flow, ty *workflow.Type) {
			ty.Preflight = append(ty.Preflight, ref(typeFile, 8, "edits_map_to_a_service"))
		}, `types/t.yaml:8`, `edits_map_to_a_service`)
	})
}

func TestValidate_RefusesAMissingStage(t *testing.T) {
	t.Run("no gate", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) { f.Gate = nil }, `flows/f.yaml:1`, `gate`)
	})
	t.Run("no types", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) { f.Types = nil }, `flows/f.yaml:5`, `types`)
	})
	t.Run("no agent prompt", func(t *testing.T) {
		refuses(t, func(_ *workflow.Flow, ty *workflow.Type) { ty.Agent.Prompt = workflow.StepRef{} }, `types/t.yaml:6`, `agent.prompt`)
	})
	t.Run("no on_pass", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) {
			f.OnPass = workflow.Output{Pos: pos(flowFile, 11)}
		}, `flows/f.yaml:11`, `on_pass`)
	})
	t.Run("no on_give_up", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) {
			f.OnGiveUp = workflow.Output{Pos: pos(flowFile, 12)}
		}, `flows/f.yaml:12`, `on_give_up`)
	})
}

func TestValidate_RefusesAFlowListingAMissingType(t *testing.T) {
	refuses(t, func(f *workflow.Flow, _ *workflow.Type) {
		f.Types = []string{"t", "ghost"}
	}, `flows/f.yaml:5`, `ghost`)
}

func TestValidate_RefusesAGateWithNoRealRun(t *testing.T) {
	refuses(t, func(f *workflow.Flow, _ *workflow.Type) {
		f.Gate = []workflow.StepRef{ref(flowFile, 8, "a_gate_without_a_run")}
	}, `flows/f.yaml:`, `gate`, `verification`)
}

// The runner runs a gate of one long check, so a gate of more steps, even one
// step named twice, is refused at the line of its second step rather than
// failing every remediation at its gate.
func TestValidate_RefusesAGateOfMoreThanOneStep(t *testing.T) {
	t.Run("two steps", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) {
			f.Gate = []workflow.StepRef{ref(flowFile, 8, "verification_run"), ref(flowFile, 9, "a_gate_without_a_run")}
		}, `flows/f.yaml:9: flow "f" has 2 gate steps`)
	})
	t.Run("one step twice", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) {
			f.Gate = []workflow.StepRef{ref(flowFile, 8, "verification_run"), ref(flowFile, 9, "verification_run")}
		}, `flows/f.yaml:9: flow "f" has 2 gate steps`)
	})
}

func TestValidate_RefusesAnUnprovidedNeed(t *testing.T) {
	t.Run("a flow context step needs what only the type context provides", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) {
			f.Context = append(f.Context, ref(flowFile, 9, "reads_dbt_log"))
		}, `flows/f.yaml:9`, `reads_dbt_log`, `dbt_log`)
	})
	t.Run("a context step needs the draft", func(t *testing.T) {
		refuses(t, func(_ *workflow.Flow, ty *workflow.Type) {
			ty.Context = append(ty.Context, ref(typeFile, 9, "reads_the_draft"))
		}, `types/t.yaml:9`, `reads_the_draft`, `draft`)
	})
	t.Run("the prompt needs evidence no context provides", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) {
			f.Context = nil
		}, `types/t.yaml:6`, `dbt-compile`, `source_files`)
	})
	t.Run("a preflight check needs evidence nothing provides", func(t *testing.T) {
		refuses(t, func(_ *workflow.Flow, ty *workflow.Type) {
			ty.Preflight = append(ty.Preflight, ref(typeFile, 8, "needs_a_precedent"))
		}, `types/t.yaml:8`, `needs_a_precedent`, `failure_precedents`)
	})
	t.Run("a later step may read what an earlier step of the chain provides", func(t *testing.T) {
		f, ty := validFlow(), validType()
		ty.Context = append(ty.Context, ref(typeFile, 9, "reads_dbt_log"))
		_, err := Validate(setOf(f, ty), stubRegistry())
		require.NoError(t, err)
	})
	t.Run("the draft is available to preflight", func(t *testing.T) {
		_, err := Validate(setOf(validFlow(), validType()), stubRegistry())
		require.NoError(t, err)
	})
}

func TestValidate_RefusesRoundsOrTriesBelowOne(t *testing.T) {
	t.Run("rounds", func(t *testing.T) {
		refuses(t, func(_ *workflow.Flow, ty *workflow.Type) { ty.Agent.Rounds = 0 }, `types/t.yaml:6`, `rounds`)
	})
	t.Run("tries", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) { f.Tries = 0 }, `flows/f.yaml:1`, `tries`)
	})
}

func TestValidate_RefusesAnUnknownOutputAction(t *testing.T) {
	t.Run("on_pass", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) { f.OnPass.Action = "hand_over" }, `flows/f.yaml:11`, `on_pass`, `hand_over`)
	})
	t.Run("on_give_up", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) { f.OnGiveUp.Action = "pull_request" }, `flows/f.yaml:12`, `on_give_up`, `pull_request`)
	})
}

func TestValidate_RefusesAnEventNotInTheContract(t *testing.T) {
	refuses(t, func(f *workflow.Flow, _ *workflow.Type) { f.OnPass.Emits = "remediation.invented" }, `flows/f.yaml:11`, `remediation.invented`)
}

func TestValidate_AcceptsEveryEventOfTheContract(t *testing.T) {
	for _, name := range streams.All {
		f, ty := validFlow(), validType()
		f.OnGiveUp.Emits = strings.SplitN(name, ":", 2)[0]
		_, err := Validate(setOf(f, ty), stubRegistry())
		assert.NoError(t, err, name)
	}
}

func TestValidate_RefusesAnUnsupportedVersion(t *testing.T) {
	t.Run("flow", func(t *testing.T) {
		refuses(t, func(f *workflow.Flow, _ *workflow.Type) { f.Version = 2 }, `flows/f.yaml:1`, `version`)
	})
	t.Run("type", func(t *testing.T) {
		refuses(t, func(_ *workflow.Flow, ty *workflow.Type) { ty.Version = 2 }, `types/t.yaml:1`, `version`)
	})
}

func TestValidate_RefusesTwoTypesThatSelectTheSameFailure(t *testing.T) {
	f, a, b := validFlow(), validType(), validType()
	b.Name = "u"
	b.File = "types/u.yaml"
	b.Match.Pos = pos("types/u.yaml", 4)
	f.Types = []string{"t", "u"}
	_, err := Validate(setOf(f, a, b), stubRegistry())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `types/u.yaml:4`)
	assert.Contains(t, err.Error(), `"t"`)
	assert.Contains(t, err.Error(), `"u"`)
}

func TestValidate_AcceptsTwoTypesWithDisjointMatches(t *testing.T) {
	f, a, b := validFlow(), validType(), validType()
	b.Name = "u"
	b.Match.Source = "parse"
	f.Types = []string{"t", "u"}
	_, err := Validate(setOf(f, a, b), stubRegistry())
	require.NoError(t, err)
}

func TestValidate_RefusesAnUnknownTrigger(t *testing.T) {
	refuses(t, func(f *workflow.Flow, _ *workflow.Type) { f.Trigger = "release.promoted" }, `flows/f.yaml:1`, `trigger`, `release.promoted`)
}

func TestValidate_RefusesATypeThatMatchesNoNodeType(t *testing.T) {
	refuses(t, func(_ *workflow.Flow, ty *workflow.Type) { ty.Match.NodeTypes = []string{} }, `types/t.yaml:4`, `node type`)
}

func TestValidate_RefusesAnUnknownClusterKind(t *testing.T) {
	refuses(t, func(_ *workflow.Flow, ty *workflow.Type) { ty.Match.Cluster = "pair" }, `types/t.yaml:4`, `cluster`)
}

func TestValidate_RefusesReservedAgentTools(t *testing.T) {
	refuses(t, func(_ *workflow.Flow, ty *workflow.Type) {
		ty.Agent.Tools = []workflow.StepRef{ref(typeFile, 6, "web_search")}
	}, `types/t.yaml:6`, `tools`)
}

func TestValidate_JoinsEveryRefusal(t *testing.T) {
	f, ty := validFlow(), validType()
	f.Tries = 0
	ty.Agent.Rounds = 0
	_, err := Validate(setOf(f, ty), stubRegistry())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `flows/f.yaml:1`)
	assert.Contains(t, err.Error(), `types/t.yaml:6`)
}

func TestValidate_WarnsAboutATypeNoFlowLists(t *testing.T) {
	orphan := validType()
	orphan.Name = "orphan"
	orphan.File = "types/orphan.yaml"
	orphan.Pos = pos("types/orphan.yaml", 1)
	warnings, err := Validate(setOf(validFlow(), validType(), orphan), stubRegistry())
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], `types/orphan.yaml:1`)
	assert.Contains(t, warnings[0], `orphan`)
}
