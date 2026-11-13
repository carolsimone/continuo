package conformance

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// Scenario names one situation a type or a flow is run through.
type Scenario string

// The scenarios every listed type runs through.
const (
	// FirstDraftPasses: the model's first draft passes pre-flight and the
	// gate.
	FirstDraftPasses Scenario = "first_draft_passes"
	// PreflightFails: pre-flight refuses the model's draft.
	PreflightFails Scenario = "preflight_fails"
	// GateFailsThenPasses: the first try's verification fails and the next
	// try's passes.
	GateFailsThenPasses Scenario = "gate_fails_then_passes"
	// TriesSpent: every try's verification fails until the flow's tries are
	// spent.
	TriesSpent Scenario = "tries_spent"
	// DeclinesBeforeModel: a context step declines before the model is
	// called.
	DeclinesBeforeModel Scenario = "declines_before_model"
	// ModelDeclines: the model declines.
	ModelDeclines Scenario = "model_declines"
	// Redelivery: the first draft passes while the trigger is delivered twice
	// and the worker restarts once after the prompt step.
	Redelivery Scenario = "redelivery"
)

// TypeScenarios is every scenario a listed type runs through, in run order.
func TypeScenarios() []Scenario {
	return []Scenario{FirstDraftPasses, PreflightFails, GateFailsThenPasses, TriesSpent, DeclinesBeforeModel, ModelDeclines, Redelivery}
}

// everyAttemptFails is the script's failed attempts when no verification run
// may pass.
const everyAttemptFails = 1 << 20

// Result is how one type, or one flow, did in one scenario. Err is nil when
// the scenario held.
type Result struct {
	Type     string
	Scenario Scenario
	Err      error
}

// Suite runs the conformance scenarios of a set of loaded files.
type Suite struct {
	// Files are the loaded flows and types.
	Files workflow.Set
	// Fixtures are the loaded fixtures.
	Fixtures []Fixture
	// Locator and Inspector are the contract locator and inspector the
	// python types' steps read the repository checkout with.
	Locator   ports.ContractLocator
	Inspector ports.ContractInspector
	// NewWorld builds the world each scenario runs in.
	NewWorld NewWorld
}

// Run runs every type a flow lists through every type scenario, each in a
// world of its own built from the type's fixture, and returns one result per
// type and scenario, in flow and scenario order. A listed type with no
// fixture, or with several, fails every scenario. A fixture for a type no flow
// lists fails too, with one result of its own.
func (s Suite) Run(ctx context.Context) []Result {
	listed := s.listedTypes()
	byType := map[string][]Fixture{}
	for _, f := range s.Fixtures {
		if f.Type != "" {
			byType[f.Type] = append(byType[f.Type], f)
		}
	}
	var out []Result
	for _, name := range listed {
		fixtures := byType[name]
		for _, sc := range TypeScenarios() {
			var err error
			switch len(fixtures) {
			case 0:
				err = fmt.Errorf("type %s has no fixture", name)
			case 1:
				err = s.runType(ctx, name, fixtures[0], sc)
			default:
				err = fmt.Errorf("type %s has %d fixtures (%s), and a type has one", name, len(fixtures), fixtureFiles(fixtures))
			}
			out = append(out, Result{Type: name, Scenario: sc, Err: err})
		}
	}
	for _, f := range s.Fixtures {
		if f.Type != "" && !slices.Contains(listed, f.Type) {
			out = append(out, Result{Type: f.Type, Err: fmt.Errorf("%s tests type %s, which no flow lists", f.File, f.Type)})
		}
	}
	return out
}

// listedTypes is every type a flow lists, flows in name order and each flow's
// types in its order, each once.
func (s Suite) listedTypes() []string {
	flows := make([]string, 0, len(s.Files.Flows))
	for name := range s.Files.Flows {
		flows = append(flows, name)
	}
	sort.Strings(flows)
	var out []string
	for _, name := range flows {
		for _, t := range s.Files.Flows[name].Types {
			if !slices.Contains(out, t) {
				out = append(out, t)
			}
		}
	}
	return out
}

// runType runs the type name through scenario sc from its fixture f and
// checks what the run wrote.
func (s Suite) runType(ctx context.Context, name string, f Fixture, sc Scenario) error {
	typ, ok := s.Files.Types[name]
	if !ok {
		return fmt.Errorf("type %s is listed but not loaded", name)
	}
	script, trigger, err := typeScript(f, sc)
	if err != nil {
		return err
	}
	sr, err := s.start(f, script, false)
	if err != nil {
		return err
	}
	if err := sr.accept(ctx, trigger); err != nil {
		return err
	}
	if sc == Redelivery {
		if err := sr.redeliverAndRestart(ctx, trigger, typ); err != nil {
			return err
		}
	}
	if err := sr.finish(ctx); err != nil {
		return err
	}
	return sr.expectType(s.Files, trigger, f, typ, sc)
}

// typeScript is what the fakes answer in scenario sc of fixture f, and the
// trigger the scenario delivers.
func typeScript(f Fixture, sc Scenario) (Script, remediation.Trigger, error) {
	script := Script{MemberAnswers: memberPasses(f)}
	trigger := f.Trigger
	switch sc {
	case FirstDraftPasses, Redelivery:
		script.Answers = f.Answers.Pass
	case GateFailsThenPasses:
		script.Answers = f.Answers.Pass
		script.FailedAttempts = 1
	case TriesSpent:
		script.Answers = f.Answers.Pass
		script.FailedAttempts = everyAttemptFails
	case PreflightFails:
		script.Answers = f.Answers.Fail
	case ModelDeclines:
		script.Answers = f.Answers.Decline
	case DeclinesBeforeModel:
		if f.DeclineBeforeModel == nil {
			return Script{}, trigger, fmt.Errorf("%s has no decline_before_model patch", f.File)
		}
		trigger = *f.DeclineBeforeModel
		script.Answers = f.Answers.Pass
	}
	if len(script.Answers) == 0 {
		return Script{}, trigger, fmt.Errorf("%s has no model answers for scenario %s", f.File, sc)
	}
	return script, trigger, nil
}

// memberPasses is the passing answers of every node the fixture answers for
// on its own.
func memberPasses(f Fixture) map[string][]ports.ProposeResult {
	if len(f.MemberAnswers) == 0 {
		return nil
	}
	out := make(map[string][]ports.ProposeResult, len(f.MemberAnswers))
	for node, a := range f.MemberAnswers {
		out[node] = a.Pass
	}
	return out
}

// redeliverAndRestart delivers the trigger a second time, then makes the
// commit after the first model call fail, which loses the first cluster's
// prompt step after the model answered, and restarts the worker.
func (sr *scenarioRun) redeliverAndRestart(ctx context.Context, trigger remediation.Trigger, typ workflow.Type) error {
	if err := sr.accept(ctx, trigger); err != nil {
		return fmt.Errorf("deliver the trigger again: %w", err)
	}
	sr.w.OnModelCall(func(calls int) {
		if calls == 1 {
			sr.w.FailNextCommit()
		}
	})
	if err := sr.finish(ctx); err == nil {
		return errors.New("the run ended although the commit after the first model call failed")
	}
	if row := sr.expect(trigger).stepRows(1)[string(typ.Agent.Prompt.Name)]; row.Failures == 0 {
		return fmt.Errorf("the failed commit was not the prompt step %s of the first cluster", typ.Agent.Prompt.Name)
	}
	sr.w.OnModelCall(nil)
	sr.restart()
	return nil
}

// expectType checks what scenario sc of the type typ wrote. When a draft is
// refused, or a step or the model declines, a shared-upstream type's members
// run their own types with the fixture's passing member answers, a
// cross-service type's members are handed over, and a single cluster is
// handed over itself.
func (sr *scenarioRun) expectType(files workflow.Set, trigger remediation.Trigger, f Fixture, typ workflow.Type, sc Scenario) error {
	e := sr.expect(trigger)
	e.ended()
	first := e.firstCluster(typ.Name)
	switch sc {
	case FirstDraftPasses, Redelivery:
		if as := e.attempts(proposal.StatusProposed); len(as) == 1 {
			e.nodes(as[0], proposal.StatusProposed)
			e.runsPerEditedService(as[0], f.ServiceRepoPaths)
		}
		e.proposedEvents(1)
		if sc == Redelivery {
			e.that(len(sr.w.Remediations()) == 1, "the trigger delivered twice made %d remediations", len(sr.w.Remediations()))
			e.distinctModelRequests()
		}
	case GateFailsThenPasses:
		if as := e.attempts(proposal.StatusFailed, proposal.StatusProposed); len(as) == 2 {
			e.verifyError(as[0], f.GateErrors)
			e.nodes(as[1], proposal.StatusProposed)
		}
		e.proposedEvents(1)
	case TriesSpent:
		as := e.attempts(proposal.StatusFailed, proposal.StatusFailed, proposal.StatusFailed, proposal.StatusEscalated)
		for _, a := range as[:min(3, len(as))] {
			e.verifyError(a, f.GateErrors)
		}
		e.proposedEvents(0)
	default:
		declinedFirst := sc == DeclinesBeforeModel
		switch typ.Match.Cluster {
		case workflow.ClusterSharedUpstream:
			e.membersRanTheirTypes(files, first)
			e.that(e.reachedModel(first, typ) != declinedFirst, "the upstream cluster reached the model: %t, in scenario %s", !declinedFirst, sc)
		case workflow.ClusterCrossService:
			if as := e.attempts(proposal.StatusSkipped); len(as) == 1 {
				e.membersSkipped(first, as[0])
			}
			e.that(e.reachedModel(first, typ) != declinedFirst, "the upstream cluster reached the model: %t, in scenario %s", !declinedFirst, sc)
		default:
			e.singleHandedOver(first, typ, sc)
		}
	}
	return e.err()
}

func fixtureFiles(fs []Fixture) string {
	files := make([]string, 0, len(fs))
	for _, f := range fs {
		files = append(files, f.File)
	}
	return strings.Join(files, ", ")
}
