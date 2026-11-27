package conformance

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// Scenario names one situation a type or a flow is run through.
type Scenario string

// The scenarios every listed type runs through.
const (
	// FirstDraftPasses: the model's first draft passes pre-flight and the
	// gate.
	FirstDraftPasses Scenario = "first_draft_passes"
	// PreflightRevisionPasses: pre-flight refuses the first round's draft and
	// the second round's revision passes it and the gate. A type of one round
	// takes no revision: its refused draft is handed over as its rounds spent,
	// with the first round's calls and no more.
	PreflightRevisionPasses Scenario = "preflight_revision_passes"
	// PreflightRoundsSpent: pre-flight refuses the draft of every round the
	// type allows. The members of a single or a cross-project cluster are
	// handed over as preflight_rounds_spent, in one remediation.handed_over
	// event. The members of a shared-upstream cluster each take their own type
	// and are proposed.
	PreflightRoundsSpent Scenario = "preflight_rounds_spent"
	// GateFailsNextTryLearns: the first try's release check fails, the next
	// try's first request shows why, and its release check passes. In a flow
	// of one try there is no next try: the failed try is handed over.
	GateFailsNextTryLearns Scenario = "gate_fails_next_try_learns"
	// TriesSpent: every try's release check fails until the flow's tries are
	// spent. The failing set is handed over as tries_spent, in one
	// remediation.handed_over event that counts the tries, and the journal
	// holds every try.
	TriesSpent Scenario = "tries_spent"
	// DeclinesBeforeModel: a context step declines before the model is
	// called. The cluster's members are handed over as declined, with no
	// model call and no later try, in one remediation.handed_over event.
	DeclinesBeforeModel Scenario = "declines_before_model"
	// ModelDeclines: the model first answers that it cannot fix the node, with
	// a low confidence or no edit, which pre-flight refuses, and its revision
	// passes. A type of one round takes no revision: the refused answer is
	// handed over as its rounds spent, with the first round's calls and no
	// more.
	ModelDeclines Scenario = "model_declines"
	// Redelivery: the first round's draft is refused and its revision passes
	// while the trigger is delivered three times and the worker restarts
	// twice: once after the first round's first model call, and once between
	// the rounds, after the first round's pre-flight committed. In a type of
	// one round the first round's draft passes, and the second restart comes
	// after that round's pre-flight committed, before the next commit of the
	// try.
	Redelivery Scenario = "redelivery"
)

// TypeScenarios is every scenario a listed type runs through, in run order.
func TypeScenarios() []Scenario {
	return []Scenario{FirstDraftPasses, PreflightRevisionPasses, PreflightRoundsSpent, GateFailsNextTryLearns,
		TriesSpent, DeclinesBeforeModel, ModelDeclines, Redelivery}
}

// everyTryFails is the script's failed tries when no release check
// may pass.
const everyTryFails = 1 << 20

// Result is how one type, or one flow, did in one scenario. Err is nil when
// the scenario held.
type Result struct {
	Type     string
	Scenario Scenario
	Err      error
	// Requests are the model requests the scenario made, in call order, as
	// the model received them.
	Requests []prompt.ProposeRequest
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
	// ModelReferences reads a dbt project in that checkout for the rename of
	// a model.
	ModelReferences ports.ModelReferenceFinder
	// NewWorld builds the world each scenario runs in.
	NewWorld NewWorld
	// Mutate, when set, changes a copy of every fixture before its scenarios
	// run, so a scenario plays the changed fixture and the loaded one stays as
	// it was.
	Mutate func(*Fixture)
}

// Run runs every type a flow lists through every type scenario, each in a
// world of its own built from the type's fixture (as Mutate changed it, when
// the suite has one) and in the flow listedTypes picks for the type, and
// returns one result per type and scenario, in the order listedTypes gives
// the types and in scenario order. A listed type with no fixture, or with
// several, fails every scenario. A fixture that nothing plays fails too, with
// one result of its own: a type fixture for a type no flow lists, and a flow
// fixture for a flow no file defines.
func (s Suite) Run(ctx context.Context) []Result {
	listed := s.listedTypes()
	byType := map[string][]Fixture{}
	for _, f := range s.Fixtures {
		if f.Type != "" {
			byType[f.Type] = append(byType[f.Type], f)
		}
	}
	var out []Result
	for _, lt := range listed {
		fixtures := byType[lt.name]
		if len(fixtures) == 1 {
			fixtures = []Fixture{s.mutated(fixtures[0])}
		}
		for _, sc := range TypeScenarios() {
			var requests []prompt.ProposeRequest
			var err error
			switch len(fixtures) {
			case 0:
				err = fmt.Errorf("type %s has no fixture", lt.name)
			case 1:
				requests, err = s.runType(ctx, lt, fixtures[0], sc)
			default:
				err = fmt.Errorf("type %s has %d fixtures (%s), and a type has one", lt.name, len(fixtures), fixtureFiles(fixtures))
			}
			out = append(out, Result{Type: lt.name, Scenario: sc, Err: err, Requests: requests})
		}
	}
	for _, f := range s.Fixtures {
		switch {
		case f.Type != "" && !slices.ContainsFunc(listed, func(lt listedType) bool { return lt.name == f.Type }):
			out = append(out, Result{Type: f.Type, Err: fmt.Errorf("%s tests type %s, which no flow lists", f.File, f.Type)})
		case f.Flow != "":
			if _, ok := s.Files.Flows[f.Flow]; !ok {
				out = append(out, Result{Type: f.Flow, Scenario: f.Scenario, Err: fmt.Errorf("%s tests flow %s, which no file defines", f.File, f.Flow)})
			}
		}
	}
	return out
}

// listedType is a type a flow lists, with the flow its scenarios run in and
// that flow's tries, which the scenarios expect.
type listedType struct {
	name  string
	flow  string
	tries int
}

// listedTypes is every type a flow lists, each once, with the flow it is
// played in: runner.TriggerFlowName, the flow every heal trigger runs, when
// that flow lists the type, else the first flow by name that lists it. The
// types come in the order of their flows, the trigger flow first and the
// others by name, and in each flow's order.
func (s Suite) listedTypes() []listedType {
	flows := make([]string, 0, len(s.Files.Flows))
	for name := range s.Files.Flows {
		if name != runner.TriggerFlowName {
			flows = append(flows, name)
		}
	}
	sort.Strings(flows)
	if _, ok := s.Files.Flows[runner.TriggerFlowName]; ok {
		flows = append([]string{runner.TriggerFlowName}, flows...)
	}
	var out []listedType
	for _, name := range flows {
		flow := s.Files.Flows[name]
		for _, t := range flow.Types {
			if !slices.ContainsFunc(out, func(lt listedType) bool { return lt.name == t }) {
				out = append(out, listedType{name: t, flow: name, tries: flow.Tries})
			}
		}
	}
	return out
}

// mutated is f after the suite's Mutate has changed a copy of it, or f itself
// when the suite has none.
func (s Suite) mutated(f Fixture) Fixture {
	if s.Mutate == nil {
		return f
	}
	c := f.clone()
	s.Mutate(&c)
	return c
}

// requests is the model requests the scenario made, in call order, as the
// model received them.
func (sr *scenarioRun) requests() []prompt.ProposeRequest {
	calls := sr.w.ModelCalls()
	out := make([]prompt.ProposeRequest, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.Request)
	}
	return out
}

// runType runs the listed type lt through scenario sc from its fixture f, in
// the first flow that lists it, checks what the run wrote, and returns the
// model requests the run made.
func (s Suite) runType(ctx context.Context, lt listedType, f Fixture, sc Scenario) ([]prompt.ProposeRequest, error) {
	typ, ok := s.Files.Types[lt.name]
	if !ok {
		return nil, fmt.Errorf("type %s is listed but not loaded", lt.name)
	}
	plan, err := typeScript(f, typ, lt.tries, sc)
	if err != nil {
		return nil, err
	}
	oneRunCalls := 0
	if sc == Redelivery {
		if oneRunCalls, err = s.modelCallsOfOneRun(ctx, f, lt.flow, plan); err != nil {
			return nil, fmt.Errorf("count the model calls of a run with no restart: %w", err)
		}
	}
	sr, err := s.start(f, lt.flow, plan.script, false)
	if err != nil {
		return nil, err
	}
	if err := sr.accept(ctx, plan.trigger); err != nil {
		return sr.requests(), err
	}
	if sc == Redelivery {
		if err := sr.redeliverAndRestart(ctx, plan, typ); err != nil {
			return sr.requests(), err
		}
	}
	if err := sr.finish(ctx); err != nil {
		return sr.requests(), err
	}
	return sr.requests(), sr.expectType(s.Files, f, typ, plan, sc, oneRunCalls)
}

// modelCallsOfOneRun is how often the model is called when the plan's trigger
// is delivered once and driven to its end in flow, in a world of its own, with
// no restart.
func (s Suite) modelCallsOfOneRun(ctx context.Context, f Fixture, flow string, plan typePlan) (int, error) {
	sr, err := s.start(f, flow, plan.script, false)
	if err != nil {
		return 0, err
	}
	if err := sr.accept(ctx, plan.trigger); err != nil {
		return 0, err
	}
	if err := sr.finish(ctx); err != nil {
		return 0, err
	}
	return len(sr.w.ModelCalls()), nil
}

// answerKind names how a round's draft ends, and so which of a fixture's
// answer lists the round answers from.
type answerKind string

const (
	passes   answerKind = "pass"
	fails    answerKind = "fail"
	declines answerKind = "decline"
)

// round is the answers of a round that ends as k: the first round's list of
// kind k, and in a later round the revision's list of kind k, or the first
// round's when the revision has none.
func (a Answers) round(first bool, k answerKind) []ports.ProposeResult {
	var firstRound, revision []ports.ProposeResult
	switch k {
	case passes:
		firstRound, revision = a.Pass, a.Revision.Pass
	case fails:
		firstRound, revision = a.Fail, a.Revision.Fail
	case declines:
		firstRound, revision = a.Decline, a.Revision.Decline
	}
	if first || len(revision) == 0 {
		return firstRound
	}
	return revision
}

// try is the answers of one try whose rounds end as rounds says, in call
// order, or an error naming the first round the fixture holds no answer for.
func (a Answers) try(file string, rounds ...answerKind) ([]ports.ProposeResult, error) {
	var out []ports.ProposeResult
	for i, k := range rounds {
		answers := a.round(i == 0, k)
		if len(answers) == 0 {
			list := "answers." + string(k)
			if i > 0 {
				list = "answers.revision." + string(k) + " or " + list
			}
			return nil, fmt.Errorf("%s has no %s for round %d", file, list, i+1)
		}
		out = append(out, answers...)
	}
	return out, nil
}

// typePlan is what the fakes answer in one scenario of a type, the trigger
// the scenario delivers, and what its expectations count: the tries scripted,
// the round the first cluster's draft passes pre-flight in (0 when no round
// passes), and the scripted calls of the first round, and of one try, of the
// first cluster.
type typePlan struct {
	script          Script
	trigger         remediation.Trigger
	tries           int
	passRound       int
	firstRoundCalls int
	firstTryCalls   int
}

// revises reports whether typ allows a round after the first, in which the
// prompt revises a draft pre-flight refused.
func revises(typ workflow.Type) bool {
	return typ.Agent.Rounds > 1
}

// refusedThenPassed is the rounds of a try whose first round ends as first and
// whose revision passes: both rounds when typ allows a revision, else the
// first round alone, which spends the type's one round.
func refusedThenPassed(typ workflow.Type, first answerKind) []answerKind {
	if revises(typ) {
		return []answerKind{first, passes}
	}
	return []answerKind{first}
}

// typeScript is the plan of scenario sc of the type typ from its fixture f,
// in a flow that makes tries tries.
func typeScript(f Fixture, typ workflow.Type, tries int, sc Scenario) (typePlan, error) {
	members, err := memberPasses(f)
	if err != nil {
		return typePlan{}, err
	}
	plan := typePlan{script: Script{MemberAnswers: members}, trigger: f.Trigger}
	var rounds []answerKind
	repeat := 1
	switch sc {
	case FirstDraftPasses:
		rounds = []answerKind{passes}
	case PreflightRevisionPasses:
		rounds = refusedThenPassed(typ, fails)
	case Redelivery:
		rounds = []answerKind{passes}
		if revises(typ) {
			rounds = []answerKind{fails, passes}
		}
	case PreflightRoundsSpent:
		for range typ.Agent.Rounds {
			rounds = append(rounds, fails)
		}
	case ModelDeclines:
		rounds = refusedThenPassed(typ, declines)
	case GateFailsNextTryLearns:
		rounds, repeat = []answerKind{passes}, min(2, tries)
		plan.script.FailedTries = 1
	case TriesSpent:
		rounds, repeat = []answerKind{passes}, tries
		plan.script.FailedTries = everyTryFails
	case DeclinesBeforeModel:
		// The cluster declines before its model is called, so no call of it
		// is scripted and any call fails.
		if f.DeclineBeforeModel == nil {
			return typePlan{}, fmt.Errorf("%s has no decline_before_model patch", f.File)
		}
		plan.trigger = *f.DeclineBeforeModel
		return plan, nil
	}
	oneTry, err := f.Answers.try(f.File, rounds...)
	if err != nil {
		return typePlan{}, fmt.Errorf("scenario %s: %w", sc, err)
	}
	for range repeat {
		plan.script.Answers = append(plan.script.Answers, oneTry...)
	}
	plan.tries = repeat
	if rounds[len(rounds)-1] == passes {
		plan.passRound = len(rounds)
	}
	plan.firstRoundCalls = len(f.Answers.round(true, rounds[0]))
	plan.firstTryCalls = len(oneTry)
	return plan, nil
}

// memberPasses is the passing answers of one try of every node the fixture
// answers for on its own.
func memberPasses(f Fixture) (map[string][]ports.ProposeResult, error) {
	if len(f.MemberAnswers) == 0 {
		return nil, nil
	}
	out := make(map[string][]ports.ProposeResult, len(f.MemberAnswers))
	for _, node := range sortedKeys(f.MemberAnswers) {
		answers, err := f.MemberAnswers[node].try(f.File+" member "+node, passes)
		if err != nil {
			return nil, err
		}
		out[node] = answers
	}
	return out, nil
}

// redeliverAndRestart delivers the trigger a second time and loses the commit
// after the first model call, which loses the first round's prompt step after
// the model answered, and restarts the worker. It then loses the commit that
// follows the first round's pre-flight: in a type that revises, the commit
// after the first model call of the second round, and in a type of one round
// the next commit of the try. It delivers the trigger a third time and
// restarts the worker again.
func (sr *scenarioRun) redeliverAndRestart(ctx context.Context, plan typePlan, typ workflow.Type) error {
	prompt := string(typ.Agent.Prompt.Name)
	if err := sr.accept(ctx, plan.trigger); err != nil {
		return fmt.Errorf("deliver the trigger again: %w", err)
	}
	if err := sr.loseCommitAfterCall(ctx, 1); err != nil {
		return err
	}
	if row, _ := sr.expect(plan.trigger).stepRow(1, 1, 1, prompt); row.Failures == 0 {
		return fmt.Errorf("the failed commit was not round 1 of the prompt step %s of the first cluster", prompt)
	}
	sr.restart()

	if revises(typ) {
		if err := sr.loseCommitAfterCall(ctx, plan.firstRoundCalls+1); err != nil {
			return err
		}
	} else if err := sr.loseCommitAfterPreflight(ctx, plan.trigger); err != nil {
		return err
	}
	e := sr.expect(plan.trigger)
	if row, _ := e.stepRow(1, 1, 1, remediation.StepPreflight); row.Status != remediation.StepDone {
		return errors.New("the first cluster's round 1 pre-flight had not committed before the second failed commit")
	}
	if row, _ := e.stepRow(1, 1, 2, prompt); revises(typ) && row.Failures == 0 {
		return fmt.Errorf("the second failed commit was not round 2 of the prompt step %s of the first cluster", prompt)
	}
	if err := sr.accept(ctx, plan.trigger); err != nil {
		return fmt.Errorf("deliver the trigger a third time: %w", err)
	}
	sr.restart()
	return nil
}

// loseCommitAfterPreflight drives the remediation with the commit that
// follows the first cluster's round 1 pre-flight failing, and expects the
// drive to fail.
func (sr *scenarioRun) loseCommitAfterPreflight(ctx context.Context, t remediation.Trigger) error {
	sr.afterCommit = func() {
		if row, _ := sr.expect(t).stepRow(1, 1, 1, remediation.StepPreflight); row.Status == remediation.StepDone {
			sr.afterCommit = nil
			sr.w.FailNextCommit()
		}
	}
	defer func() { sr.afterCommit = nil }()
	if err := sr.finish(ctx); err == nil {
		return errors.New("the run ended although the commit after the first cluster's round 1 pre-flight failed")
	}
	return nil
}

// loseCommitAfterCall drives the remediation with the commit after the call
// numbered n to reach the model failing, and expects the drive to fail.
func (sr *scenarioRun) loseCommitAfterCall(ctx context.Context, n int) error {
	sr.w.OnModelCall(func(calls int) {
		if calls == n {
			sr.w.FailNextCommit()
		}
	})
	defer sr.w.OnModelCall(nil)
	if err := sr.finish(ctx); err == nil {
		return fmt.Errorf("the run ended although the commit after model call %d failed", n)
	}
	return nil
}

// expectType checks what scenario sc of the type typ wrote, from fixture f
// and the scenario's plan. oneRunCalls is how often the model is called when
// the trigger is delivered once and nothing restarts, which a redelivery and a
// restart must not change.
func (sr *scenarioRun) expectType(files workflow.Set, f Fixture, typ workflow.Type, plan typePlan, sc Scenario, oneRunCalls int) error {
	e := sr.expect(plan.trigger)
	e.ended()
	first := e.firstCluster(typ.Name)
	switch sc {
	case FirstDraftPasses:
		e.proposed(f)
		e.passedInRound(first, typ, 1)
		e.clusterCalls(first, plan.firstTryCalls)
	case PreflightRevisionPasses, ModelDeclines:
		if plan.passRound == 0 {
			e.refusedEveryRound(files, first, typ, plan)
			break
		}
		e.proposed(f)
		e.passedInRound(first, typ, plan.passRound)
		e.clusterCalls(first, plan.firstTryCalls)
	case Redelivery:
		e.proposed(f)
		e.passedInRound(first, typ, plan.passRound)
		e.that(len(sr.w.Remediations()) == 1, "the trigger delivered three times made %d remediations", len(sr.w.Remediations()))
		e.distinctModelRequests()
		e.modelCalls(oneRunCalls)
		e.clusterCalls(first, plan.firstTryCalls)
	case PreflightRoundsSpent:
		e.refusedEveryRound(files, first, typ, plan)
	case GateFailsNextTryLearns:
		if plan.tries < 2 {
			e.triesSpent(f, plan)
			break
		}
		if as := e.fixRows(fix.StatusFailed, fix.StatusProposed); len(as) == 2 {
			e.verifyError(as[0], f.GateErrors)
			e.nodes(as[1], fix.StatusProposed)
		}
		e.proposedEvents(1)
		e.noHandedOver()
		e.nextTryShownGateErrors(first, plan.firstTryCalls, f.GateErrors)
	case TriesSpent:
		e.triesSpent(f, plan)
	case DeclinesBeforeModel:
		switch typ.Match.Cluster {
		case workflow.ClusterSharedUpstream:
			e.membersRanTheirTypes(files, first)
			e.that(!e.reachedModel(first, typ), "the upstream cluster that declined before the model reached it")
		case workflow.ClusterCrossProject:
			if as := e.fixRows(fix.StatusSkipped); len(as) == 1 {
				e.membersSkipped(first, as[0], model.HandoverReasonDeclined)
			}
			e.that(!e.reachedModel(first, typ), "the upstream cluster that declined before the model reached it")
			e.modelCalls(0)
		default:
			e.singleHandedOver(first, typ, false)
		}
	}
	return e.err()
}

// refusedEveryRound expects pre-flight to have refused the first cluster's
// draft in every round its type allows, with the plan's calls of one try, and
// the cluster to have ended by its shape: a shared-upstream cluster's members
// each ran their own type, a cross-project cluster's members were skipped with
// the code preflight_rounds_spent and the last round's first finding in their
// reason, and a single cluster was handed over with its last round's first
// finding.
func (e *expectation) refusedEveryRound(files workflow.Set, first remediation.Cluster, typ workflow.Type, plan typePlan) {
	e.roundsSpent(first, typ, plan.firstTryCalls)
	switch typ.Match.Cluster {
	case workflow.ClusterSharedUpstream:
		e.membersRanTheirTypes(files, first)
	case workflow.ClusterCrossProject:
		if as := e.fixRows(fix.StatusSkipped); len(as) == 1 {
			e.membersSkipped(first, as[0], model.HandoverReasonPreflightRoundsSpent)
		}
		finding, ok := e.firstFinding(first, typ)
		e.that(ok, "pre-flight refused nothing in cluster %d", first.Cluster)
		e.that(first.Outcome != nil && strings.HasSuffix(first.Outcome.Reason, ": "+finding.Message),
			"cluster %d ended with %+v, want a reason that ends with the first finding %q", first.Cluster, first.Outcome, finding.Message)
		e.modelCalls(plan.firstTryCalls)
	default:
		e.singleHandedOver(first, typ, true)
	}
}

// triesSpent expects every one of the plan's tries to have failed its
// release check with the fixture's gate errors, the journal to hold each try,
// the failing set handed over as tries_spent by one remediation.handed_over
// event that counts the tries, and no fix.
func (e *expectation) triesSpent(f Fixture, plan typePlan) {
	failed := slices.Repeat([]fix.Status{fix.StatusFailed}, plan.tries)
	as := e.fixRows(append(failed, fix.StatusEscalated)...)
	for _, a := range as[:min(plan.tries, len(as))] {
		e.verifyError(a, f.GateErrors)
	}
	e.everyTryFailed(plan.tries)
	if len(as) == plan.tries+1 {
		e.handedOver(as[plan.tries], plan.tries, codes(model.HandoverReasonTriesSpent, e.trigger.NodeIDs()...))
	}
	e.proposedEvents(0)
}

func fixtureFiles(fs []Fixture) string {
	files := make([]string, 0, len(fs))
	for _, f := range fs {
		files = append(files, f.File)
	}
	return strings.Join(files, ", ")
}
