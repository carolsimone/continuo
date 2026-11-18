package conformance

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// The scenarios a flow runs through, each played by a flow fixture.
const (
	// FlowTwoTypesOneService: two clusters of two types fix one service.
	FlowTwoTypesOneService Scenario = "two_types_one_service"
	// FlowTwoServices: the try's edits change two services.
	FlowTwoServices Scenario = "two_services"
	// FlowCrossClusterFails: two clusters edit one path in every try, so every
	// try fails before the gate and the flow's tries are spent.
	FlowCrossClusterFails Scenario = "cross_cluster_fails"
	// FlowSharedUpstreamDeclines: a shared-upstream cluster declines.
	FlowSharedUpstreamDeclines Scenario = "shared_upstream_declines"
	// FlowCrossServiceDeclines: a cross-service cluster declines.
	FlowCrossServiceDeclines Scenario = "cross_service_declines"
	// FlowNoType: a failure no type handles.
	FlowNoType Scenario = "no_type"
	// FlowUnfixable: the classifier marked the failing node unfixable.
	FlowUnfixable Scenario = "unfixable"
	// FlowMaintenance: the trigger arrives in maintenance mode.
	FlowMaintenance Scenario = "maintenance"
)

// FlowScenarios is every scenario a flow runs through, in run order.
func FlowScenarios() []Scenario {
	return []Scenario{FlowTwoTypesOneService, FlowTwoServices, FlowCrossClusterFails, FlowSharedUpstreamDeclines,
		FlowCrossServiceDeclines, FlowNoType, FlowUnfixable, FlowMaintenance}
}

// RunFlowScenarios runs the named flow through every flow scenario, each in a
// world of its own built from the flow fixture that plays it (as Mutate changed
// it, when the suite has one), and returns one result per scenario, in
// scenario order, with the flow as the result's type.
// A scenario with no fixture, or with several, fails. A fixture for a scenario
// that is not a flow scenario fails too, with one result of its own. Every
// cluster takes its fixture's passing answers, and every verification run
// passes. In the scenario whose tries all fail, every try takes them again.
func (s Suite) RunFlowScenarios(ctx context.Context, flow string) []Result {
	byScenario := map[Scenario][]Fixture{}
	for _, f := range s.Fixtures {
		if f.Flow == flow {
			byScenario[f.Scenario] = append(byScenario[f.Scenario], f)
		}
	}
	_, loaded := s.Files.Flows[flow]
	var out []Result
	for _, sc := range FlowScenarios() {
		fixtures := byScenario[sc]
		var requests []prompt.ProposeRequest
		var err error
		switch {
		case !loaded:
			err = fmt.Errorf("flow %s is not loaded", flow)
		case len(fixtures) == 0:
			err = fmt.Errorf("flow %s has no fixture for scenario %s", flow, sc)
		case len(fixtures) > 1:
			err = fmt.Errorf("flow %s has %d fixtures for scenario %s (%s), and a scenario has one", flow, len(fixtures), sc, fixtureFiles(fixtures))
		default:
			requests, err = s.runFlow(ctx, s.mutated(fixtures[0]), sc)
		}
		out = append(out, Result{Type: flow, Scenario: sc, Err: err, Requests: requests})
	}
	var unknown []Scenario
	for sc := range byScenario {
		if !slices.Contains(FlowScenarios(), sc) {
			unknown = append(unknown, sc)
		}
	}
	sort.Slice(unknown, func(i, j int) bool { return unknown[i] < unknown[j] })
	for _, sc := range unknown {
		out = append(out, Result{Type: flow, Scenario: sc, Err: fmt.Errorf("%s plays %s, which is not a flow scenario (%v)",
			fixtureFiles(byScenario[sc]), sc, FlowScenarios())})
	}
	return out
}

// runFlow runs flow scenario sc from its fixture f, checks what the run wrote,
// and returns the model requests the run made.
func (s Suite) runFlow(ctx context.Context, f Fixture, sc Scenario) ([]prompt.ProposeRequest, error) {
	members, err := memberPasses(f)
	if err != nil {
		return nil, err
	}
	script := Script{Answers: f.Answers.Pass, MemberAnswers: members}
	if sc == FlowCrossClusterFails {
		script = script.everyTry(s.Files.Flows[f.Flow].Tries)
	}
	sr, err := s.start(f, script, sc == FlowMaintenance)
	if err != nil {
		return nil, err
	}
	if err := sr.accept(ctx, f.Trigger); err != nil {
		return sr.requests(), err
	}
	if err := sr.finish(ctx); err != nil {
		return sr.requests(), err
	}
	return sr.requests(), sr.expectFlow(s.Files, f, sc)
}

// expectFlow checks what flow scenario sc wrote.
func (sr *scenarioRun) expectFlow(files workflow.Set, f Fixture, sc Scenario) error {
	e := sr.expect(f.Trigger)
	e.ended()
	switch sc {
	case FlowTwoTypesOneService:
		as := e.attempts(proposal.StatusProposed)
		var types []string
		for _, c := range e.clustersOfTry1() {
			if c.Status == remediation.ClusterChecked && !slices.Contains(types, c.TypeName) {
				types = append(types, c.TypeName)
			}
		}
		e.that(len(types) >= 2, "the try's checked clusters are of the types %v, want two", types)
		if len(as) == 1 {
			e.that(len(as[0].Proposal.Edits) >= 2, "the verifying attempt carries %d edits, want one of each cluster", len(as[0].Proposal.Edits))
			e.nodes(as[0], proposal.StatusProposed)
			e.runsPerEditedService(as[0], f.ServiceRepoPaths)
		}
		e.runs(1)
		e.noHandedOver()
	case FlowTwoServices:
		if as := e.attempts(proposal.StatusProposed); len(as) == 1 {
			e.nodes(as[0], proposal.StatusProposed)
			e.runsPerEditedService(as[0], f.ServiceRepoPaths)
		}
		e.runs(2)
		e.noHandedOver()
	case FlowCrossClusterFails:
		flow := files.Flows[f.Flow]
		failed := slices.Repeat([]proposal.Status{proposal.StatusFailed}, flow.Tries)
		as := e.attempts(append(failed, proposal.StatusEscalated)...)
		e.crossClusterFailedEveryTry(flow, as[:min(flow.Tries, len(as))])
		if len(as) == flow.Tries+1 {
			e.handedOver(as[flow.Tries], flow.Tries, codes(model.HandoverReasonUnverifiableEdits, e.trigger.NodeIDs()...))
		}
		e.proposedEvents(0)
		e.runs(0)
	case FlowSharedUpstreamDeclines:
		if c, ok := e.firstOfKind(workflow.ClusterSharedUpstream); ok {
			e.membersRanTheirTypes(files, c)
		}
	case FlowCrossServiceDeclines:
		c, ok := e.firstOfKind(workflow.ClusterCrossService)
		if as := e.attempts(proposal.StatusSkipped); ok && len(as) == 1 {
			e.membersSkipped(c, as[0], model.HandoverReasonDeclined)
		}
	case FlowNoType:
		if as := e.attempts(proposal.StatusSkipped); len(as) == 1 {
			e.nodes(as[0], proposal.StatusSkipped)
			e.handedOverInFirstTry(as[0], codes(model.HandoverReasonNoType, e.trigger.NodeIDs()...))
		}
		clusters := e.clustersOfTry1()
		e.that(len(clusters) > 0, "try 1 has no cluster")
		for _, c := range clusters {
			e.that(c.Status == remediation.ClusterHandedOver && c.TypeName == "", "cluster %d is %s with type %q", c.Cluster, c.Status, c.TypeName)
		}
		e.onlyHandOverRan(files.Flows[f.Flow])
		e.modelCalls(0)
	case FlowUnfixable:
		e.unfixableHandedOver(files.Flows[f.Flow])
	case FlowMaintenance:
		if as := e.attempts(proposal.StatusSkipped); len(as) == 1 {
			e.nodes(as[0], proposal.StatusSkipped)
		}
		e.that(len(sr.w.Tries(sr.id)) == 0, "maintenance mode opened %d tries", len(sr.w.Tries(sr.id)))
		e.modelCalls(0)
		e.noHandedOver()
	}
	return e.err()
}

// onlyHandOverRan expects no step to have run in the remediation but the
// flow's give-up output, which journals one flow-level row of try 1: no
// cluster ran a step, and no cross-cluster check or gate ran.
func (e *expectation) onlyHandOverRan(flow workflow.Flow) {
	output, rows, others := string(flow.OnGiveUp.Action), 0, 0
	for _, row := range e.sr.w.Steps(e.sr.id) {
		if row.Cluster == remediation.FlowCluster && row.Kind == string(step.KindOutput) && row.Step == output {
			e.that(row.Try == firstTry && row.Status == remediation.StepDone, "the %s row is %s in try %d, want done in try %d", output, row.Status, row.Try, firstTry)
			rows++
			continue
		}
		others++
	}
	e.that(others == 0, "a remediation that hands every node over ran %d steps besides the output %s", others, output)
	e.that(rows == 1, "the journal holds %d rows of the output %s, want 1", rows, output)
}

// unfixableHandedOver expects every failing node of the trigger, each marked
// unfixable by the classifier, to have been handed over with the classifier's
// reason and detail in the remediation's first try, with no model call, no
// step but the flow's give-up output, no verification run and one skipped
// attempt.
func (e *expectation) unfixableHandedOver(flow workflow.Flow) {
	want := map[string]model.HandoverReason{}
	for _, n := range e.trigger.Nodes {
		e.that(n.Unfixable != nil, "node %s of the trigger is not marked unfixable, so the fixture does not test the scenario", n.NodeID)
		if n.Unfixable != nil {
			want[n.NodeID] = n.Unfixable.Reason
		}
	}
	if as := e.attempts(proposal.StatusSkipped); len(as) == 1 {
		e.nodes(as[0], proposal.StatusSkipped)
		for _, n := range e.trigger.Nodes {
			if n.Unfixable != nil {
				o := as[0].Proposal.NodeOutcomes[n.NodeID]
				e.that(o.Reason == n.Unfixable.Detail, "node %s ended the attempt with the reason %q, want the trigger's detail %q", n.NodeID, o.Reason, n.Unfixable.Detail)
			}
		}
		e.handedOverInFirstTry(as[0], want)
	}
	clusters := e.clustersOfTry1()
	e.that(len(clusters) == len(e.trigger.Nodes), "try 1 has %d clusters, want one for each of the %d nodes", len(clusters), len(e.trigger.Nodes))
	for _, c := range clusters {
		e.that(c.Status == remediation.ClusterHandedOver, "cluster %d is %s, not handed_over", c.Cluster, c.Status)
	}
	e.onlyHandOverRan(flow)
	e.modelCalls(0)
	e.runs(0)
}

// firstOfKind returns try 1's first cluster of kind k.
func (e *expectation) firstOfKind(k workflow.ClusterKind) (remediation.Cluster, bool) {
	for _, c := range e.clustersOfTry1() {
		if c.Kind == k {
			return c, true
		}
	}
	e.problems = append(e.problems, fmt.Sprintf("try 1 has no %s cluster", k))
	return remediation.Cluster{}, false
}

// everyTry returns the script with the calls it answers made by each of tries
// tries in turn, for a scenario whose every try makes the calls its first try
// makes.
func (sc Script) everyTry(tries int) Script {
	out := sc
	out.Answers = slices.Repeat(sc.Answers, tries)
	if sc.MemberAnswers != nil {
		out.MemberAnswers = make(map[string][]ports.ProposeResult, len(sc.MemberAnswers))
		for node, answers := range sc.MemberAnswers {
			out.MemberAnswers[node] = slices.Repeat(answers, tries)
		}
	}
	return out
}

// crossClusterFailedEveryTry expects the failed attempts as, one per try in
// try order, to have failed every node with the finding of that try's
// cross-cluster checks, every try to have failed, the model to have made the
// same calls in every try, and each later try's requests to show the finding
// that failed the try before it.
func (e *expectation) crossClusterFailedEveryTry(flow workflow.Flow, as []Attempt) {
	e.everyTryFailed(flow.Tries)
	findings := make([][]remediation.Finding, len(as))
	for i, a := range as {
		findings[i] = e.crossClusterFindings(flow, i+1)
		e.that(len(findings[i]) > 0, "no cross-cluster check of try %d found anything", i+1)
		e.nodes(a, proposal.StatusFailed)
		reason := findingMessages(findings[i])
		e.that(a.Proposal.Rationale == reason, "attempt %d's reason %q is not the cross-cluster findings %q", a.Proposal.Attempt, a.Proposal.Rationale, reason)
	}

	calls := e.sr.w.ModelCalls()
	if len(calls) == 0 || len(calls)%flow.Tries != 0 {
		e.that(false, "the model was called %d times, which %d tries do not divide evenly", len(calls), flow.Tries)
		return
	}
	perTry := len(calls) / flow.Tries
	for try := 2; try <= min(flow.Tries, len(findings)); try++ {
		for _, call := range calls[(try-1)*perTry : try*perTry] {
			user := call.Request.User
			e.that(strings.Contains(user, "## Earlier tries at this failure"), "try %d's request about %s shows no earlier try", try, call.Target)
			for _, f := range findings[try-2] {
				want := "across clusters: " + f.Code + ": " + f.Message
				e.that(strings.Contains(user, want), "try %d's request about %s does not show the cross-cluster finding %q", try, call.Target, want)
			}
		}
	}
}

// crossClusterFindings is the findings of the first of the flow's
// cross-cluster checks to find any in one try, in the order the flow lists
// them. A flow-level row runs once per try, as round 1 of the flow cluster.
func (e *expectation) crossClusterFindings(flow workflow.Flow, try int) []remediation.Finding {
	rows := e.stepRows(try, remediation.FlowCluster, 1)
	for _, ref := range flow.AcrossClusters {
		if row, ok := rows[string(ref.Name)]; ok && len(row.Findings) > 0 {
			return row.Findings
		}
	}
	return nil
}

// findingMessages is the messages of findings joined by "; ", the reason a
// try's attempt carries.
func findingMessages(findings []remediation.Finding) string {
	messages := make([]string, 0, len(findings))
	for _, f := range findings {
		messages = append(messages, f.Message)
	}
	return strings.Join(messages, "; ")
}
