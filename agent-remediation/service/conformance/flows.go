package conformance

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

// The scenarios a flow runs through, each played by a flow fixture.
const (
	// FlowTwoTypesOneService: two clusters of two types fix one service.
	FlowTwoTypesOneService Scenario = "two_types_one_service"
	// FlowTwoServices: the try's edits change two services.
	FlowTwoServices Scenario = "two_services"
	// FlowCrossClusterFails: two clusters edit one path.
	FlowCrossClusterFails Scenario = "cross_cluster_fails"
	// FlowSharedUpstreamDeclines: a shared-upstream cluster declines.
	FlowSharedUpstreamDeclines Scenario = "shared_upstream_declines"
	// FlowCrossServiceDeclines: a cross-service cluster declines.
	FlowCrossServiceDeclines Scenario = "cross_service_declines"
	// FlowNoType: a failure no type handles.
	FlowNoType Scenario = "no_type"
	// FlowMaintenance: the trigger arrives in maintenance mode.
	FlowMaintenance Scenario = "maintenance"
)

// FlowScenarios is every scenario a flow runs through, in run order.
func FlowScenarios() []Scenario {
	return []Scenario{FlowTwoTypesOneService, FlowTwoServices, FlowCrossClusterFails, FlowSharedUpstreamDeclines,
		FlowCrossServiceDeclines, FlowNoType, FlowMaintenance}
}

// RunFlowScenarios runs the named flow through every flow scenario, each in a
// world of its own built from the flow fixture that plays it, and returns one
// result per scenario, in scenario order, with the flow as the result's type.
// A scenario with no fixture, or with several, fails. A fixture for a scenario
// that is not a flow scenario fails too, with one result of its own. Every
// cluster takes its fixture's passing answers, and every verification run
// passes.
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
		var err error
		switch {
		case !loaded:
			err = fmt.Errorf("flow %s is not loaded", flow)
		case len(fixtures) == 0:
			err = fmt.Errorf("flow %s has no fixture for scenario %s", flow, sc)
		case len(fixtures) > 1:
			err = fmt.Errorf("flow %s has %d fixtures for scenario %s (%s), and a scenario has one", flow, len(fixtures), sc, fixtureFiles(fixtures))
		default:
			err = s.runFlow(ctx, fixtures[0], sc)
		}
		out = append(out, Result{Type: flow, Scenario: sc, Err: err})
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

// runFlow runs flow scenario sc from its fixture f and checks what the run
// wrote.
func (s Suite) runFlow(ctx context.Context, f Fixture, sc Scenario) error {
	script := Script{Answers: f.Answers.Pass, MemberAnswers: memberPasses(f)}
	sr, err := s.start(f, script, sc == FlowMaintenance)
	if err != nil {
		return err
	}
	if err := sr.accept(ctx, f.Trigger); err != nil {
		return err
	}
	if err := sr.finish(ctx); err != nil {
		return err
	}
	return sr.expectFlow(s.Files, f, sc)
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
	case FlowTwoServices:
		if as := e.attempts(proposal.StatusProposed); len(as) == 1 {
			e.nodes(as[0], proposal.StatusProposed)
			e.runsPerEditedService(as[0], f.ServiceRepoPaths)
		}
		e.runs(2)
	case FlowCrossClusterFails:
		if as := e.attempts(proposal.StatusFailed); len(as) == 1 {
			e.nodes(as[0], proposal.StatusFailed)
			finding, ok := e.crossClusterFinding(files.Flows[f.Flow])
			e.that(ok, "no cross-cluster check found anything")
			e.that(as[0].Proposal.Rationale == finding.Message, "the attempt's reason %q is not the cross-cluster finding %q", as[0].Proposal.Rationale, finding.Message)
		}
		e.runs(0)
	case FlowSharedUpstreamDeclines:
		if c, ok := e.firstOfKind(workflow.ClusterSharedUpstream); ok {
			e.membersRanTheirTypes(files, c)
		}
	case FlowCrossServiceDeclines:
		c, ok := e.firstOfKind(workflow.ClusterCrossService)
		if as := e.attempts(proposal.StatusSkipped); ok && len(as) == 1 {
			e.membersSkipped(c, as[0])
		}
	case FlowNoType:
		if as := e.attempts(proposal.StatusSkipped); len(as) == 1 {
			e.nodes(as[0], proposal.StatusSkipped)
		}
		for _, c := range e.clustersOfTry1() {
			e.that(c.Status == remediation.ClusterHandedOver && c.TypeName == "", "cluster %d is %s with type %q", c.Cluster, c.Status, c.TypeName)
		}
		e.that(len(sr.w.Steps(sr.id)) == 0, "a cluster no type handles ran %d steps", len(sr.w.Steps(sr.id)))
		e.modelCalls(0)
	case FlowMaintenance:
		if as := e.attempts(proposal.StatusSkipped); len(as) == 1 {
			e.nodes(as[0], proposal.StatusSkipped)
		}
		e.that(len(sr.w.Tries(sr.id)) == 0, "maintenance mode opened %d tries", len(sr.w.Tries(sr.id)))
		e.modelCalls(0)
	}
	return e.err()
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

// crossClusterFinding is the first finding of try 1's cross-cluster checks,
// in the order flow lists them.
func (e *expectation) crossClusterFinding(flow workflow.Flow) (remediation.Finding, bool) {
	rows := e.stepRows(0)
	for _, ref := range flow.AcrossClusters {
		if row, ok := rows[string(ref.Name)]; ok && len(row.Findings) > 0 {
			return row.Findings[0], true
		}
	}
	return remediation.Finding{}, false
}
