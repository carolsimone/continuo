package conformance

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/pkg/streams"
)

// codeTargetNotShown is the finding of a draft that edits a file the model was
// not shown. Pre-flight hands its members over skipped rather than failed.
const codeTargetNotShown = "target_not_shown"

// expectation collects every way a run differs from what its scenario
// expects.
type expectation struct {
	sr       *scenarioRun
	trigger  remediation.Trigger
	problems []string
}

func (sr *scenarioRun) expect(t remediation.Trigger) *expectation {
	return &expectation{sr: sr, trigger: t}
}

// that records the problem when ok is false.
func (e *expectation) that(ok bool, format string, args ...any) {
	if !ok {
		e.problems = append(e.problems, fmt.Sprintf(format, args...))
	}
}

// err is every problem recorded, or nil.
func (e *expectation) err() error {
	if len(e.problems) == 0 {
		return nil
	}
	return errors.New(strings.Join(e.problems, "; "))
}

// ended expects the remediation done.
func (e *expectation) ended() {
	for _, r := range e.sr.w.Remediations() {
		if r.ID == e.sr.id {
			e.that(r.Status == remediation.StatusDone, "the remediation is %s, not done", r.Status)
		}
	}
}

// attempts expects the release's attempts, in attempt order, to have these
// statuses, and returns them.
func (e *expectation) attempts(want ...proposal.Status) []Attempt {
	got := e.sr.w.Attempts()
	statuses := make([]proposal.Status, 0, len(got))
	numbered := true
	for i, a := range got {
		statuses = append(statuses, a.Proposal.Status)
		numbered = numbered && a.Proposal.Attempt == i+1
		e.that(a.Proposal.RemediationID == e.sr.id.String(), "attempt %d carries remediation %q", a.Proposal.Attempt, a.Proposal.RemediationID)
	}
	e.that(numbered, "the attempts are numbered %v", attemptNumbers(got))
	e.that(slices.Equal(statuses, want), "the attempts are %v, want %v", statuses, want)
	return got
}

// nodes expects every failing node of the trigger to have ended attempt a
// with status.
func (e *expectation) nodes(a Attempt, status proposal.Status) {
	e.members(a, e.trigger.NodeIDs(), status)
}

// members expects each named node to have ended attempt a with status.
func (e *expectation) members(a Attempt, nodeIDs []string, status proposal.Status) {
	for _, id := range nodeIDs {
		o, ok := a.Proposal.NodeOutcomes[id]
		e.that(ok && o.Status == status, "node %s ended attempt %d as %q (%s), want %q", id, a.Proposal.Attempt, o.Status, o.Reason, status)
	}
}

// runsPerEditedService expects one verification run per service attempt a
// edits, and no other run.
func (e *expectation) runsPerEditedService(a Attempt, serviceRepoPaths map[string]string) {
	edited := map[string]bool{}
	for _, ed := range a.Proposal.Edits {
		service, _, ok := proposal.ServiceForPath(serviceRepoPaths, ed.Path)
		e.that(ok, "attempt %d edits %s, which belongs to no service", a.Proposal.Attempt, ed.Path)
		edited[service] = true
	}
	e.that(len(edited) > 0, "attempt %d has no edit", a.Proposal.Attempt)
	runs := map[string]int{}
	for _, r := range e.sr.w.Runs() {
		runs[r.Service]++
	}
	for _, service := range sortedKeys(edited) {
		e.that(runs[service] == 1, "service %s had %d verification runs, want 1", service, runs[service])
	}
	for _, service := range sortedKeys(runs) {
		e.that(edited[service], "service %s had %d verification runs and no edit", service, runs[service])
	}
}

// runs expects n verification runs.
func (e *expectation) runs(n int) {
	got := e.sr.w.Runs()
	e.that(len(got) == n, "%d verification runs were submitted, want %d", len(got), n)
}

// proposedEvents expects n remediation.proposed events in the outbox.
func (e *expectation) proposedEvents(n int) {
	got := 0
	for _, entry := range e.sr.w.Outbox() {
		if entry.StreamName == streams.RemediationProposedV1 {
			got++
		}
	}
	e.that(got == n, "%d remediation.proposed events were enqueued, want %d", got, n)
}

// modelCalls expects n calls to reach the model.
func (e *expectation) modelCalls(n int) {
	got := len(e.sr.w.ModelRequests())
	e.that(got == n, "the model was called %d times, want %d", got, n)
}

// distinctModelRequests expects no request to have reached the model twice:
// a step run again after a lost commit is answered by the response cache.
func (e *expectation) distinctModelRequests() {
	reqs := e.sr.w.ModelRequests()
	e.that(len(reqs) > 0, "the model was never called")
	for i := range reqs {
		for j := range i {
			e.that(!reflect.DeepEqual(reqs[i], reqs[j]), "model call %d repeats call %d", i+1, j+1)
		}
	}
}

// cluster returns cluster n of try 1.
func (e *expectation) cluster(n int) (remediation.Cluster, bool) {
	for _, c := range e.sr.w.Clusters(e.sr.id) {
		if c.Try == 1 && c.Cluster == n {
			return c, true
		}
	}
	e.problems = append(e.problems, fmt.Sprintf("try 1 has no cluster %d", n))
	return remediation.Cluster{}, false
}

// clustersOfTry1 is every cluster of try 1.
func (e *expectation) clustersOfTry1() []remediation.Cluster {
	var out []remediation.Cluster
	for _, c := range e.sr.w.Clusters(e.sr.id) {
		if c.Try == 1 {
			out = append(out, c)
		}
	}
	return out
}

// firstCluster expects try 1's first cluster to be of type name and returns
// it.
func (e *expectation) firstCluster(name string) remediation.Cluster {
	c, ok := e.cluster(1)
	if ok {
		e.that(c.TypeName == name, "try 1's first cluster is of type %q, not %q, so the fixture does not test its type", c.TypeName, name)
	}
	return c
}

// fellBackToMembers expects the upstream cluster c to have fallen back and
// one single cluster of try 1 to stand for each of its members.
func (e *expectation) fellBackToMembers(c remediation.Cluster) {
	e.that(c.Status == remediation.ClusterFellBack, "cluster %d is %s, not fell_back", c.Cluster, c.Status)
	var targets []string
	for _, m := range e.clustersOfTry1() {
		if m.Cluster > c.Cluster && m.Kind == workflow.ClusterSingle && slices.Contains(c.Members, m.TargetNodeID) {
			e.that(slices.Equal(m.Members, []string{m.TargetNodeID}), "member cluster %d has members %v", m.Cluster, m.Members)
			targets = append(targets, m.TargetNodeID)
		}
	}
	sort.Strings(targets)
	members := slices.Sorted(slices.Values(c.Members))
	e.that(slices.Equal(targets, members), "the members %v fell back to single clusters for %v", members, targets)
}

// membersRanTheirTypes expects the shared-upstream cluster c to have fallen
// back, each member's own cluster to have taken a single-cluster type that
// called the model with the member's passing answers, and the attempt to be
// proposed for every failing node.
func (e *expectation) membersRanTheirTypes(files workflow.Set, c remediation.Cluster) {
	e.fellBackToMembers(c)
	for _, m := range e.clustersOfTry1() {
		if m.Cluster <= c.Cluster || !slices.Contains(c.Members, m.TargetNodeID) {
			continue
		}
		typ, known := files.Types[m.TypeName]
		e.that(known && typ.Match.Cluster == workflow.ClusterSingle, "member %s fell back to type %q", m.TargetNodeID, m.TypeName)
		e.that(known && e.reachedModel(m, typ), "member %s's own type %q never reached the model", m.TargetNodeID, m.TypeName)
	}
	if as := e.attempts(proposal.StatusProposed); len(as) == 1 {
		e.nodes(as[0], proposal.StatusProposed)
	}
}

// membersSkipped expects the cross-service cluster c to have fallen back with
// every member skipped in attempt a, and no verification run.
func (e *expectation) membersSkipped(c remediation.Cluster, a Attempt) {
	e.that(c.Status == remediation.ClusterFellBack, "cluster %d is %s, not fell_back", c.Cluster, c.Status)
	e.members(a, c.Members, proposal.StatusSkipped)
	e.runs(0)
}

// stepRows is the journal rows of one cluster of try 1, by step name.
func (e *expectation) stepRows(cluster int) map[string]remediation.StepRun {
	out := map[string]remediation.StepRun{}
	for _, s := range e.sr.w.Steps(e.sr.id) {
		if s.Try == 1 && s.Cluster == cluster {
			out[s.Step] = s
		}
	}
	return out
}

// reachedModel reports whether cluster c's prompt step of type typ ran.
func (e *expectation) reachedModel(c remediation.Cluster, typ workflow.Type) bool {
	row, ok := e.stepRows(c.Cluster)[string(typ.Agent.Prompt.Name)]
	return ok && row.Status == remediation.StepDone
}

// upstreamReachedModel expects the upstream cluster c's prompt step to have
// run in scenario sc when want is true, and not to have run otherwise.
func (e *expectation) upstreamReachedModel(c remediation.Cluster, typ workflow.Type, want bool, sc Scenario) {
	reached := e.reachedModel(c, typ)
	e.that(reached == want, "in scenario %s the upstream cluster reached the model: %t, want %t", sc, reached, want)
}

// firstFinding is the first finding of cluster c's pre-flight checks, in the
// order typ lists them.
func (e *expectation) firstFinding(c remediation.Cluster, typ workflow.Type) (remediation.Finding, bool) {
	rows := e.stepRows(c.Cluster)
	for _, ref := range typ.Preflight {
		if row, ok := rows[string(ref.Name)]; ok && len(row.Findings) > 0 {
			return row.Findings[0], true
		}
	}
	return remediation.Finding{}, false
}

// singleHandedOver expects the single cluster c handed over in scenario sc,
// with no verification run: by its pre-flight pass, the attempt settled
// skipped when the first finding is a target the model was not shown and
// failed otherwise; by a context step that declined, skipped with no model
// call; by the model, failed.
func (e *expectation) singleHandedOver(c remediation.Cluster, typ workflow.Type, sc Scenario) {
	e.that(c.Status == remediation.ClusterHandedOver, "cluster %d is %s, not handed_over", c.Cluster, c.Status)
	want := proposal.StatusFailed
	switch sc {
	case PreflightFails:
		finding, ok := e.firstFinding(c, typ)
		e.that(ok, "pre-flight refused nothing in cluster %d", c.Cluster)
		if finding.Code == codeTargetNotShown {
			want = proposal.StatusSkipped
		}
	case DeclinesBeforeModel:
		want = proposal.StatusSkipped
		e.modelCalls(0)
	}
	if as := e.attempts(want); len(as) == 1 {
		e.nodes(as[0], want)
	}
	e.runs(0)
}

// verifyError expects attempt a failed by its verification, with every gate
// error the fixture scripts in its verify error.
func (e *expectation) verifyError(a Attempt, gateErrors map[string]string) {
	e.that(a.VerifyError != "", "attempt %d carries no verify error", a.Proposal.Attempt)
	for _, node := range sortedKeys(gateErrors) {
		msg := gateErrors[node]
		e.that(strings.Contains(a.VerifyError, msg), "attempt %d's verify error %q does not name %s's error %q",
			a.Proposal.Attempt, a.VerifyError, node, msg)
	}
}

// sortedKeys is the keys of m, sorted, so every message that names them names
// them in one order.
func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

func attemptNumbers(as []Attempt) []int {
	out := make([]int, 0, len(as))
	for _, a := range as {
		out = append(out, a.Proposal.Attempt)
	}
	return out
}
