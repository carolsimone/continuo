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

// roundsSpentPrefix starts the outcome of the members of a cluster whose last
// pre-flight round refused its draft.
const roundsSpentPrefix = "pre-flight rounds spent: "

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
	got := len(e.sr.w.ModelCalls())
	e.that(got == n, "the model was called %d times, want %d", got, n)
}

// callsAbout is every call that reached the model about the node, in call
// order.
func (e *expectation) callsAbout(node string) []ModelCall {
	var out []ModelCall
	for _, c := range e.sr.w.ModelCalls() {
		if c.Target == node {
			out = append(out, c)
		}
	}
	return out
}

// clusterCalls expects n calls to have reached the model about cluster c's
// target.
func (e *expectation) clusterCalls(c remediation.Cluster, n int) {
	got := len(e.callsAbout(c.TargetNodeID))
	e.that(got == n, "the model was called %d times about cluster %d's target %s, want %d", got, c.Cluster, c.TargetNodeID, n)
}

// distinctModelRequests expects no request to have reached the model twice:
// a step run again after a lost commit is answered by the response cache.
func (e *expectation) distinctModelRequests() {
	calls := e.sr.w.ModelCalls()
	e.that(len(calls) > 0, "the model was never called")
	for i := range calls {
		for j := range i {
			e.that(!reflect.DeepEqual(calls[i].Request, calls[j].Request), "model call %d repeats call %d", i+1, j+1)
		}
	}
}

// proposed expects one attempt, proposed for every failing node, with one
// verification run per service it edits and one remediation.proposed event.
func (e *expectation) proposed(f Fixture) {
	if as := e.attempts(proposal.StatusProposed); len(as) == 1 {
		e.nodes(as[0], proposal.StatusProposed)
		e.runsPerEditedService(as[0], f.ServiceRepoPaths)
	}
	e.proposedEvents(1)
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
// back, each member's own cluster to have taken a single-cluster type and
// started fresh: its context steps run in its round 1, its draft passed in
// round 1, and the model was called about it once for each of the member's
// scripted answers. The attempt is proposed for every failing node.
func (e *expectation) membersRanTheirTypes(files workflow.Set, c remediation.Cluster) {
	e.fellBackToMembers(c)
	for _, m := range e.clustersOfTry1() {
		if m.Cluster <= c.Cluster || !slices.Contains(c.Members, m.TargetNodeID) {
			continue
		}
		typ, known := files.Types[m.TypeName]
		e.that(known && typ.Match.Cluster == workflow.ClusterSingle, "member %s fell back to type %q", m.TargetNodeID, m.TypeName)
		if !known {
			continue
		}
		e.that(e.reachedModel(m, typ), "member %s's own type %q never reached the model", m.TargetNodeID, m.TypeName)
		e.that(m.Round == 1, "member %s's own cluster %d ended in round %d, want 1", m.TargetNodeID, m.Cluster, m.Round)
		e.contextOnlyInRound1(m, typ)
		e.clusterCalls(m, len(e.sr.script.MemberAnswers[m.TargetNodeID]))
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

// stepRows is the journal rows of one round of one cluster of one try, by
// step name. The journal holds one row per try, cluster, round and step, so
// each name has one row whatever order the store lists rows in; a second row
// under one name is recorded as a problem rather than picked by that order.
func (e *expectation) stepRows(try, cluster, round int) map[string]remediation.StepRun {
	out := map[string]remediation.StepRun{}
	for _, s := range e.sr.w.Steps(e.sr.id) {
		if s.Try != try || s.Cluster != cluster || s.Round != round {
			continue
		}
		if _, dup := out[s.Step]; dup {
			e.problems = append(e.problems, fmt.Sprintf("try %d cluster %d round %d has two rows of step %s", try, cluster, round, s.Step))
		}
		out[s.Step] = s
	}
	return out
}

// stepRow is the journal row of one step in one round of one cluster of one
// try.
func (e *expectation) stepRow(try, cluster, round int, step string) (remediation.StepRun, bool) {
	row, ok := e.stepRows(try, cluster, round)[step]
	return row, ok
}

// reachedModel reports whether the first round of cluster c's prompt step of
// type typ ran.
func (e *expectation) reachedModel(c remediation.Cluster, typ workflow.Type) bool {
	row, ok := e.stepRow(1, c.Cluster, 1, string(typ.Agent.Prompt.Name))
	return ok && row.Status == remediation.StepDone
}

// contextOnlyInRound1 expects every context step of typ to have run in
// cluster c's round 1 and in no later round.
func (e *expectation) contextOnlyInRound1(c remediation.Cluster, typ workflow.Type) {
	for _, ref := range typ.Context {
		row, ok := e.stepRow(1, c.Cluster, 1, string(ref.Name))
		e.that(ok && row.Status == remediation.StepDone, "cluster %d's context step %s did not run in round 1", c.Cluster, ref.Name)
		for round := 2; round <= c.Round; round++ {
			_, again := e.stepRow(1, c.Cluster, round, string(ref.Name))
			e.that(!again, "cluster %d's context step %s ran again in round %d", c.Cluster, ref.Name, round)
		}
	}
}

// passedInRound expects cluster c's draft to have passed pre-flight in round
// pass after every earlier round's pre-flight refused its draft.
func (e *expectation) passedInRound(c remediation.Cluster, typ workflow.Type, pass int) {
	e.that(c.Status == remediation.ClusterChecked, "cluster %d is %s, not checked", c.Cluster, c.Status)
	e.that(c.Round == pass, "cluster %d ended in round %d, want %d", c.Cluster, c.Round, pass)
	for round := 1; round <= pass; round++ {
		prompt, ok := e.stepRow(1, c.Cluster, round, string(typ.Agent.Prompt.Name))
		e.that(ok && prompt.Status == remediation.StepDone, "cluster %d's prompt step did not run in round %d", c.Cluster, round)
		pre, ok := e.stepRow(1, c.Cluster, round, remediation.StepPreflight)
		e.that(ok, "cluster %d has no pre-flight pass in round %d", c.Cluster, round)
		refused := len(pre.Findings) > 0
		e.that(refused == (round < pass), "cluster %d's round %d pre-flight refused its draft: %t, want %t (%v)",
			c.Cluster, round, refused, round < pass, pre.Findings)
	}
	e.contextOnlyInRound1(c, typ)
}

// roundsSpent expects pre-flight to have refused cluster c's draft in every
// round typ allows, its prompt step to have run once per round, and the model
// to have been called about its target exactly calls times.
func (e *expectation) roundsSpent(c remediation.Cluster, typ workflow.Type, calls int) {
	rounds := typ.Agent.Rounds
	e.that(c.Round == rounds, "cluster %d ended in round %d, want %d", c.Cluster, c.Round, rounds)
	for round := 1; round <= rounds; round++ {
		prompt, ok := e.stepRow(1, c.Cluster, round, string(typ.Agent.Prompt.Name))
		e.that(ok && prompt.Status == remediation.StepDone, "cluster %d's prompt step did not run in round %d", c.Cluster, round)
		pre, ok := e.stepRow(1, c.Cluster, round, remediation.StepPreflight)
		e.that(ok && len(pre.Findings) > 0, "cluster %d's round %d pre-flight refused nothing", c.Cluster, round)
	}
	_, beyond := e.stepRow(1, c.Cluster, rounds+1, string(typ.Agent.Prompt.Name))
	e.that(!beyond, "cluster %d's prompt step ran in round %d, past its %d rounds", c.Cluster, rounds+1, rounds)
	e.contextOnlyInRound1(c, typ)
	e.clusterCalls(c, calls)
}

// nextTryShownGateErrors expects the first request of the second try about
// cluster c's target, the call after a try's calls, to show the earlier tries
// with a line saying why the gate failed that names every gate error the
// fixture scripts.
func (e *expectation) nextTryShownGateErrors(c remediation.Cluster, tryCalls int, gateErrors map[string]string) {
	calls := e.callsAbout(c.TargetNodeID)
	if len(calls) <= tryCalls {
		e.that(false, "the model was called %d times about %s, so the second try made no call", len(calls), c.TargetNodeID)
		return
	}
	user := calls[tryCalls].Request.User
	e.that(strings.Contains(user, "## Earlier tries at this failure"), "the second try's first request shows no earlier try")
	var gateLine string
	for line := range strings.Lines(user) {
		if strings.HasPrefix(line, "Why the gate failed: ") {
			gateLine = line
		}
	}
	e.that(gateLine != "", "the second try's first request has no line saying why the gate failed")
	for _, node := range sortedKeys(gateErrors) {
		e.that(strings.Contains(gateLine, gateErrors[node]), "the second try's gate line %q does not name %s's error %q",
			strings.TrimSpace(gateLine), node, gateErrors[node])
	}
}

// firstFinding is the first finding of cluster c's pre-flight checks in the
// cluster's last round, in the order typ lists them.
func (e *expectation) firstFinding(c remediation.Cluster, typ workflow.Type) (remediation.Finding, bool) {
	rows := e.stepRows(1, c.Cluster, c.Round)
	for _, ref := range typ.Preflight {
		if row, ok := rows[string(ref.Name)]; ok && len(row.Findings) > 0 {
			return row.Findings[0], true
		}
	}
	return remediation.Finding{}, false
}

// singleHandedOver expects the single cluster c handed over with no
// verification run: when roundsSpent, by its last pre-flight round, which
// refused its draft, failed with that round's first finding; otherwise by a
// context step that declined, skipped with no model call.
func (e *expectation) singleHandedOver(c remediation.Cluster, typ workflow.Type, roundsSpent bool) {
	e.that(c.Status == remediation.ClusterHandedOver, "cluster %d is %s, not handed_over", c.Cluster, c.Status)
	want := proposal.StatusFailed
	if roundsSpent {
		finding, ok := e.firstFinding(c, typ)
		e.that(ok, "pre-flight refused nothing in cluster %d", c.Cluster)
		reason := roundsSpentPrefix + finding.Message
		e.that(c.Outcome != nil && c.Outcome.Reason == reason, "cluster %d ended with %+v, want the reason %q", c.Cluster, c.Outcome, reason)
	} else {
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
