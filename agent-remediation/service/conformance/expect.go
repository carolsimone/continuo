package conformance

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/flow"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

// firstTry is the try a remediation ends in when every cluster of its first
// try is handed over before any draft is made: nothing is left to try again.
const firstTry = 1

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

// fixRows expects the release's fix rows, in the order they were written, to
// have these statuses, and returns them. The tries are numbered from 1, and a
// hand-over row, which only a handed_over row is, comes last as
// fix.HandOverTry.
func (e *expectation) fixRows(want ...fix.Status) []FixRow {
	got := e.sr.w.FixRows()
	statuses := make([]fix.Status, 0, len(got))
	numbered := true
	for i, a := range got {
		statuses = append(statuses, a.Fix.Status)
		if a.Fix.Status == fix.StatusHandedOver {
			numbered = numbered && a.Fix.Try == fix.HandOverTry && i == len(got)-1
		} else {
			numbered = numbered && a.Fix.Try == i+1
		}
		e.that(a.Fix.RemediationID == e.sr.id.String(), "try %d carries remediation %q", a.Fix.Try, a.Fix.RemediationID)
	}
	e.that(numbered, "the tries are numbered %v", tryNumbers(got))
	e.that(slices.Equal(statuses, want), "the tries are %v, want %v", statuses, want)
	return got
}

// nodes expects every failing node of the trigger to have ended try a
// with status.
func (e *expectation) nodes(a FixRow, status fix.Status) {
	e.members(a, e.trigger.NodeIDs(), status)
}

// members expects each named node to have ended try a with status.
func (e *expectation) members(a FixRow, nodeIDs []string, status fix.Status) {
	for _, id := range nodeIDs {
		o, ok := a.Fix.NodeOutcomes[id]
		e.that(ok && o.Status == status, "node %s ended try %d as %q (%s), want %q", id, a.Fix.Try, o.Status, o.Reason, status)
	}
}

// checksPerEditedProject expects one release check per project try a
// edits, and no other run.
func (e *expectation) checksPerEditedProject(a FixRow, projectRepoPaths map[string]string) {
	edited := map[string]bool{}
	for _, ed := range a.Fix.Edits {
		project, _, ok := fix.ProjectForPath(projectRepoPaths, ed.Path)
		e.that(ok, "try %d edits %s, which belongs to no project", a.Fix.Try, ed.Path)
		edited[project] = true
	}
	e.that(len(edited) > 0, "try %d has no edit", a.Fix.Try)
	runs := map[string]int{}
	for _, r := range e.sr.w.Checks() {
		runs[r.Project]++
	}
	for _, project := range sortedKeys(edited) {
		e.that(runs[project] == 1, "project %s had %d release checks, want 1", project, runs[project])
	}
	for _, project := range sortedKeys(runs) {
		e.that(edited[project], "project %s had %d release checks and no edit", project, runs[project])
	}
}

// runs expects n release checks.
func (e *expectation) checks(n int) {
	got := e.sr.w.Checks()
	e.that(len(got) == n, "%d release checks were submitted, want %d", len(got), n)
}

// proposedEvents expects n fix.proposed events in the outbox.
func (e *expectation) proposedEvents(n int) {
	got := 0
	for _, entry := range e.sr.w.Outbox() {
		if entry.StreamName == streams.FixProposedV1 {
			got++
		}
	}
	e.that(got == n, "%d fix.proposed events were enqueued, want %d", got, n)
}

// handedOverEvent is one remediation.handed_over event of the outbox, with the
// tenant its row is stored under.
type handedOverEvent struct {
	tenant  string
	payload events.RemediationHandedOver
}

// handedOverEvents is every remediation.handed_over event in the outbox, as
// the publisher reads a row: its tenant unwrapped, its payload decoded. A row
// that cannot be read is recorded as a problem.
func (e *expectation) handedOverEvents() []handedOverEvent {
	var out []handedOverEvent
	for _, entry := range e.sr.w.Outbox() {
		if entry.StreamName != streams.RemediationHandedOverV1 {
			continue
		}
		tenant, raw, err := outbox.UnwrapTenant(entry.Payload)
		if err != nil {
			e.that(false, "a remediation.handed_over row does not carry its tenant: %v", err)
			continue
		}
		var p events.RemediationHandedOver
		if err := json.Unmarshal(raw, &p); err != nil {
			e.that(false, "a remediation.handed_over row does not hold a hand-over payload: %v", err)
			continue
		}
		out = append(out, handedOverEvent{tenant: tenant, payload: p})
	}
	return out
}

// noHandedOver expects no remediation.handed_over event: every node ended
// in a proposed fix, and none is left to a human.
func (e *expectation) noHandedOver() {
	got := e.handedOverEvents()
	e.that(len(got) == 0, "%d remediation.handed_over events were enqueued, want none", len(got))
}

// codes is the want of handedOver: every named node with the code.
func codes(code model.HandoverReason, nodeIDs ...string) map[string]model.HandoverReason {
	out := make(map[string]model.HandoverReason, len(nodeIDs))
	for _, id := range nodeIDs {
		out[id] = code
	}
	return out
}

// handedOver expects the nodes of want to have ended try a, the
// remediation's last, with the code want names for them, and the remediation
// to have enqueued exactly one remediation.handed_over event, under the
// remediation's tenant, that carries the remediation's identity and the tries
// it made and names exactly the nodes of want, in node id order, each with its
// code and the message its outcome on try a carries, which is never empty.
func (e *expectation) handedOver(a FixRow, tries int, want map[string]model.HandoverReason) {
	for _, id := range sortedKeys(want) {
		o := a.Fix.NodeOutcomes[id]
		e.that(o.Code == want[id], "node %s ended try %d with the code %q (%s), want %q", id, a.Fix.Try, o.Code, o.Reason, want[id])
	}
	got := e.handedOverEvents()
	if len(got) != 1 {
		e.that(false, "%d remediation.handed_over events were enqueued, want 1", len(got))
		return
	}
	var rem remediation.Remediation
	for _, r := range e.sr.w.Remediations() {
		if r.ID == e.sr.id {
			rem = r
		}
	}
	p := got[0].payload
	e.that(got[0].tenant == rem.TenantID, "the hand-over event is stored under the tenant %q, want %q", got[0].tenant, rem.TenantID)
	e.that(p.RemediationID == e.sr.id.String(), "the hand-over event names the remediation %q, want %q", p.RemediationID, e.sr.id)
	e.that(p.ReleaseID == e.trigger.ReleaseID, "the hand-over event names the release %q, want %q", p.ReleaseID, e.trigger.ReleaseID)
	e.that(p.Round == e.trigger.Round, "the hand-over event names the round %d, want %d", p.Round, e.trigger.Round)
	e.that(p.Flow == rem.Flow, "the hand-over event names the flow %q, want %q", p.Flow, rem.Flow)
	e.that(p.Tries == tries, "the hand-over event counts %d tries, want %d", p.Tries, tries)

	named := make([]string, 0, len(p.Nodes))
	for _, n := range p.Nodes {
		named = append(named, n.NodeID)
		w, ok := want[n.NodeID]
		e.that(ok, "the hand-over event names node %s (%s), which the scenario does not hand over", n.NodeID, n.Reason)
		e.that(ok && n.Reason == string(w), "the hand-over event gives node %s the reason %q, want %q", n.NodeID, n.Reason, w)
		e.that(strings.TrimSpace(n.Message) != "", "the hand-over event gives node %s (%s) no message", n.NodeID, n.Reason)
		e.that(n.Message == a.Fix.NodeOutcomes[n.NodeID].Reason, "the hand-over event's message for node %s is %q, but its outcome says %q",
			n.NodeID, n.Message, a.Fix.NodeOutcomes[n.NodeID].Reason)
	}
	e.that(slices.Equal(named, sortedKeys(want)), "the hand-over event names the nodes %v, want %v", named, sortedKeys(want))
}

// noLaterTry expects the remediation to have made no try but its first: a node
// handed over before it is drafted for is not tried again.
func (e *expectation) noLaterTry() {
	tries := e.sr.w.Tries(e.sr.id)
	e.that(len(tries) == firstTry, "the remediation made %d tries, want %d", len(tries), firstTry)
	for _, c := range e.sr.w.Clusters(e.sr.id) {
		e.that(c.Try == firstTry, "cluster %d of try %d exists, but no try follows the first", c.Cluster, c.Try)
	}
}

// handedOverInFirstTry expects the remediation to have ended in its first try
// with the nodes of want handed over in try a (handedOver).
func (e *expectation) handedOverInFirstTry(a FixRow, want map[string]model.HandoverReason) {
	e.noLaterTry()
	e.handedOver(a, firstTry, want)
}

// everyTryFailed expects the remediation to have made n tries, each failed.
func (e *expectation) everyTryFailed(n int) {
	tries := e.sr.w.Tries(e.sr.id)
	e.that(len(tries) == n, "the remediation made %d tries, want %d", len(tries), n)
	for i, t := range tries {
		e.that(t.Try == i+1, "the remediation's try number %d is numbered %d", i+1, t.Try)
		e.that(t.Status == remediation.TryFailed, "try %d is %s, not failed", t.Try, t.Status)
	}
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

// proposed expects one try, proposed for every failing node, with one
// release check per project it edits and one fix.proposed event.
func (e *expectation) proposed(f Fixture) {
	if as := e.fixRows(fix.StatusProposed); len(as) == 1 {
		e.nodes(as[0], fix.StatusProposed)
		e.checksPerEditedProject(as[0], f.ProjectRepoPaths)
	}
	e.proposedEvents(1)
	e.noHandedOver()
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
		if m.Cluster > c.Cluster && m.Kind == flow.ClusterSingle && slices.Contains(c.Members, m.TargetNodeID) {
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
// started fresh: its context steps run in its draft 1, its draft passed in
// draft 1, and the model was called about it once for each of the member's
// scripted answers. The try is proposed for every failing node.
func (e *expectation) membersRanTheirTypes(files flow.Set, c remediation.Cluster) {
	e.fellBackToMembers(c)
	for _, m := range e.clustersOfTry1() {
		if m.Cluster <= c.Cluster || !slices.Contains(c.Members, m.TargetNodeID) {
			continue
		}
		typ, known := files.Types[m.TypeName]
		e.that(known && typ.Match.Cluster == flow.ClusterSingle, "member %s fell back to type %q", m.TargetNodeID, m.TypeName)
		if !known {
			continue
		}
		e.that(e.reachedModel(m, typ), "member %s's own type %q never reached the model", m.TargetNodeID, m.TypeName)
		e.that(m.Draft == 1, "member %s's own cluster %d ended in draft %d, want 1", m.TargetNodeID, m.Cluster, m.Draft)
		e.contextOnlyInFirstDraft(m, typ)
		e.clusterCalls(m, len(e.sr.script.MemberAnswers[m.TargetNodeID]))
	}
	if as := e.fixRows(fix.StatusProposed); len(as) == 1 {
		e.nodes(as[0], fix.StatusProposed)
	}
	e.noHandedOver()
}

// membersSkipped expects the cross-project cluster c to have fallen back with
// every member skipped in try a and handed over with code, the cause of
// the fall back, in the remediation's first and only try, and no release check
// run. The code is declined when a step declined the upstream fix and
// drafts_spent when pre-flight refused every draft of it.
func (e *expectation) membersSkipped(c remediation.Cluster, a FixRow, code model.HandoverReason) {
	e.that(c.Status == remediation.ClusterFellBack, "cluster %d is %s, not fell_back", c.Cluster, c.Status)
	e.that(c.Outcome != nil && c.Outcome.Code == code, "cluster %d ended with %+v, want the code %q", c.Cluster, c.Outcome, code)
	e.members(a, c.Members, fix.StatusSkipped)
	e.handedOverInFirstTry(a, codes(code, c.Members...))
	e.checks(0)
}

// stepRows is the journal rows of one draft of one cluster of one try, by
// step name. The journal holds one row per try, cluster, draft and step, so
// each name has one row whatever order the store lists rows in; a second row
// under one name is recorded as a problem rather than picked by that order.
func (e *expectation) stepRows(try, cluster, draft int) map[string]remediation.StepRun {
	out := map[string]remediation.StepRun{}
	for _, s := range e.sr.w.Steps(e.sr.id) {
		if s.Try != try || s.Cluster != cluster || s.Draft != draft {
			continue
		}
		if _, dup := out[s.Step]; dup {
			e.problems = append(e.problems, fmt.Sprintf("try %d cluster %d draft %d has two rows of step %s", try, cluster, draft, s.Step))
		}
		out[s.Step] = s
	}
	return out
}

// stepRow is the journal row of one step in one draft of one cluster of one
// try.
func (e *expectation) stepRow(try, cluster, draft int, step string) (remediation.StepRun, bool) {
	row, ok := e.stepRows(try, cluster, draft)[step]
	return row, ok
}

// reachedModel reports whether the first draft of cluster c's prompt step of
// type typ ran.
func (e *expectation) reachedModel(c remediation.Cluster, typ flow.Type) bool {
	row, ok := e.stepRow(1, c.Cluster, 1, string(typ.Agent.Prompt.Name))
	return ok && row.Status == remediation.StepDone
}

// contextOnlyInFirstDraft expects every context step of typ to have run in
// cluster c's draft 1 and in no later draft.
func (e *expectation) contextOnlyInFirstDraft(c remediation.Cluster, typ flow.Type) {
	for _, ref := range typ.Context {
		row, ok := e.stepRow(1, c.Cluster, 1, string(ref.Name))
		e.that(ok && row.Status == remediation.StepDone, "cluster %d's context step %s did not run in draft 1", c.Cluster, ref.Name)
		for draft := 2; draft <= c.Draft; draft++ {
			_, again := e.stepRow(1, c.Cluster, draft, string(ref.Name))
			e.that(!again, "cluster %d's context step %s ran again in draft %d", c.Cluster, ref.Name, draft)
		}
	}
}

// passedInDraft expects cluster c's draft to have passed pre-flight in draft
// pass after every earlier draft's pre-flight refused its draft.
func (e *expectation) passedInDraft(c remediation.Cluster, typ flow.Type, pass int) {
	e.that(c.Status == remediation.ClusterChecked, "cluster %d is %s, not checked", c.Cluster, c.Status)
	e.that(c.Draft == pass, "cluster %d ended in draft %d, want %d", c.Cluster, c.Draft, pass)
	for draft := 1; draft <= pass; draft++ {
		prompt, ok := e.stepRow(1, c.Cluster, draft, string(typ.Agent.Prompt.Name))
		e.that(ok && prompt.Status == remediation.StepDone, "cluster %d's prompt step did not run in draft %d", c.Cluster, draft)
		pre, ok := e.stepRow(1, c.Cluster, draft, remediation.StepPreflight)
		e.that(ok, "cluster %d has no pre-flight pass in draft %d", c.Cluster, draft)
		refused := len(pre.Findings) > 0
		e.that(refused == (draft < pass), "cluster %d's draft %d pre-flight refused its draft: %t, want %t (%v)",
			c.Cluster, draft, refused, draft < pass, pre.Findings)
	}
	e.contextOnlyInFirstDraft(c, typ)
}

// draftsSpent expects pre-flight to have refused cluster c's draft in every
// draft typ allows, its prompt step to have run once per draft, and the model
// to have been called about its target exactly calls times.
func (e *expectation) draftsSpent(c remediation.Cluster, typ flow.Type, calls int) {
	drafts := typ.Agent.Drafts
	e.that(c.Draft == drafts, "cluster %d ended in draft %d, want %d", c.Cluster, c.Draft, drafts)
	for draft := 1; draft <= drafts; draft++ {
		prompt, ok := e.stepRow(1, c.Cluster, draft, string(typ.Agent.Prompt.Name))
		e.that(ok && prompt.Status == remediation.StepDone, "cluster %d's prompt step did not run in draft %d", c.Cluster, draft)
		pre, ok := e.stepRow(1, c.Cluster, draft, remediation.StepPreflight)
		e.that(ok && len(pre.Findings) > 0, "cluster %d's draft %d pre-flight refused nothing", c.Cluster, draft)
	}
	_, beyond := e.stepRow(1, c.Cluster, drafts+1, string(typ.Agent.Prompt.Name))
	e.that(!beyond, "cluster %d's prompt step ran in draft %d, past its %d drafts", c.Cluster, drafts+1, drafts)
	e.contextOnlyInFirstDraft(c, typ)
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
// cluster's last draft, in the order typ lists them.
func (e *expectation) firstFinding(c remediation.Cluster, typ flow.Type) (remediation.Finding, bool) {
	rows := e.stepRows(1, c.Cluster, c.Draft)
	for _, ref := range typ.Preflight {
		if row, ok := rows[string(ref.Name)]; ok && len(row.Findings) > 0 {
			return row.Findings[0], true
		}
	}
	return remediation.Finding{}, false
}

// singleHandedOver expects the single cluster c handed over with no
// release check, in the remediation's first and only try: when draftsSpent,
// by its last draft, which refused its draft, failed with that
// draft's first finding as its reason and the code drafts_spent;
// otherwise by a context step that declined, skipped with the code declined
// and no model call.
func (e *expectation) singleHandedOver(c remediation.Cluster, typ flow.Type, draftsSpent bool) {
	e.that(c.Status == remediation.ClusterHandedOver, "cluster %d is %s, not handed_over", c.Cluster, c.Status)
	want, code := fix.StatusFailed, model.HandoverReasonDraftsSpent
	if draftsSpent {
		finding, ok := e.firstFinding(c, typ)
		e.that(ok, "pre-flight refused nothing in cluster %d", c.Cluster)
		e.that(c.Outcome != nil && c.Outcome.Reason == finding.Message, "cluster %d ended with %+v, want the reason %q", c.Cluster, c.Outcome, finding.Message)
	} else {
		want, code = fix.StatusSkipped, model.HandoverReasonDeclined
		e.modelCalls(0)
	}
	e.that(c.Outcome != nil && c.Outcome.Code == code, "cluster %d ended with %+v, want the code %q", c.Cluster, c.Outcome, code)
	if as := e.fixRows(want); len(as) == 1 {
		e.nodes(as[0], want)
		e.handedOverInFirstTry(as[0], codes(code, e.trigger.NodeIDs()...))
	}
	e.checks(0)
}

// verifyError expects try a failed by its release check, with every gate
// error the fixture scripts in its verify error.
func (e *expectation) verifyError(a FixRow, gateErrors map[string]string) {
	e.that(a.VerifyError != "", "try %d carries no verify error", a.Fix.Try)
	for _, node := range sortedKeys(gateErrors) {
		msg := gateErrors[node]
		e.that(strings.Contains(a.VerifyError, msg), "try %d's verify error %q does not name %s's error %q",
			a.Fix.Try, a.VerifyError, node, msg)
	}
}

// sortedKeys is the keys of m, sorted, so every message that names them names
// them in one order.
func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

func tryNumbers(as []FixRow) []int {
	out := make([]int, 0, len(as))
	for _, a := range as {
		out = append(out, a.Fix.Try)
	}
	return out
}
