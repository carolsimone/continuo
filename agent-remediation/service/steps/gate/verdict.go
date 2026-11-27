package gate

import (
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// The finding codes the gate reports.
const (
	codeReleaseCheckFailed   = "release_check_failed"
	codeReleaseCheckTimedOut = "release_check_timed_out"
)

// timedOutError is the reason recorded on a try whose release check never
// reached a verdict inside the budget.
const timedOutError = "release check timed out"

// read is what one poll learned about the checks: every check of the try in
// submission order, of which the first read have the phase and error each last
// reported (the poll stops at the first status it cannot read), and the
// per-node errors of the failed ones.
type read struct {
	checks     []fix.ReleaseCheck
	read       int
	nodeErrors map[string]string
}

// decide turns what a poll read into the gate's verdict.
//
// The try is one fix over one set of edits, so the statuses are combined
// rather than acted on individually: one failure fails it (the edits cannot be
// split, and the errors of every failed check together are the evidence the next
// try reads), otherwise every check must have reached a terminal phase before the
// try passes.
//
// A status that cannot be read is deliberately not a failure: the gateway
// reports a release-controller that is briefly unreachable and a check id it will
// never know with the same error, so counting either as a failed try would let
// one outage burn the try budget on fixes that were never judged. The read is
// retried at the next poll, and the wait is still bounded because the same
// timeout that ends a check which never reaches a verdict also ends one whose
// status never becomes readable, counted from when the try was submitted.
//
// The budget of a running check counts from when that check itself left the
// release queue. Checks share one global queue and go one after another, so
// measuring from the try's start would fail a try whose check never ran, and
// sharing one budget across the try's checks would grow it with the number of
// projects edited. A check still queued has not started, so it has spent nothing.
func (g *ReleaseCheck) decide(in step.GateInput, got read) step.GateVerdict {
	name := string(nameReleaseCheck)
	now := g.clock.Now()
	seen := got.checks[:got.read]
	phases := observedPhases(seen)
	ids := checkIDs(got.checks)

	if got.read < len(got.checks) {
		// The budget of an unreadable check counts from when the try was submitted:
		// with no status to read, nothing else measures it.
		if now.Sub(in.SubmittedAt) >= g.timeout {
			return timedOut(ids, seen, phases)
		}
		return waiting(name, ids, seen, phases)
	}

	anyFailed, allTerminal := false, true
	var runningSince []time.Time
	for _, r := range seen {
		switch r.Phase {
		case fix.PhaseFailed:
			anyFailed = true
		case fix.PhasePassed:
		default:
			allTerminal = false
			if r.ActivatedAt != nil {
				runningSince = append(runningSince, *r.ActivatedAt)
			}
		}
	}
	switch {
	case anyFailed:
		verifyErr := verifyError(draftedNodes(in.Drafts), got.nodeErrors, got.checks)
		return step.GateVerdict{
			Result: remediation.CheckResult{
				Check:    name,
				Findings: failureFindings(got.nodeErrors, verifyErr),
				CheckIDs: ids,
			},
			ReleaseChecks: seen,
			VerifyError:   verifyErr,
			Phases:        phases,
		}
	case allTerminal:
		return step.GateVerdict{
			Result:        remediation.CheckResult{Check: name, CheckIDs: ids},
			ReleaseChecks: seen,
			Phases:        phases,
		}
	}
	for _, since := range runningSince {
		if now.Sub(since) >= g.timeout {
			return timedOut(ids, seen, phases)
		}
	}
	return waiting(name, ids, seen, phases)
}

func timedOut(ids []string, seen, phases []fix.ReleaseCheck) step.GateVerdict {
	return step.GateVerdict{
		Result: remediation.CheckResult{
			Check:    string(nameReleaseCheck),
			Findings: []remediation.Finding{{Code: codeReleaseCheckTimedOut, Message: timedOutError}},
			CheckIDs: ids,
		},
		ReleaseChecks: seen,
		VerifyError:   timedOutError,
		Phases:        phases,
	}
}

func waiting(name string, ids []string, seen, phases []fix.ReleaseCheck) step.GateVerdict {
	return step.GateVerdict{
		Result:        remediation.CheckResult{Check: name, Waiting: true, CheckIDs: ids},
		ReleaseChecks: seen,
		Phases:        phases,
	}
}

// observedPhases are the checks this poll saw queued or running. A terminal
// phase is written with the outcome of the try instead.
func observedPhases(checks []fix.ReleaseCheck) []fix.ReleaseCheck {
	var out []fix.ReleaseCheck
	for _, r := range checks {
		if r.Phase != fix.PhasePassed && r.Phase != fix.PhaseFailed {
			out = append(out, r)
		}
	}
	return out
}

func checkIDs(checks []fix.ReleaseCheck) []string {
	ids := make([]string, len(checks))
	for i, r := range checks {
		ids[i] = r.CheckID
	}
	return ids
}

// failureFindings is one release_check_failed finding per node a failed check
// reports an error for, in node order. A failure that names no node is one
// finding carrying the reason, so the result never passes.
func failureFindings(nodeErrors map[string]string, verifyErr string) []remediation.Finding {
	if len(nodeErrors) == 0 {
		return []remediation.Finding{{Code: codeReleaseCheckFailed, Message: verifyErr}}
	}
	nodes := sortedNodes(nodeErrors)
	findings := make([]remediation.Finding, len(nodes))
	for i, n := range nodes {
		findings[i] = remediation.Finding{Code: codeReleaseCheckFailed, Message: n + ": " + nodeErrors[n]}
	}
	return findings
}

// draftedNodes is the try's own nodes: every member of a cluster whose draft
// the gate judged, sorted. A node handed over (unfixable, or settled by an
// earlier try) has no draft, so it is never one of them.
func draftedNodes(drafts []step.ClusterDraft) []string {
	var nodes []string
	for _, d := range drafts {
		nodes = append(nodes, d.Cluster.Members...)
	}
	sort.Strings(nodes)
	return slices.Compact(nodes)
}

// verifyError is the text recorded on a rejected try, and the evidence the next
// try is shown. The errors of the try's own nodes (own) come first, named and
// in a stable order; when every one of them passed and other nodes did not (a
// fix that broke something downstream of itself, or a handed-over node that
// still fails) every failing node is named instead, so the reason describes
// what actually failed. A failure that names no node records the failed check
// and its own reason and detail (fix.FailedCheckReason), so the try never
// ends with a blank reason and the next try is told what the check reported.
func verifyError(own []string, nodeErrors map[string]string, checks []fix.ReleaseCheck) string {
	if named := namedErrors(nodeErrors, stillFailing(own, nodeErrors)); named != "" {
		return named
	}
	if all := namedErrors(nodeErrors, sortedNodes(nodeErrors)); all != "" {
		return all
	}
	return fix.FailedCheckReason(checks)
}

// stillFailing is the nodes of own that the failure still reports an error
// for, in the order own gives. A node the failure names that no draft of the
// try addresses is not returned: it is not this try's to re-fix.
func stillFailing(own []string, nodeErrors map[string]string) []string {
	still := make([]string, 0, len(own))
	for _, id := range own {
		if nodeErrors[id] != "" {
			still = append(still, id)
		}
	}
	return still
}

// namedErrors renders the errors of the given nodes as "node: message" joined by
// "; ", in the order the nodes are given. It is empty when no node is given.
func namedErrors(nodeErrors map[string]string, nodes []string) string {
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		parts = append(parts, node+": "+nodeErrors[node])
	}
	return strings.Join(parts, "; ")
}

// sortedNodes is every node the failure reports an error for, in a stable
// order, so the recorded reason does not depend on map iteration order.
func sortedNodes(nodeErrors map[string]string) []string {
	nodes := make([]string, 0, len(nodeErrors))
	for node := range nodeErrors {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

// applyStatus records one status read onto a check.
func applyStatus(r fix.ReleaseCheck, st ports.ReleaseCheckStatus) fix.ReleaseCheck {
	r.Phase = st.Phase
	if !st.ActivatedAt.IsZero() {
		at := st.ActivatedAt
		r.ActivatedAt = &at
	}
	if st.Phase == fix.PhaseFailed {
		r.Error = namedErrors(st.NodeErrors, sortedNodes(st.NodeErrors))
		if r.Error == "" {
			r.Error = st.CheckError
		}
	}
	return r
}
