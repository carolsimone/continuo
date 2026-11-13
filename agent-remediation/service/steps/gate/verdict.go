package gate

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// The finding codes the gate reports.
const (
	codeVerificationFailed   = "verification_failed"
	codeVerificationTimedOut = "verification_timed_out"
)

// timedOutError is the reason recorded on a try whose verification run never
// reached a verdict inside the budget.
const timedOutError = "verification timed out"

// read is what one poll learned about the runs: every run of the try in
// submission order, of which the first read have the phase and error each last
// reported (the poll stops at the first status it cannot read), and the
// per-node errors of the failed ones.
type read struct {
	runs       []proposal.Verification
	read       int
	nodeErrors map[string]string
}

// decide turns what a poll read into the gate's verdict.
//
// The try is one proposal over one set of edits, so the statuses are combined
// rather than acted on individually: one failure fails it (the edits cannot be
// split, and the errors of every failed run together are the evidence the next
// try reads), otherwise every run must have reached a terminal phase before the
// try passes.
//
// A status that cannot be read is deliberately not a failure: the pipeline
// reports a release-controller that is briefly unreachable and a run id it will
// never know with the same error, so counting either as a failed try would let
// one outage burn the try budget on fixes that were never judged. The read is
// retried at the next poll, and the wait is still bounded because the same
// timeout that ends a run which never reaches a verdict also ends one whose
// status never becomes readable, counted from when the try was submitted.
//
// The budget of a running run counts from when that run itself left the
// pipeline's queue. Runs share one global queue and go one after another, so
// measuring from the try's start would fail a try whose run never ran, and
// sharing one budget across the try's runs would grow it with the number of
// services edited. A run still queued has not started, so it has spent nothing.
func (g *VerificationRun) decide(in step.GateInput, got read) step.GateVerdict {
	name := string(nameVerificationRun)
	now := g.clock.Now()
	seen := got.runs[:got.read]
	phases := observedPhases(seen)
	ids := runIDs(got.runs)

	if got.read < len(got.runs) {
		// The budget of an unreadable run runs from when the try was submitted:
		// with no status to read, nothing else measures it.
		since := in.SubmittedAt
		if since.IsZero() {
			since = in.Try.CreatedAt
		}
		if now.Sub(since) >= g.timeout {
			return timedOut(ids, seen, phases)
		}
		return waiting(name, ids, seen, phases)
	}

	anyFailed, allTerminal := false, true
	var runningSince []time.Time
	for _, r := range seen {
		switch r.Phase {
		case proposal.PhaseFailed:
			anyFailed = true
		case proposal.PhasePassed:
		default:
			allTerminal = false
			if r.ActivatedAt != nil {
				runningSince = append(runningSince, *r.ActivatedAt)
			}
		}
	}
	switch {
	case anyFailed:
		verifyErr := verifyError(in.Trigger.NodeIDs(), got.nodeErrors, got.runs)
		return step.GateVerdict{
			Result: remediation.CheckResult{
				Check:    name,
				Findings: failureFindings(got.nodeErrors, verifyErr),
				RunIDs:   ids,
			},
			Verifications: seen,
			VerifyError:   verifyErr,
			Phases:        phases,
		}
	case allTerminal:
		return step.GateVerdict{
			Result:        remediation.CheckResult{Check: name, RunIDs: ids},
			Verifications: seen,
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

func timedOut(ids []string, seen, phases []proposal.Verification) step.GateVerdict {
	return step.GateVerdict{
		Result: remediation.CheckResult{
			Check:    string(nameVerificationRun),
			Findings: []remediation.Finding{{Code: codeVerificationTimedOut, Message: timedOutError}},
			RunIDs:   ids,
		},
		Verifications: seen,
		VerifyError:   timedOutError,
		Phases:        phases,
	}
}

func waiting(name string, ids []string, seen, phases []proposal.Verification) step.GateVerdict {
	return step.GateVerdict{
		Result:        remediation.CheckResult{Check: name, Waiting: true, RunIDs: ids},
		Verifications: seen,
		Phases:        phases,
	}
}

// observedPhases are the runs this poll saw queued or running. A terminal
// phase is written with the outcome of the try instead.
func observedPhases(runs []proposal.Verification) []proposal.Verification {
	var out []proposal.Verification
	for _, r := range runs {
		if r.Phase != proposal.PhasePassed && r.Phase != proposal.PhaseFailed {
			out = append(out, r)
		}
	}
	return out
}

func runIDs(runs []proposal.Verification) []string {
	ids := make([]string, len(runs))
	for i, r := range runs {
		ids[i] = r.RunID
	}
	return ids
}

// failureFindings is one verification_failed finding per node a failed run
// reports an error for, in node order. A failure that names no node is one
// finding carrying the reason, so the result never passes.
func failureFindings(nodeErrors map[string]string, verifyErr string) []remediation.Finding {
	if len(nodeErrors) == 0 {
		return []remediation.Finding{{Code: codeVerificationFailed, Message: verifyErr}}
	}
	nodes := sortedNodes(nodeErrors)
	findings := make([]remediation.Finding, len(nodes))
	for i, n := range nodes {
		findings[i] = remediation.Finding{Code: codeVerificationFailed, Message: n + ": " + nodeErrors[n]}
	}
	return findings
}

// verifyError is the text recorded on a rejected try, and the evidence the next
// try is shown. The errors of the nodes this try fixed come first, named and in
// a stable order; when every one of them passed and other nodes did not (a fix
// that broke something downstream of itself) every failing node is named
// instead, so the reason describes what actually failed. A failure carrying no
// per-node error at all still records which run failed it, so the try never
// ends with a blank reason.
func verifyError(resolved []string, nodeErrors map[string]string, runs []proposal.Verification) string {
	if own := namedErrors(nodeErrors, stillFailing(resolved, nodeErrors)); own != "" {
		return own
	}
	if all := namedErrors(nodeErrors, sortedNodes(nodeErrors)); all != "" {
		return all
	}
	return fmt.Sprintf("verification run %s failed without a per-node error", runs[0].RunID)
}

// stillFailing is the try's own nodes that the failure still reports an error
// for. Nodes the failure names that this try never addressed are not returned:
// the trigger carries no failure for them, so there is nothing to re-fix.
func stillFailing(resolved []string, nodeErrors map[string]string) []string {
	still := make([]string, 0, len(resolved))
	for _, id := range resolved {
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

// applyStatus records one status read onto a run.
func applyStatus(r proposal.Verification, st ports.VerificationStatus) proposal.Verification {
	r.Phase = st.Phase
	if !st.ActivatedAt.IsZero() {
		at := st.ActivatedAt
		r.ActivatedAt = &at
	}
	if st.Phase == proposal.PhaseFailed {
		r.Error = namedErrors(st.NodeErrors, sortedNodes(st.NodeErrors))
	}
	return r
}
