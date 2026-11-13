package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// noEditReason is the outcome of the members of a checked cluster whose draft
// has no edit: nothing a verification run could run, and nothing a pull
// request could carry.
const noEditReason = "the fix could not be applied to version-controlled source, so there is nothing to verify"

// checkedCluster is a checked cluster of the try with the draft it produced and
// the evidence it was made from, as the journal recorded them, and the results
// of its pre-flight checks.
type checkedCluster struct {
	draft   step.ClusterDraft
	results []remediation.CheckResult
}

// checkedClusters rebuilds every checked cluster of the try from the journal,
// in cluster order. Its evidence is the failure, what each of its context
// steps gathered, what each of its pre-flight checks gathered (a packaged
// contract among them) and the draft as the pre-flight pass left it, with its
// originals and stored content. The same rows always rebuild the same drafts,
// so the gate is polled with exactly the drafts it was submitted.
func (r *Runner) checkedClusters(run tryRun) ([]checkedCluster, error) {
	var out []checkedCluster
	for _, c := range run.snap.Clusters {
		if c.Status != remediation.ClusterChecked {
			continue
		}
		cc, err := r.checkedCluster(run, c)
		if err != nil {
			return nil, fmt.Errorf("read back cluster %d: %w", c.Cluster, err)
		}
		out = append(out, cc)
	}
	return out, nil
}

func (r *Runner) checkedCluster(run tryRun, c remediation.Cluster) (checkedCluster, error) {
	plan, err := r.planFor(run, c)
	if err != nil {
		return checkedCluster{}, err
	}
	doneRow := func(name string) (remediation.StepRun, error) {
		row, done := run.journal.done(c.Cluster, name, c.Round)
		if !done {
			return row, fmt.Errorf("step %s has no done row", name)
		}
		return row, nil
	}
	ev := remediation.NewEvidence().With(remediation.KeyFailure, failureFor(run.trigger, c, run.snap.Try.Attempt))
	for _, ps := range plan.context {
		row, err := doneRow(string(ps.name))
		if err != nil {
			return checkedCluster{}, err
		}
		if ev, err = r.contextEvidence(ev, row); err != nil {
			return checkedCluster{}, err
		}
	}
	results := make([]remediation.CheckResult, 0, len(plan.preflight))
	for _, ps := range plan.preflight {
		row, err := doneRow(string(ps.name))
		if err != nil {
			return checkedCluster{}, err
		}
		rec, err := checkResult(row)
		if err != nil {
			return checkedCluster{}, err
		}
		if ev, err = r.withEvidence(ev, rec.Evidence); err != nil {
			return checkedCluster{}, err
		}
		results = append(results, rec.Result)
	}
	pass, err := doneRow(stepPreflight)
	if err != nil {
		return checkedCluster{}, err
	}
	draft, err := preflightDraft(pass)
	if err != nil {
		return checkedCluster{}, err
	}
	return checkedCluster{
		draft:   step.ClusterDraft{Cluster: c, Draft: draft, Evidence: ev.With(remediation.KeyDraft, draft)},
		results: results,
	}, nil
}

// draftsOf is the drafts of the checked clusters that have an edit, in cluster
// order: what the cross-cluster checks and the gate judge.
func draftsOf(checked []checkedCluster) []step.ClusterDraft {
	var out []step.ClusterDraft
	for _, cc := range checked {
		if len(cc.draft.Draft.Edits) > 0 {
			out = append(out, cc.draft)
		}
	}
	return out
}

// draftsDigest identifies what a flow-level step ran on: every draft it was
// given, with its cluster and evidence.
func draftsDigest(drafts []step.ClusterDraft) (string, error) {
	type entry struct {
		Cluster  int                             `json:"cluster"`
		Draft    remediation.Draft               `json:"draft"`
		Evidence map[remediation.EvidenceKey]any `json:"evidence"`
	}
	entries := make([]entry, 0, len(drafts))
	for _, d := range drafts {
		entries = append(entries, entry{Cluster: d.Cluster.Cluster, Draft: d.Draft, Evidence: evidenceValues(d.Evidence)})
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		return "", fmt.Errorf("encode the step's drafts: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// nodeOutcomes is how the try ended for each failing node: verifying for the
// members of a checked cluster with an edit, failed for the members of a
// checked cluster whose draft has no edit, and the outcome a handed-over or
// fallen-back cluster recorded for its members. A shared-upstream cluster that
// fell back records none: each of its members is the target of a cluster of
// its own.
func nodeOutcomes(clusters []remediation.Cluster, checked []checkedCluster) map[string]proposal.NodeOutcome {
	withEdit := make(map[int]bool, len(checked))
	for _, cc := range checked {
		withEdit[cc.draft.Cluster.Cluster] = len(cc.draft.Draft.Edits) > 0
	}
	out := map[string]proposal.NodeOutcome{}
	for _, c := range clusters {
		var o proposal.NodeOutcome
		switch {
		case c.Status == remediation.ClusterChecked && withEdit[c.Cluster]:
			o = proposal.NodeOutcome{Status: proposal.StatusVerifying}
		case c.Status == remediation.ClusterChecked:
			o = proposal.NodeOutcome{Status: proposal.StatusFailed, Reason: noEditReason}
		case c.Outcome != nil:
			o = *c.Outcome
		default:
			continue
		}
		for _, m := range c.Members {
			out[m] = o
		}
	}
	return out
}

// settledStatus is the attempt's status when no cluster produced an edit: a
// skip when every node was skipped, and a failure as soon as one node was
// actually attempted and did not yield a change.
func settledStatus(outcomes map[string]proposal.NodeOutcome) proposal.Status {
	for _, o := range outcomes {
		if o.Status != proposal.StatusSkipped {
			return proposal.StatusFailed
		}
	}
	return proposal.StatusSkipped
}

// verifyingAttempt is the attempt's proposal row once the gate started its
// runs: every edit of the checked drafts, each naming the members of its
// cluster, the runs that verify them, the weakest cluster's confidence, one
// composed rationale per cluster, the first model that answered, and the
// repository and commit the edits apply to.
func verifyingAttempt(run tryRun, checked []checkedCluster, verifications []proposal.Verification) proposal.Proposal {
	t := run.trigger
	p := proposal.Proposal{
		Status:         proposal.StatusVerifying,
		NodeOutcomes:   nodeOutcomes(run.snap.Clusters, checked),
		Verifications:  verifications,
		SourceResolved: true,
		Repo:           t.Repo,
		CommitSHA:      t.CommitSHA,
		TriggerPayload: run.snap.Remediation.TriggerPayload,
	}
	var rationales []string
	for _, d := range draftsOf(checked) {
		for _, e := range d.Draft.Edits {
			target := e.TargetNodeID
			if target == "" {
				target = d.Cluster.TargetNodeID
			}
			p.Edits = append(p.Edits, proposal.FileEdit{
				Path:          e.Path,
				ContentURI:    e.ContentURI,
				DiffURI:       e.DiffURI,
				TargetNodeID:  target,
				MemberNodeIDs: append([]string(nil), d.Cluster.Members...),
			})
		}
		rationales = append(rationales, proposal.ComposeRationale(rationaleFactsFor(t, d.Cluster, d.Draft)))
		c := normalizeConfidence(d.Draft.Confidence)
		if p.Confidence == "" || confidenceRank(c) < confidenceRank(p.Confidence) {
			p.Confidence = c
		}
		if p.Model == "" {
			p.Model = d.Draft.Model
		}
	}
	p.Rationale = strings.Join(rationales, "\n\n")
	return p
}

// normalizeConfidence maps the model's free-form confidence to the domain enum,
// defaulting to medium on unrecognised values.
func normalizeConfidence(c string) proposal.Confidence {
	switch c {
	case "low":
		return proposal.ConfidenceLow
	case "high":
		return proposal.ConfidenceHigh
	default:
		return proposal.ConfidenceMedium
	}
}

// confidenceRank orders the confidence values so an attempt can report the
// weakest cluster's: a reviewer judges the whole change at once, so the least
// confident part of it is what the value has to reflect.
func confidenceRank(c proposal.Confidence) int {
	switch c {
	case proposal.ConfidenceLow:
		return 0
	case proposal.ConfidenceHigh:
		return 2
	default:
		return 1
	}
}
