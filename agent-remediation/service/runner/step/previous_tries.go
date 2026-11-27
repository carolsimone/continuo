package step

import (
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// WithPreviousTries returns req with the section on the earlier tries at the
// cluster's failure that ev holds under remediation.KeyPreviousTries, as
// prompt.WithPreviousTries renders it. A prompt step applies it to every
// request of its first round; a later round revises the last request of the
// round before it (RevisionRequest), which therefore already carries the
// section. Evidence with no earlier try leaves req as it is.
func WithPreviousTries(req prompt.ProposeRequest, ev remediation.Evidence) prompt.ProposeRequest {
	records, _ := remediation.Get[[]remediation.TryRecord](ev, remediation.KeyPreviousTries)
	return prompt.WithPreviousTries(req, previousTries(records))
}

// previousTries is each record as the prompt shows it, labelled by its round
// and try ("Round 2, try 1"). Its diffs are every cluster's edits; its findings are each pre-flight round's
// findings, then how a cluster ended when it ended without a checked draft,
// then the cross-cluster findings; its gate error is the gate's finding
// messages joined by "; ".
func previousTries(records []remediation.TryRecord) []prompt.PreviousTry {
	out := make([]prompt.PreviousTry, 0, len(records))
	for _, rec := range records {
		t := prompt.PreviousTry{Label: fmt.Sprintf("Round %d, try %d", rec.Round, rec.Try)}
		for _, c := range rec.Clusters {
			for _, e := range c.Edits {
				t.Diffs = append(t.Diffs, prompt.TryDiff{Path: e.Path, Diff: e.Diff})
			}
			for i, round := range c.Rounds {
				for _, line := range FindingLines(round) {
					t.Findings = append(t.Findings, fmt.Sprintf("round %d: %s", i+1, line))
				}
			}
			if c.Outcome != "" {
				t.Findings = append(t.Findings, "outcome: "+c.Outcome)
			}
		}
		for _, line := range FindingLines(rec.CrossCluster) {
			t.Findings = append(t.Findings, "across clusters: "+line)
		}
		gate := make([]string, 0, len(rec.Gate))
		for _, f := range rec.Gate {
			gate = append(gate, f.Message)
		}
		t.GateError = strings.Join(gate, "; ")
		out = append(out, t)
	}
	return out
}
