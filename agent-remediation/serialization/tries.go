package serialization

import (
	"errors"
	"fmt"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// The journal rows TryRecordOf reads, named as the runner writes them.
const (
	// PreflightStep is the step name of the runner's pre-flight pass of a
	// cluster, one row per round, whose output is a PreflightRecord and whose
	// findings are every finding of the pass.
	PreflightStep = "preflight"
	// RunnerKind is the kind of the row of a step the runner performs itself.
	RunnerKind = "runner"
	// CheckKind is the kind of a check's row: a pre-flight check, a
	// cross-cluster check or the gate.
	CheckKind = "check"
	// FlowCluster is the cluster number of a flow-level row: a cross-cluster
	// check, the gate or an output.
	FlowCluster = 0
)

// TryRecordOf reads one try of a remediation from its journal, as snap holds
// it (snap.Try and that try's clusters and steps), into the record a later try
// is shown. Each cluster's edits are the draft of its last pre-flight round,
// one per edit that changes its file, with the diff from the file's original;
// its rounds are the findings of each pre-flight round; its outcome is the
// outcome it ended with, if any. The flow-level check rows give the try's
// cross-cluster findings and, from the row marked Gate, the gate's. Only done
// rows are read. A snapshot with no try, or a done row whose output does not
// decode, is an error.
func TryRecordOf(snap remediation.Snapshot) (remediation.TryRecord, error) {
	if snap.Try == nil {
		return remediation.TryRecord{}, errors.New("the snapshot holds no try")
	}
	rec := remediation.TryRecord{Round: snap.Remediation.Round, Try: snap.Try.Try, Attempt: snap.Try.Attempt}
	preflight := map[int][]remediation.StepRun{}
	for _, s := range snap.Steps {
		if s.Status != remediation.StepDone || s.Try != snap.Try.Try {
			continue
		}
		switch {
		case s.Cluster != FlowCluster && s.Kind == RunnerKind && s.Step == PreflightStep:
			preflight[s.Cluster] = append(preflight[s.Cluster], s)
		case s.Cluster == FlowCluster && s.Kind == CheckKind:
			check, err := DecodeRecord[CheckRecord](s.Output)
			if err != nil {
				return remediation.TryRecord{}, fmt.Errorf("decode the output of step %s of try %d: %w", s.Step, s.Try, err)
			}
			if check.Gate {
				rec.Gate = append(rec.Gate, check.Result.Findings...)
			} else {
				rec.CrossCluster = append(rec.CrossCluster, check.Result.Findings...)
			}
		}
	}
	for _, c := range snap.Clusters {
		cr, err := clusterRecord(c, preflight[c.Cluster])
		if err != nil {
			return remediation.TryRecord{}, fmt.Errorf("try %d: %w", snap.Try.Try, err)
		}
		rec.Clusters = append(rec.Clusters, cr)
	}
	return rec, nil
}

// clusterRecord is cluster c of a try with its done pre-flight rows.
func clusterRecord(c remediation.Cluster, rows []remediation.StepRun) (remediation.ClusterRecord, error) {
	cr := remediation.ClusterRecord{
		Type:         c.TypeName,
		TargetNodeID: c.TargetNodeID,
		Members:      append([]string(nil), c.Members...),
	}
	if c.Outcome != nil {
		cr.Outcome = string(c.Outcome.Status) + ": " + c.Outcome.Reason
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Round < rows[j].Round })
	for _, row := range rows {
		cr.Rounds = append(cr.Rounds, row.Findings)
	}
	if len(rows) == 0 {
		return cr, nil
	}
	last := rows[len(rows)-1]
	pass, err := DecodeRecord[PreflightRecord](last.Output)
	if err != nil {
		return cr, fmt.Errorf("decode the pre-flight draft of cluster %d in round %d: %w", c.Cluster, last.Round, err)
	}
	for _, e := range pass.Draft.Edits {
		if diff := proposal.ComputeUnifiedDiff(e.Original, e.Content, e.Path); diff != "" {
			cr.Edits = append(cr.Edits, remediation.EditRecord{Path: e.Path, Diff: diff})
		}
	}
	return cr, nil
}
