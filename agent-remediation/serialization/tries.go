package serialization

import (
	"errors"
	"fmt"
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
)

// TryRecordOf reads one try of a remediation from its journal, as snap holds
// it (snap.Try and that try's clusters and steps), into the record a later try
// is shown. It reads only the rows StepRun.ShownToLaterTries names. Each
// cluster's edits are the draft of its last pre-flight round, one per edit that
// changes its file, with the diff from the file's original; its rounds are the
// findings of each pre-flight round; its outcome is the outcome it ended with,
// if any, as its status, the code it was handed over with and its reason. The
// flow-level check rows give the try's cross-cluster findings and, from the row
// marked Gate, the gate's, in the order the rows were written (by id). Clusters
// come in cluster order. The record does not depend on the order snap lists
// its clusters and steps in. A snapshot with no try, or a row whose output does
// not decode, is an error.
func TryRecordOf(snap remediation.Snapshot) (remediation.TryRecord, error) {
	if snap.Try == nil {
		return remediation.TryRecord{}, errors.New("the snapshot holds no try")
	}
	rec := remediation.TryRecord{Round: snap.Remediation.Round, Try: snap.Try.Try, Attempt: snap.Try.Attempt}
	steps := append([]remediation.StepRun(nil), snap.Steps...)
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].ID < steps[j].ID })
	preflight := map[int][]remediation.StepRun{}
	for _, s := range steps {
		if !s.ShownToLaterTries() || s.Try != snap.Try.Try {
			continue
		}
		if s.Cluster != remediation.FlowCluster {
			preflight[s.Cluster] = append(preflight[s.Cluster], s)
			continue
		}
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
	clusters := append([]remediation.Cluster(nil), snap.Clusters...)
	sort.SliceStable(clusters, func(i, j int) bool { return clusters[i].Cluster < clusters[j].Cluster })
	for _, c := range clusters {
		cr, err := clusterRecord(c, preflight[c.Cluster])
		if err != nil {
			return remediation.TryRecord{}, fmt.Errorf("try %d: %w", snap.Try.Try, err)
		}
		rec.Clusters = append(rec.Clusters, cr)
	}
	return rec, nil
}

// outcomeText is how a later try is shown the outcome a cluster ended with: its
// status, the code in parentheses when it carries one, then its reason.
func outcomeText(o proposal.NodeOutcome) string {
	if o.Code == "" {
		return string(o.Status) + ": " + o.Reason
	}
	return string(o.Status) + " (" + string(o.Code) + "): " + o.Reason
}

// clusterRecord is cluster c of a try with its done pre-flight rows.
func clusterRecord(c remediation.Cluster, rows []remediation.StepRun) (remediation.ClusterRecord, error) {
	cr := remediation.ClusterRecord{
		Type:         c.TypeName,
		TargetNodeID: c.TargetNodeID,
		Members:      append([]string(nil), c.Members...),
	}
	if c.Outcome != nil {
		cr.Outcome = outcomeText(*c.Outcome)
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
