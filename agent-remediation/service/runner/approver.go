package runner

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// approversFor runs the flow's approver for nodes, worked in clusters, and
// returns one approver per node sorted by node id. It runs in the transaction
// that ends the remediation, so an error fails that transaction and the end
// runs again in the next drive or poll. With no node, nothing runs.
func (r *Runner) approversFor(ctx context.Context, run tryRun, clusters []step.ClusterDraft, nodes []string) ([]proposal.Approver, error) {
	if len(nodes) == 0 {
		return nil, nil
	}
	name := run.flow.Approver.Name
	a, err := lookup[step.Approver](r.d.Registry, name, step.KindApprover)
	if err != nil {
		return nil, err
	}
	out, err := a.Approvers(ctx, step.ApproverInput{Trigger: run.trigger, Clusters: clusters, Nodes: nodes})
	if err != nil {
		return nil, fmt.Errorf("step %s: %w", name, err)
	}
	slices.SortFunc(out, func(a, b proposal.Approver) int { return strings.Compare(a.NodeID, b.NodeID) })
	return out, nil
}

// coveredNodes is every member of the drafts' clusters, sorted, each once: the
// nodes a verified fix of those drafts covers.
func coveredNodes(drafts []step.ClusterDraft) []string {
	var out []string
	for _, d := range drafts {
		out = append(out, d.Cluster.Members...)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// workedClusters is every cluster of the remediation's current try, in cluster
// order, with the evidence its context steps gathered (gatheredEvidence). A
// remediation that made no try has none.
func (r *Runner) workedClusters(run tryRun) ([]step.ClusterDraft, error) {
	if run.snap.Try == nil {
		return nil, nil
	}
	out := make([]step.ClusterDraft, 0, len(run.snap.Clusters))
	for _, c := range run.snap.Clusters {
		ev, err := r.gatheredEvidence(run, c, failureFor(run.trigger, c, *run.snap.Try))
		if err != nil {
			return nil, fmt.Errorf("read back cluster %d: %w", c.Cluster, err)
		}
		out = append(out, step.ClusterDraft{Cluster: c, Evidence: ev})
	}
	return out, nil
}
