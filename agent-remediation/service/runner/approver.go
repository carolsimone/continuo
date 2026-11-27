package runner

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// approversFor runs the flow's approver for nodes, worked in clusters, and
// returns one approver per node sorted by node id. It runs in the transaction
// that ends the remediation, so an error fails that transaction and the end
// runs again in the next drive or poll. With no node, nothing runs.
func (r *Runner) approversFor(ctx context.Context, run tryRun, clusters []step.ClusterDraft, nodes []string) ([]fix.Approver, error) {
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
	slices.SortFunc(out, func(a, b fix.Approver) int { return strings.Compare(a.NodeID, b.NodeID) })
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
// order, with the evidence its context steps gathered (gatheredEvidence),
// followed by the clusters of earlier tries that settled the nodes the current
// try carried (settlingClusters). A remediation that made no try has none.
func (r *Runner) workedClusters(ctx context.Context, repo repository.RemediationRepository, run tryRun) ([]step.ClusterDraft, error) {
	if run.snap.Try == nil {
		return nil, nil
	}
	out := make([]step.ClusterDraft, 0, len(run.snap.Clusters))
	var carried []string
	for _, c := range run.snap.Clusters {
		ev, err := r.gatheredEvidence(run, c, failureFor(run.trigger, c, *run.snap.Try))
		if err != nil {
			return nil, fmt.Errorf("read back cluster %d: %w", c.Cluster, err)
		}
		out = append(out, step.ClusterDraft{Cluster: c, Evidence: ev})
		if !run.journal.worked(c.Cluster) {
			carried = append(carried, c.Members...)
		}
	}
	if len(carried) == 0 || run.snap.Try.Try == 1 {
		return out, nil
	}
	slices.Sort(carried)
	carried = slices.Compact(carried)
	earlier, err := repo.ListContextOfEarlierTries(ctx, run.snap.Remediation.ID, run.snap.Try.Try, carried)
	if err != nil {
		return nil, err
	}
	settling, err := r.settlingClusters(run.trigger, earlier, carried)
	if err != nil {
		return nil, err
	}
	return append(out, settling...), nil
}

// settlingClusters is, for each node of nodes, the clusters of the latest
// earlier try in which a cluster holding the node ran a context step, each with
// the evidence it gathered there, in the order the nodes and then the tries'
// clusters come. A node settled for good is carried into every later try as
// a cluster that runs no step, so the clusters that last worked it are the ones
// that settled it, and their journal rows hold what they read, such as the
// contract file that names the node's owner. earlier is the snapshots of the
// earlier tries, newest first, with their done context rows
// (ListContextOfEarlierTries). A node no earlier cluster worked adds none, and
// a cluster is added once.
func (r *Runner) settlingClusters(t remediation.Trigger, earlier []remediation.Snapshot, nodes []string) ([]step.ClusterDraft, error) {
	type origin struct{ try, cluster int }
	added := map[origin]bool{}
	var out []step.ClusterDraft
	for _, node := range nodes {
		for _, snap := range earlier {
			j := newJournal(snap.Steps)
			found := false
			for _, c := range snap.Clusters {
				if !slices.Contains(c.Members, node) || !j.worked(c.Cluster) {
					continue
				}
				found = true
				if added[origin{c.Try, c.Cluster}] {
					continue
				}
				added[origin{c.Try, c.Cluster}] = true
				ev, err := r.gatheredIn(j, c, failureFor(t, c, *snap.Try))
				if err != nil {
					return nil, fmt.Errorf("read back cluster %d of try %d: %w", c.Cluster, c.Try, err)
				}
				out = append(out, step.ClusterDraft{Cluster: c, Evidence: ev})
			}
			if found {
				break
			}
		}
	}
	return out, nil
}
