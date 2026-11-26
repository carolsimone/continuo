package typology

import "sort"

// CrossProjectCause claims every failing node that descends from a node ANOTHER
// project changed in this release. A release changes one project, so such a
// node did not change itself: a fix in its own project can never ship before
// the change that broke it (its release would be validated against the
// producer's production code), and the only fix that ships is at that changed
// ancestor. It runs before every other strategy, ignores error signatures (two
// consumers may fail differently on one change), and needs no minimum group
// size.
//
// A node with several cross-project changed ancestors is assigned to the
// nearest one (fewest upstream hops), ties broken by the smallest id: the
// direct upstream is the contract the node reads. Nodes are grouped per
// target, members sorted, clusters ordered by their smallest member.
type CrossProjectCause struct{}

func (CrossProjectCause) Claim(remaining []FailingNode, dag DagView) ([]Cluster, []FailingNode) {
	byID := make(map[string]FailingNode, len(remaining))
	nodeTypeByID := make(map[string]string, len(remaining))
	byTarget := map[string][]string{}
	var rest []FailingNode
	for _, n := range remaining {
		byID[n.NodeID] = n
		nodeTypeByID[n.NodeID] = n.NodeType
		target, ok := nearestCrossProjectAncestor(n, dag)
		if !ok {
			rest = append(rest, n)
			continue
		}
		byTarget[target] = append(byTarget[target], n.NodeID)
	}
	// Consider targets in id order so both the emitted clusters and the members
	// handed back unclaimed stay deterministic regardless of map iteration.
	targets := make([]string, 0, len(byTarget))
	for target := range byTarget {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	clusters := make([]Cluster, 0, len(byTarget))
	for _, target := range targets {
		members := byTarget[target]
		sort.Strings(members)
		// A prospective cluster whose members are ALL dbt-test nodes is not a fix
		// target: a test binds again when the model it guards is fixed, and a test
		// that is itself wrong is a human's edit. Do not emit that cluster; hand
		// its members back unclaimed so they flow to the next cause and, failing
		// that, the independent default where the runner hands a dbt-test target over. A
		// mixed cluster (at least one model/seed/snapshot member) still forms, so
		// the producer is fixed and its tests ride along.
		if allDbtTest(members, nodeTypeByID) {
			for _, m := range members {
				rest = append(rest, byID[m])
			}
			continue
		}
		clusters = append(clusters, Cluster{TargetNodeID: target, Members: members, Kind: KindCrossProjectUpstream})
	}
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].Members[0] < clusters[j].Members[0] })
	return clusters, rest
}

// nearestCrossProjectAncestor picks the changed ancestor in a project other
// than the node's own that is closest to it, ties on id. False when the node
// or an ancestor names no project (nothing to compare) or no ancestor is in
// another project.
func nearestCrossProjectAncestor(n FailingNode, dag DagView) (string, bool) {
	if n.Project == "" {
		return "", false
	}
	var best ChangedAncestor
	found := false
	for _, a := range dag.ChangedAncestorsByNode[n.NodeID] {
		if a.Project == "" || a.Project == n.Project {
			continue
		}
		if !found || a.Depth < best.Depth || (a.Depth == best.Depth && a.NodeID < best.NodeID) {
			best, found = a, true
		}
	}
	return best.NodeID, found
}
