package typology

import "sort"

// SharedUpstreamCause groups failing nodes that fail the same way (identical
// error signature) and descend from a common node that changed this release.
// It targets that changed ancestor, so one edit resolves every member — even
// when the ancestor itself did not fail. An empty signature never groups.
type SharedUpstreamCause struct{}

func (SharedUpstreamCause) Claim(remaining []FailingNode, dag DagView) ([]Cluster, []FailingNode) {
	bySig := map[string][]FailingNode{}
	var sigOrder []string
	for _, n := range remaining {
		if _, ok := bySig[n.ErrorSignature]; !ok {
			sigOrder = append(sigOrder, n.ErrorSignature)
		}
		bySig[n.ErrorSignature] = append(bySig[n.ErrorSignature], n)
	}

	claimedSet := map[string]bool{}
	var clusters []Cluster
	for _, sig := range sigOrder {
		group := bySig[sig]
		if sig == "" || len(group) < 2 {
			continue
		}
		target, ok := commonChangedAncestor(group, dag)
		if !ok {
			continue
		}
		members := make([]string, 0, len(group))
		for _, n := range group {
			members = append(members, n.NodeID)
			claimedSet[n.NodeID] = true
		}
		sort.Strings(members)
		clusters = append(clusters, Cluster{TargetNodeID: target, Members: members, Kind: KindSharedUpstream})
	}

	var rest []FailingNode
	for _, n := range remaining {
		if !claimedSet[n.NodeID] {
			rest = append(rest, n)
		}
	}
	return clusters, rest
}

// commonChangedAncestor returns a changed ancestor shared by every node in the
// group. Ties are broken by the lexicographically smallest id so grouping is
// deterministic regardless of map iteration order.
func commonChangedAncestor(group []FailingNode, dag DagView) (string, bool) {
	counts := map[string]int{}
	for _, n := range group {
		seen := map[string]bool{}
		for _, anc := range dag.ChangedAncestorsByNode[n.NodeID] {
			if seen[anc] {
				continue
			}
			seen[anc] = true
			counts[anc]++
		}
	}
	var common []string
	for anc, c := range counts {
		if c == len(group) {
			common = append(common, anc)
		}
	}
	if len(common) == 0 {
		return "", false
	}
	sort.Strings(common)
	return common[0], true
}
