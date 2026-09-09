package typology

import (
	"sort"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// SharedUpstreamCause groups failing nodes that fail the same way (identical
// error signature) and descend from a common node that changed this release.
// It targets that changed ancestor, so one edit resolves every member — even
// when the ancestor itself did not fail. An empty signature never groups.
//
// Within one signature the nodes may not all share a single ancestor: the same
// failure can reach two unrelated changed ancestors. Each signature is therefore
// partitioned into one cluster per ancestor-sharing subset, so a batch like
// {a,b}→u and {c,d}→v under one signature yields two upstream clusters rather
// than falling through to independent fixes.
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
		for _, c := range clusterByChangedAncestor(group, dag) {
			for _, m := range c.Members {
				claimedSet[m] = true
			}
			clusters = append(clusters, c)
		}
	}

	// Emit clusters ordered by their smallest member so the output does not
	// depend on input order. Members are disjoint across clusters, so the
	// smallest member is a unique, stable key.
	sort.Slice(clusters, func(i, j int) bool { return clusters[i].Members[0] < clusters[j].Members[0] })

	var rest []FailingNode
	for _, n := range remaining {
		if !claimedSet[n.NodeID] {
			rest = append(rest, n)
		}
	}
	return clusters, rest
}

// clusterByChangedAncestor partitions one same-signature group into shared
// clusters. It considers candidate changed ancestors in ascending id order and,
// for each, gathers the still-unclaimed group members that descend from it; a
// candidate with at least two such members becomes one cluster targeting it.
// Considering the smallest ancestor id first makes both the assignment and the
// target choice deterministic regardless of map iteration or input order. Nodes
// matching no shared ancestor are left for the independent default strategy.
func clusterByChangedAncestor(group []FailingNode, dag DagView) []Cluster {
	ancestorsByNode := make(map[string]map[string]bool, len(group))
	nodeTypeByID := make(map[string]string, len(group))
	candidateSet := map[string]bool{}
	for _, n := range group {
		nodeTypeByID[n.NodeID] = n.NodeType
		set := map[string]bool{}
		for _, anc := range dag.ChangedAncestorsByNode[n.NodeID] {
			set[anc.NodeID] = true
			candidateSet[anc.NodeID] = true
		}
		ancestorsByNode[n.NodeID] = set
	}
	candidates := make([]string, 0, len(candidateSet))
	for anc := range candidateSet {
		candidates = append(candidates, anc)
	}
	sort.Strings(candidates)

	claimed := map[string]bool{}
	var clusters []Cluster
	for _, anc := range candidates {
		var members []string
		for _, n := range group {
			if claimed[n.NodeID] {
				continue
			}
			if ancestorsByNode[n.NodeID][anc] {
				members = append(members, n.NodeID)
			}
		}
		if len(members) < 2 {
			continue
		}
		sort.Strings(members)
		// A prospective cluster whose members are ALL dbt-test nodes is not a fix
		// target: a test binds again when the model it guards is fixed, and a test
		// that is itself wrong is a human's edit. Leave those members unclaimed so
		// they fall through to the independent default, where the driver skips a
		// dbt-test target. A mixed cluster (at least one model/seed/snapshot) still
		// forms, so the model is fixed and its tests ride along.
		if allDbtTest(members, nodeTypeByID) {
			continue
		}
		for _, m := range members {
			claimed[m] = true
		}
		clusters = append(clusters, Cluster{TargetNodeID: anc, Members: members, Kind: KindSharedUpstream})
	}
	return clusters
}

// allDbtTest reports whether every listed node id is a dbt-test node, per the
// node kinds the trigger carried. An empty list is not "all tests".
func allDbtTest(ids []string, nodeTypeByID map[string]string) bool {
	if len(ids) == 0 {
		return false
	}
	for _, id := range ids {
		if nodeTypeByID[id] != string(pkg_model.NodeTypeDbtTest) {
			return false
		}
	}
	return true
}
