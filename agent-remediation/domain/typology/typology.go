// Package typology deterministically partitions a release's failing node set
// into clusters, each naming the node whose source a fix must edit. It is pure:
// it consumes the failing set and a precomputed DAG view (both supplied by the
// driver) and performs no I/O, so the routing decision is testable without the
// LLM or any port.
package typology

import "sort"

// FailingNode is one classified failure in the release under remediation.
type FailingNode struct {
	NodeID         string
	ErrorSignature string
	Category       string
	Reason         string
}

// DagView answers the graph questions grouping needs, precomputed by the driver
// from orchestrator reads. ChangedAncestorsByNode maps a failing node's id to
// the unique_ids of its ancestors that changed in this release, nearest-first.
type DagView struct {
	ChangedAncestorsByNode map[string][]string
}

// Kind labels why a cluster was formed.
type Kind string

const (
	// KindIndependent is a single failing node fixed in its own source.
	KindIndependent Kind = "independent"
	// KindSharedUpstream is several same-signature failing nodes fixed by one
	// edit to a common changed ancestor that may not itself have failed.
	KindSharedUpstream Kind = "shared_upstream"
)

// Cluster is one fix target: the node to edit (TargetNodeID, which may be an
// upstream node absent from Members) and the failing nodes it resolves.
type Cluster struct {
	TargetNodeID string
	Members      []string
	Kind         Kind
}

// Typology is one grouping strategy: it claims the failing nodes it recognizes
// as clusters and returns the nodes it did not claim for the next strategy.
type Typology interface {
	Claim(remaining []FailingNode, dag DagView) (claimed []Cluster, rest []FailingNode)
}

// Group runs strategies in order over the failing set; every node no strategy
// claims becomes its own independent cluster. Output is deterministic: claimed
// clusters in strategy order, then independents sorted by node id.
func Group(nodes []FailingNode, dag DagView, strategies ...Typology) []Cluster {
	remaining := append([]FailingNode(nil), nodes...)
	var out []Cluster
	for _, s := range strategies {
		claimed, rest := s.Claim(remaining, dag)
		out = append(out, claimed...)
		remaining = rest
	}
	sort.Slice(remaining, func(i, j int) bool { return remaining[i].NodeID < remaining[j].NodeID })
	for _, n := range remaining {
		out = append(out, Cluster{
			TargetNodeID: n.NodeID,
			Members:      []string{n.NodeID},
			Kind:         KindIndependent,
		})
	}
	return out
}
