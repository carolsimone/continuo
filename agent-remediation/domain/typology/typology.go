// Package typology deterministically partitions a release's failing node set
// into clusters, each naming the node whose source a fix must edit. It is pure:
// it consumes the failing set and a precomputed DAG view (both supplied by the
// driver) and performs no I/O, so the routing decision is testable without the
// LLM or any port.
package typology

import "sort"

// FailingNode is one classified failure in the release under remediation.
// Service is the node's owning service, as the trigger carries it.
type FailingNode struct {
	NodeID         string
	ErrorSignature string
	Category       string
	Reason         string
	Service        string
}

// ChangedAncestor is one upstream node of a failing node that changed in this
// release: its id, its owning service, and its minimum upstream hop distance
// from the failing node (1 = direct upstream; 0 when the trigger carried none).
type ChangedAncestor struct {
	NodeID  string
	Service string
	Depth   int
}

// DagView answers the graph questions grouping needs, precomputed by the driver
// from the trigger. ChangedAncestorsByNode maps a failing node's id to its
// ancestors that changed in this release, in the trigger's order (sorted by id).
type DagView struct {
	ChangedAncestorsByNode map[string][]ChangedAncestor
}

// Kind labels why a cluster was formed.
type Kind string

const (
	// KindIndependent is a single failing node fixed in its own source.
	KindIndependent Kind = "independent"
	// KindSharedUpstream is several same-signature failing nodes fixed by one
	// edit to a common changed ancestor that may not itself have failed.
	KindSharedUpstream Kind = "shared_upstream"
	// KindCrossServiceUpstream is one or more failing nodes fixed by one edit to
	// a changed ancestor that lives in ANOTHER service: under one service per
	// release the failing nodes did not change, and a fix in their own service
	// could never ship before the change that broke them.
	KindCrossServiceUpstream Kind = "cross_service_upstream"
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
