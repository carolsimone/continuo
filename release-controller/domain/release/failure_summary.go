package release

import (
	"fmt"
	"sort"
	"strings"
)

// maxNamedNodes and maxNamedUpstream bound how many names a failure summary
// spells out. The summary lands in a single operator-facing text field (the
// release's fail_detail), and a stage can fail for hundreds of nodes; the rest
// are counted, and the full list stays in the release's per-node results.
const (
	maxNamedNodes    = 5
	maxNamedUpstream = 3
)

// FailedNode is a node that did not pass a pipeline stage, with the service
// the candidate topology declares for it ("" when it names none).
type FailedNode struct {
	NodeID  string
	Service string
}

// SummarizeValidationFailure is the operator-facing explanation of a
// validation rejection, built from what release-controller knows about the
// failure without reading any log: which nodes did not pass, in which service,
// and which changed nodes sit upstream of them. It says where to look, not why
// a node failed; the why lives in the node's log, which only the log's owner
// interprets. total is the number of nodes the stage validated and upstream
// the changed ancestors of every failing node, in any order and possibly
// repeating a node reached through several of them. An ancestor that failed
// too is already named as failing, so only the ones that passed are listed:
// those are the changes that broke their consumers without breaking themselves.
func SummarizeValidationFailure(total int, failing []FailedNode, upstream []ChangedAncestor) string {
	if len(failing) == 0 {
		return "validation failed, but no per-node failure was recorded"
	}
	summary := fmt.Sprintf("%d of %d nodes did not pass validation: %s", len(failing), total, nameNodes(failing, maxNamedNodes))
	passing := nearestFirstExcept(upstream, failing)
	if len(passing) == 0 {
		return summary
	}
	return summary + "; changed upstream of the failing nodes: " + nameNodes(passing, maxNamedUpstream)
}

// SummarizeSeedFailure is the operator-facing explanation of a seed-build
// rejection: which seeds did not build, and in which service.
func SummarizeSeedFailure(failing []FailedNode) string {
	if len(failing) == 0 {
		return "seed build failed, but no per-node failure was recorded"
	}
	noun := "seeds"
	if len(failing) == 1 {
		noun = "seed"
	}
	return fmt.Sprintf("%d %s did not build: %s", len(failing), noun, nameNodes(failing, maxNamedNodes))
}

// SummarizeCompileFailure is the operator-facing explanation of a compile
// rejection that carries no attribution to a pod container. The compile leg
// reports one node per service it compiled, so the node ids name services.
func SummarizeCompileFailure(failing []FailedNode) string {
	if len(failing) == 0 {
		return "dbt compile failed, but no per-node failure was recorded"
	}
	return "dbt compile failed for: " + nameNodes(failing, maxNamedNodes)
}

// nameNodes renders "a (svc), b (svc) and c (svc)", or, past limit nodes,
// "a (svc), …, e (svc) and 7 more". Only the nodes it names are formatted, so
// a stage that fails for thousands of nodes costs the same as one that fails
// for five.
func nameNodes(nodes []FailedNode, limit int) string {
	named := nodes
	if len(named) > limit {
		named = named[:limit]
	}
	labels := make([]string, len(named))
	for i, n := range named {
		labels[i] = n.label()
	}
	if rest := len(nodes) - len(named); rest > 0 {
		return strings.Join(labels, ", ") + fmt.Sprintf(" and %d more", rest)
	}
	return joinWithAnd(labels)
}

func (n FailedNode) label() string {
	if n.Service == "" {
		return n.NodeID
	}
	return n.NodeID + " (" + n.Service + ")"
}

// nearestFirstExcept collapses the ancestors to one entry per node, at the
// shortest distance any failing node reaches it, drops the nodes in except, and
// orders what is left nearest first and then by id. It leaves its argument
// untouched: the same slice also feeds the rejection payload, which keeps the
// order it was built in.
func nearestFirstExcept(ancestors []ChangedAncestor, except []FailedNode) []FailedNode {
	if len(ancestors) == 0 {
		return nil
	}
	skip := make(map[string]struct{}, len(except))
	for _, n := range except {
		skip[n.NodeID] = struct{}{}
	}
	closest := make(map[string]ChangedAncestor, len(ancestors))
	for _, a := range ancestors {
		if _, skipped := skip[a.NodeID]; skipped {
			continue
		}
		if seen, ok := closest[a.NodeID]; !ok || a.Depth < seen.Depth {
			closest[a.NodeID] = a
		}
	}
	unique := make([]ChangedAncestor, 0, len(closest))
	for _, a := range closest {
		unique = append(unique, a)
	}
	sort.Slice(unique, func(i, j int) bool {
		if unique[i].Depth != unique[j].Depth {
			return unique[i].Depth < unique[j].Depth
		}
		return unique[i].NodeID < unique[j].NodeID
	})
	nodes := make([]FailedNode, len(unique))
	for i, a := range unique {
		nodes[i] = FailedNode{NodeID: a.NodeID, Service: a.Service}
	}
	return nodes
}
