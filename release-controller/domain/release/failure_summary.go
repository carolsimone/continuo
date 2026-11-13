package release

import (
	"fmt"
	"slices"
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
// the candidate topology declares for it ("" when it names none). Skipped marks
// a node that never ran because something upstream of it failed: a consequence
// of a failure, not a failure of its own.
type FailedNode struct {
	NodeID  string
	Service string
	Skipped bool
}

// SummarizeValidationFailure is the operator-facing explanation of a
// validation rejection, built from what release-controller knows about the
// failure without reading any log: which nodes failed, in which service, how
// many more were skipped because of them, and which changed nodes sit upstream
// of them. It says where to look, not why a node failed; the why lives in the
// node's log, which only the log's owner interprets.
//
// total is the number of nodes the stage validated and nodes the ones that did
// not pass, in any order. upstream holds the changed ancestors of the failed
// nodes, possibly repeating a node reached through several of them. An ancestor
// that did not pass is already counted above, so only the ones that passed are
// listed: those are the changes that broke their consumers without breaking
// their own validation.
func SummarizeValidationFailure(total int, nodes []FailedNode, upstream []ChangedAncestor) string {
	named, failed, skipped := firstFailures(nodes, maxNamedNodes)
	switch {
	case failed == 0 && skipped == 0:
		return "validation failed, but no per-node failure was recorded"
	case failed == 0:
		return fmt.Sprintf("validation failed, but no node reported a failure; %d %s %s because an upstream failed",
			skipped, plural(skipped, "node", "nodes"), plural(skipped, "was skipped", "were skipped"))
	}

	summary := fmt.Sprintf("%d of %d nodes failed validation: %s", failed, total, nameFirst(named, failed))
	if skipped > 0 {
		summary += fmt.Sprintf("; %d %s because an upstream failed", skipped, plural(skipped, "was skipped", "were skipped"))
	}
	if passing := nearestFirstExcept(upstream, nodes); len(passing) > 0 {
		summary += "; changed upstream of the failing nodes: " + nameNodes(passing, maxNamedUpstream)
	}
	return summary
}

// SummarizeCloneFailure is the operator-facing explanation of the unchanged
// upstreams whose production table validation could not copy: it names the
// first maxNamedNodes in id order and counts the rest. Running a node builds
// its production table, so the text asks for that. It is empty when no copy
// failed.
func SummarizeCloneFailure(cloned []string) string {
	if len(cloned) == 0 {
		return ""
	}
	ids := slices.Sorted(slices.Values(cloned))
	named := ids[:min(maxNamedNodes, len(ids))]
	list := strings.Join(named, ", ")
	if rest := len(ids) - len(named); rest > 0 {
		list = andMore(list, rest)
	}
	return fmt.Sprintf("Could not copy the production table of %s. Run %s, then release again.",
		list, plural(len(ids), "it", "them"))
}

// SummarizeSeedFailure is the operator-facing explanation of a seed-build
// rejection: which seeds did not build, and in which service.
func SummarizeSeedFailure(failing []FailedNode) string {
	if len(failing) == 0 {
		return "seed build failed, but no per-node failure was recorded"
	}
	return fmt.Sprintf("%d %s did not build: %s",
		len(failing), plural(len(failing), "seed", "seeds"), nameNodes(failing, maxNamedNodes))
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

// firstFailures walks nodes once and returns the first limit that failed, in
// their given order, with the count of nodes that failed and of nodes that were
// skipped. It copies only what it names, so a stage that fails for thousands of
// nodes costs no more than one that fails for five.
func firstFailures(nodes []FailedNode, limit int) (named []FailedNode, failed, skipped int) {
	named = make([]FailedNode, 0, min(limit, len(nodes)))
	for _, n := range nodes {
		if n.Skipped {
			skipped++
			continue
		}
		failed++
		if len(named) < limit {
			named = append(named, n)
		}
	}
	return named, failed, skipped
}

// nameNodes renders the first limit of nodes; see nameFirst.
func nameNodes(nodes []FailedNode, limit int) string {
	return nameFirst(nodes[:min(limit, len(nodes))], len(nodes))
}

// nameFirst renders "a (svc), b (svc) and c (svc)" for the nodes it is given,
// or, when they are only the first of total, "a (svc), …, e (svc) and 7 more".
func nameFirst(named []FailedNode, total int) string {
	labels := make([]string, len(named))
	for i, n := range named {
		labels[i] = n.label()
	}
	if rest := total - len(named); rest > 0 {
		return andMore(strings.Join(labels, ", "), rest)
	}
	return joinWithAnd(labels)
}

// andMore appends to a list of names the count of the names it leaves out:
// "a, b and 7 more".
func andMore(list string, rest int) string {
	return fmt.Sprintf("%s and %d more", list, rest)
}

func (n FailedNode) label() string {
	if n.Service == "" {
		return n.NodeID
	}
	return n.NodeID + " (" + n.Service + ")"
}

// plural picks the wording for a count: one for exactly 1, many otherwise.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
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
