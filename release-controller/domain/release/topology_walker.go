package release

import (
	"fmt"
	"sort"
)

// DerivedChangedNodeIDs returns the unique_ids of candidate nodes whose dbt
// content_hash differs from the current prod topology — either because the node
// is new (absent from prod) or because its hash changed. Removed nodes (present
// in prod but absent from the candidate) are not returned: only candidate nodes
// can seed a validation run. Output follows candidate order for determinism.
//
// Bootstrap: an empty prod topology yields an empty hash map, so every
// candidate node is treated as new and returned — i.e. validate-all.
func DerivedChangedNodeIDs(candidate, prod Topology) []string {
	prodHash := make(map[string]string, len(prod))
	for _, n := range prod {
		prodHash[n.UniqueID] = n.ContentHash
	}
	changed := []string{}
	for _, n := range candidate {
		if h, ok := prodHash[n.UniqueID]; !ok || h != n.ContentHash {
			changed = append(changed, n.UniqueID)
		}
	}
	return changed
}

// DescendantsClosure returns the union of the seed nodes and all their
// transitive downstream descendants, deduplicated and sorted topologically
// (upstreams before downstreams). Nodes named in seeds but not present in
// the topology are silently ignored. Panics if the subgraph contains a cycle,
// which is a data-integrity violation (dbt itself rejects cyclic graphs at
// compile time).
func DescendantsClosure(topo Topology, seeds []string) []string {
	children := map[string][]string{}
	known := map[string]bool{}
	for _, n := range topo {
		known[n.UniqueID] = true
		for _, up := range n.UpstreamUniqueIDs {
			children[up] = append(children[up], n.UniqueID)
		}
	}

	included := map[string]bool{}
	for _, s := range seeds {
		if !known[s] {
			continue
		}
		stack := []string{s}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if included[cur] {
				continue
			}
			included[cur] = true
			stack = append(stack, children[cur]...)
		}
	}

	indegree := map[string]int{}
	for id := range included {
		indegree[id] = 0
	}
	for _, n := range topo {
		if !included[n.UniqueID] {
			continue
		}
		for _, up := range n.UpstreamUniqueIDs {
			if included[up] {
				indegree[n.UniqueID]++
			}
		}
	}
	queue := []string{}
	for id, d := range indegree {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)
	out := make([]string, 0, len(included))
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		out = append(out, cur)
		var ready []string
		for _, c := range children[cur] {
			if !included[c] {
				continue
			}
			indegree[c]--
			if indegree[c] == 0 {
				ready = append(ready, c)
			}
		}
		sort.Strings(ready)
		queue = append(queue, ready...)
	}
	if len(out) != len(included) {
		panic(fmt.Sprintf("topology_walker: cycle detected in topology; included=%d emitted=%d", len(included), len(out)))
	}
	return out
}

// AncestorsClosure returns the union of the seed nodes and all their
// transitive UPSTREAM ancestors that share the seed's service, deduplicated and
// sorted topologically (upstreams before downstreams). The walk crosses only
// intra-service edges: a cross-service upstream is not followed (cross-service
// refs are raw schema-qualified SQL that read the prod schema, so they need not
// be built into the candidate schema). Seeds absent from the topology are
// ignored. Panics on a cycle — a data-integrity violation dbt rejects at
// compile time.
func AncestorsClosure(topo Topology, seeds []string) []string {
	byID := make(map[string]Node, len(topo))
	parents := map[string][]string{}
	for _, n := range topo {
		byID[n.UniqueID] = n
	}
	for _, n := range topo {
		for _, up := range n.UpstreamUniqueIDs {
			upNode, ok := byID[up]
			if !ok || upNode.ServiceName != n.ServiceName {
				continue // cross-service or unknown edge: not an intra-service ancestor edge
			}
			parents[n.UniqueID] = append(parents[n.UniqueID], up)
		}
	}

	included := map[string]bool{}
	for _, s := range seeds {
		if _, ok := byID[s]; !ok {
			continue
		}
		stack := []string{s}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if included[cur] {
				continue
			}
			included[cur] = true
			stack = append(stack, parents[cur]...)
		}
	}

	return topoSortIncluded(byID, included)
}

// InSetIntraServiceUpstreams returns nodeID's upstreams that are BOTH in the
// build-set and the same service — the gating edges that must succeed before
// nodeID's candidate --empty run can resolve its ref()s. Cross-service edges
// and out-of-set edges are excluded. Output is lexically sorted for
// determinism.
func InSetIntraServiceUpstreams(topo Topology, nodeID string, set map[string]bool) []string {
	byID := make(map[string]Node, len(topo))
	for _, n := range topo {
		byID[n.UniqueID] = n
	}
	n, ok := byID[nodeID]
	if !ok {
		return nil
	}
	var out []string
	for _, up := range n.UpstreamUniqueIDs {
		upNode, known := byID[up]
		if !known || !set[up] || upNode.ServiceName != n.ServiceName {
			continue
		}
		out = append(out, up)
	}
	sort.Strings(out)
	return out
}

// CrossServiceEdge names a node and one of its cross-service upstreams.
type CrossServiceEdge struct {
	Node     string
	Upstream string
}

// NewCrossServiceUpstreams returns, for every changed node, each cross-service
// upstream that is absent from the prod topology — i.e. a NEW cross-service
// dependency. Such a release cannot be validated (the dependent's raw
// schema-qualified SQL reads the prod schema, where the upstream does not yet
// exist) and must be rejected early. Output is sorted (node, upstream) for
// determinism.
func NewCrossServiceUpstreams(candidate, prod Topology, changed []string) []CrossServiceEdge {
	byID := make(map[string]Node, len(candidate))
	for _, n := range candidate {
		byID[n.UniqueID] = n
	}
	inProd := make(map[string]bool, len(prod))
	for _, n := range prod {
		inProd[n.UniqueID] = true
	}
	var out []CrossServiceEdge
	for _, id := range changed {
		n, ok := byID[id]
		if !ok {
			continue
		}
		for _, up := range n.UpstreamUniqueIDs {
			upNode, known := byID[up]
			if !known || upNode.ServiceName == n.ServiceName {
				continue // intra-service edges are built into the candidate schema
			}
			if !inProd[up] {
				out = append(out, CrossServiceEdge{Node: id, Upstream: up})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Node != out[j].Node {
			return out[i].Node < out[j].Node
		}
		return out[i].Upstream < out[j].Upstream
	})
	return out
}

// topoSortIncluded returns the included node IDs in topological order
// (upstreams before downstreams), considering only intra-included edges.
// Ties break on lexical order for determinism. Panics on a cycle.
func topoSortIncluded(byID map[string]Node, included map[string]bool) []string {
	children := map[string][]string{}
	indegree := map[string]int{}
	for id := range included {
		indegree[id] = 0
	}
	for id := range included {
		for _, up := range byID[id].UpstreamUniqueIDs {
			if included[up] {
				indegree[id]++
				children[up] = append(children[up], id)
			}
		}
	}
	queue := []string{}
	for id, d := range indegree {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)
	out := make([]string, 0, len(included))
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		out = append(out, cur)
		var ready []string
		for _, c := range children[cur] {
			indegree[c]--
			if indegree[c] == 0 {
				ready = append(ready, c)
			}
		}
		sort.Strings(ready)
		queue = append(queue, ready...)
	}
	if len(out) != len(included) {
		panic(fmt.Sprintf("topology_walker: cycle detected; included=%d emitted=%d", len(included), len(out)))
	}
	return out
}
