package release

import (
	"fmt"
	"sort"
)

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
