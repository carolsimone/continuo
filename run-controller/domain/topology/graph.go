package topology

import "slices"

// Graph is an immutable slice of one release: some of its nodes and the edges
// whose both ends are among them. Every list it returns is sorted ascending in
// byte order and is the caller's to keep; the zero Graph holds no node.
type Graph struct {
	nodes       map[NodeKey]Node
	keys        []NodeKey
	upstreams   map[NodeKey][]NodeKey
	downstreams map[NodeKey][]NodeKey
}

// NewGraph builds the Graph of nodes and the edges among them. An edge whose
// upstream or downstream is not among nodes is dropped, and a repeated edge is
// kept once. When two nodes share a key the first is kept; apart from that the
// Graph does not depend on the order of either slice. Neither slice is
// retained.
func NewGraph(nodes []Node, edges []Edge) Graph {
	g := Graph{
		nodes:       make(map[NodeKey]Node, len(nodes)),
		keys:        make([]NodeKey, 0, len(nodes)),
		upstreams:   make(map[NodeKey][]NodeKey),
		downstreams: make(map[NodeKey][]NodeKey),
	}
	for _, n := range nodes {
		if _, taken := g.nodes[n.Key]; taken {
			continue
		}
		n.TestCount = cloneCount(n.TestCount)
		g.nodes[n.Key] = n
		g.keys = append(g.keys, n.Key)
	}
	slices.Sort(g.keys)

	seen := make(map[Edge]bool, len(edges))
	for _, e := range edges {
		if seen[e] || !g.has(e.Upstream) || !g.has(e.Downstream) {
			continue
		}
		seen[e] = true
		g.upstreams[e.Downstream] = append(g.upstreams[e.Downstream], e.Upstream)
		g.downstreams[e.Upstream] = append(g.downstreams[e.Upstream], e.Downstream)
	}
	for _, ks := range g.upstreams {
		slices.Sort(ks)
	}
	for _, ks := range g.downstreams {
		slices.Sort(ks)
	}
	return g
}

// Len returns the number of nodes in g.
func (g Graph) Len() int { return len(g.keys) }

// Node returns the node of g with key k, and false when g does not hold it.
func (g Graph) Node(k NodeKey) (Node, bool) {
	n, ok := g.nodes[k]
	if !ok {
		return Node{}, false
	}
	n.TestCount = cloneCount(n.TestCount)
	return n, true
}

// Keys returns the key of every node in g.
func (g Graph) Keys() []NodeKey { return slices.Clone(g.keys) }

// Upstreams returns the nodes of g that k reads.
func (g Graph) Upstreams(k NodeKey) []NodeKey { return slices.Clone(g.upstreams[k]) }

// Downstreams returns the nodes of g that read k.
func (g Graph) Downstreams(k NodeKey) []NodeKey { return slices.Clone(g.downstreams[k]) }

// Descendants returns every node of g reached from a node in from by following
// one or more edges downstream. A key of from is in the result only when it is
// reached that way from a key of from, and a key g does not hold is ignored.
func (g Graph) Descendants(from []NodeKey) []NodeKey { return reachable(from, g.downstreams) }

// Ancestors returns every node of g reached from a node in from by following
// one or more edges upstream, with the same convention as Descendants.
func (g Graph) Ancestors(from []NodeKey) []NodeKey { return reachable(from, g.upstreams) }

// Restrict returns the Graph of the nodes of g whose key is in keys and the
// edges of g among them. A key g does not hold is ignored.
func (g Graph) Restrict(keys []NodeKey) Graph {
	nodes := make([]Node, 0, len(keys))
	var edges []Edge
	for _, k := range keys {
		n, ok := g.nodes[k]
		if !ok {
			continue
		}
		nodes = append(nodes, n)
		for _, d := range g.downstreams[k] {
			edges = append(edges, Edge{Upstream: k, Downstream: d})
		}
	}
	return NewGraph(nodes, edges)
}

func (g Graph) has(k NodeKey) bool {
	_, ok := g.nodes[k]
	return ok
}

// reachable returns every key reached from a key of from by following next
// one or more times, sorted. Each key is expanded once, so a walk costs one
// step per edge however many paths join two nodes, and a cycle ends it.
func reachable(from []NodeKey, next map[NodeKey][]NodeKey) []NodeKey {
	visited := make(map[NodeKey]bool)
	var stack []NodeKey
	for _, k := range from {
		stack = append(stack, next[k]...)
	}
	for len(stack) > 0 {
		k := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[k] {
			continue
		}
		visited[k] = true
		stack = append(stack, next[k]...)
	}
	out := make([]NodeKey, 0, len(visited))
	for k := range visited {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
