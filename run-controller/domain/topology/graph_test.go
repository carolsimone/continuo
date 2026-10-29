package topology_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gnode(key string) topology.Node {
	return topology.Node{Key: topology.NodeKey(key), NodeType: "dbt-model", TestCount: count(1)}
}

func gedge(upstream, downstream string) topology.Edge {
	return topology.Edge{Upstream: topology.NodeKey(upstream), Downstream: topology.NodeKey(downstream)}
}

func gkeys(keys ...string) []topology.NodeKey {
	out := make([]topology.NodeKey, 0, len(keys))
	for _, k := range keys {
		out = append(out, topology.NodeKey(k))
	}
	return out
}

// diamond: stg.a feeds stg.b and stg.c, which both feed stg.d.
func diamond() topology.Graph {
	return topology.NewGraph(
		[]topology.Node{gnode("stg.a"), gnode("stg.b"), gnode("stg.c"), gnode("stg.d")},
		[]topology.Edge{gedge("stg.a", "stg.b"), gedge("stg.a", "stg.c"), gedge("stg.b", "stg.d"), gedge("stg.c", "stg.d")},
	)
}

// layered builds the benchmark's synthetic DAG the way
// scripts/bench/gen_topology.py does: level k holds nodes named nKK_IIII, and
// each reads fanIn nodes of level k-1 (index, index+1, … modulo the level's
// size). With failRoot, level 0 is the single node fail_root and the other
// nodes fill the remaining levels.
func layered(nodes, levels, fanIn int, failRoot bool) ([]topology.Node, []topology.Edge) {
	var names [][]string
	for level, size := range levelSizes(nodes, levels, failRoot) {
		row := make([]string, 0, size)
		if level == 0 && failRoot {
			row = append(row, "fail_root")
		} else {
			for index := range size {
				row = append(row, fmt.Sprintf("n%02d_%04d", level, index))
			}
		}
		names = append(names, row)
	}
	var outNodes []topology.Node
	var outEdges []topology.Edge
	for level, row := range names {
		for index, table := range row {
			outNodes = append(outNodes, topology.Node{Key: topology.KeyOf("bench", table), NodeType: "dbt-model"})
			if level == 0 {
				continue
			}
			previous := names[level-1]
			for offset := range min(fanIn, len(previous)) {
				outEdges = append(outEdges, topology.Edge{
					Upstream:   topology.KeyOf("bench", previous[(index+offset)%len(previous)]),
					Downstream: topology.KeyOf("bench", table),
				})
			}
		}
	}
	return outNodes, outEdges
}

func levelSizes(nodes, levels int, failRoot bool) []int {
	if failRoot {
		return append([]int{1}, levelSizes(nodes-1, levels-1, false)...)
	}
	base, extra := nodes/levels, nodes%levels
	sizes := make([]int, levels)
	for i := range sizes {
		sizes[i] = base
		if i < extra {
			sizes[i]++
		}
	}
	return sizes
}

func TestGraph_ZeroValueHoldsNothing(t *testing.T) {
	var g topology.Graph
	assert.Equal(t, 0, g.Len())
	assert.Empty(t, g.Keys())
	_, ok := g.Node("stg.a")
	assert.False(t, ok)
	assert.Empty(t, g.Upstreams("stg.a"))
	assert.Empty(t, g.Descendants(gkeys("stg.a")))
	assert.Equal(t, 0, g.Restrict(gkeys("stg.a")).Len())
}

func TestNewGraph_KeysAndNeighboursAreSorted(t *testing.T) {
	g := diamond()
	assert.Equal(t, 4, g.Len())
	assert.Equal(t, gkeys("stg.a", "stg.b", "stg.c", "stg.d"), g.Keys())
	assert.Equal(t, gkeys("stg.b", "stg.c"), g.Downstreams("stg.a"))
	assert.Equal(t, gkeys("stg.b", "stg.c"), g.Upstreams("stg.d"))
	assert.Empty(t, g.Upstreams("stg.a"))
	assert.Empty(t, g.Downstreams("stg.d"))

	n, ok := g.Node("stg.b")
	require.True(t, ok)
	assert.Equal(t, gnode("stg.b"), n)
}

// An edge with an end outside the nodes is dropped and a repeated edge is kept
// once, so a slice of a release never names a node it does not hold.
func TestNewGraph_DropsEdgesLeavingTheNodesAndRepeatedEdges(t *testing.T) {
	g := topology.NewGraph(
		[]topology.Node{gnode("stg.a"), gnode("stg.b")},
		[]topology.Edge{gedge("stg.a", "stg.b"), gedge("stg.a", "stg.b"), gedge("raw.ghost", "stg.b"), gedge("stg.b", "mart.ghost")},
	)
	assert.Equal(t, gkeys("stg.a"), g.Upstreams("stg.b"))
	assert.Equal(t, gkeys("stg.b"), g.Downstreams("stg.a"))
	assert.Empty(t, g.Downstreams("stg.b"))
	assert.Empty(t, g.Upstreams("raw.ghost"))
}

func TestNewGraph_DoesNotDependOnInputOrder(t *testing.T) {
	nodes := []topology.Node{gnode("stg.d"), gnode("stg.c"), gnode("stg.b"), gnode("stg.a")}
	edges := []topology.Edge{gedge("stg.c", "stg.d"), gedge("stg.b", "stg.d"), gedge("stg.a", "stg.c"), gedge("stg.a", "stg.b")}
	g := topology.NewGraph(nodes, edges)
	want := diamond()
	assert.Equal(t, want.Keys(), g.Keys())
	for _, k := range want.Keys() {
		assert.Equal(t, want.Upstreams(k), g.Upstreams(k), "upstreams of %s", k)
		assert.Equal(t, want.Downstreams(k), g.Downstreams(k), "downstreams of %s", k)
	}
}

func TestNewGraph_KeepsTheFirstOfTwoNodesWithOneKey(t *testing.T) {
	first := gnode("stg.a")
	first.ServiceName = "svc-1"
	second := gnode("stg.a")
	second.ServiceName = "svc-2"
	g := topology.NewGraph([]topology.Node{first, second}, nil)
	assert.Equal(t, 1, g.Len())
	n, _ := g.Node("stg.a")
	assert.Equal(t, "svc-1", n.ServiceName)
}

// A Graph shares nothing with its inputs or its callers: changing an input
// slice, a returned slice or a returned node's test count leaves it as built.
func TestGraph_IsImmutable(t *testing.T) {
	nodes := []topology.Node{gnode("stg.a"), gnode("stg.b")}
	edges := []topology.Edge{gedge("stg.a", "stg.b")}
	g := topology.NewGraph(nodes, edges)

	*nodes[0].TestCount = 9
	nodes[1].Key = "stg.z"
	edges[0].Downstream = "stg.z"
	keys := g.Keys()
	keys[0] = "stg.z"
	ups := g.Upstreams("stg.b")
	ups[0] = "stg.z"
	n, _ := g.Node("stg.a")
	*n.TestCount = 7

	assert.Equal(t, gkeys("stg.a", "stg.b"), g.Keys())
	assert.Equal(t, gkeys("stg.a"), g.Upstreams("stg.b"))
	again, _ := g.Node("stg.a")
	require.NotNil(t, again.TestCount)
	assert.Equal(t, 1, *again.TestCount)
}

func TestGraph_DescendantsAndAncestorsOfTheDiamond(t *testing.T) {
	g := diamond()
	assert.Equal(t, gkeys("stg.b", "stg.c", "stg.d"), g.Descendants(gkeys("stg.a")))
	assert.Equal(t, gkeys("stg.a", "stg.b", "stg.c"), g.Ancestors(gkeys("stg.d")))
	assert.Equal(t, gkeys("stg.d"), g.Descendants(gkeys("stg.b")))
	assert.Equal(t, gkeys("stg.a"), g.Ancestors(gkeys("stg.c")))
	assert.Empty(t, g.Descendants(gkeys("stg.d")))
	assert.Empty(t, g.Ancestors(gkeys("stg.a")))
}

// A from key is in the result only when another from key reaches it; a key
// the graph does not hold reaches nothing.
func TestGraph_WalksIncludeAFromKeyOnlyWhenReached(t *testing.T) {
	g := topology.NewGraph(
		[]topology.Node{gnode("stg.a"), gnode("stg.b"), gnode("stg.c")},
		[]topology.Edge{gedge("stg.a", "stg.b"), gedge("stg.b", "stg.c")},
	)
	assert.Equal(t, gkeys("stg.c"), g.Descendants(gkeys("stg.b")))
	assert.Equal(t, gkeys("stg.b", "stg.c"), g.Descendants(gkeys("stg.b", "stg.a")))
	assert.Equal(t, gkeys("stg.a", "stg.b"), g.Ancestors(gkeys("stg.c", "stg.b")))
	assert.Empty(t, g.Descendants(gkeys("raw.ghost")))
	assert.Empty(t, g.Descendants(nil))
}

// A release is a DAG, but a walk must still end on a cycle: each node is
// expanded once.
func TestGraph_WalksStopOnACycle(t *testing.T) {
	g := topology.NewGraph(
		[]topology.Node{gnode("stg.a"), gnode("stg.b"), gnode("stg.c"), gnode("stg.d")},
		[]topology.Edge{gedge("stg.a", "stg.b"), gedge("stg.b", "stg.c"), gedge("stg.c", "stg.a"), gedge("stg.c", "stg.d")},
	)
	assert.Equal(t, gkeys("stg.a", "stg.b", "stg.c", "stg.d"), g.Descendants(gkeys("stg.a")))
	assert.Equal(t, gkeys("stg.a", "stg.b", "stg.c"), g.Ancestors(gkeys("stg.d")))
}

// The cascade-2000 benchmark shape: one root, 1,999 nodes in 19 levels below
// it, fan-in 2. Every other node descends from the root.
func TestGraph_DescendantsOfTheCascadeRootAreEveryOtherNode(t *testing.T) {
	nodes, edges := layered(2000, 20, 2, true)
	g := topology.NewGraph(nodes, edges)
	require.Equal(t, 2000, g.Len())
	root := topology.KeyOf("bench", "fail_root")

	got := g.Descendants([]topology.NodeKey{root})
	assert.Len(t, got, 1999)
	assert.NotContains(t, got, root)
	assert.True(t, slices.IsSorted(got))
	assert.Len(t, g.Descendants(g.Keys()), 1999, "every node but the root reads another")
}

// The dag-2000 benchmark shape: 20 levels of 100, fan-in 2. A level-19 node
// reads d+1 nodes d levels up, so it has 2+3+…+20 = 209 ancestors.
func TestGraph_AncestorsInTheLayeredDAG(t *testing.T) {
	nodes, edges := layered(2000, 20, 2, false)
	g := topology.NewGraph(nodes, edges)
	require.Equal(t, 2000, g.Len())

	got := g.Ancestors([]topology.NodeKey{topology.KeyOf("bench", "n19_0000")})
	assert.Len(t, got, 209)
	assert.Contains(t, got, topology.KeyOf("bench", "n00_0019"))
	assert.NotContains(t, got, topology.KeyOf("bench", "n00_0020"))

	var level0 []topology.NodeKey
	for i := range 100 {
		level0 = append(level0, topology.KeyOf("bench", fmt.Sprintf("n00_%04d", i)))
	}
	assert.Len(t, g.Descendants(level0), 1900)
}

// 64 levels of two nodes, each reading both nodes of the level above, join the
// top and the bottom by 2^63 paths. A walk that followed paths would not end
// before the test binary's timeout; one that visits nodes ends at once.
func TestGraph_WalksVisitNodesNotPaths(t *testing.T) {
	nodes, edges := layered(128, 64, 2, false)
	g := topology.NewGraph(nodes, edges)
	top := []topology.NodeKey{topology.KeyOf("bench", "n00_0000"), topology.KeyOf("bench", "n00_0001")}
	bottom := []topology.NodeKey{topology.KeyOf("bench", "n63_0000"), topology.KeyOf("bench", "n63_0001")}

	assert.Len(t, g.Descendants(top), 126)
	assert.Len(t, g.Ancestors(bottom), 126)
}

func TestGraph_RestrictKeepsTheEdgesAmongTheKeys(t *testing.T) {
	r := diamond().Restrict(gkeys("stg.d", "stg.a", "stg.b", "stg.b", "raw.ghost"))
	assert.Equal(t, gkeys("stg.a", "stg.b", "stg.d"), r.Keys())
	assert.Equal(t, gkeys("stg.b"), r.Upstreams("stg.d"))
	assert.Equal(t, gkeys("stg.b"), r.Downstreams("stg.a"))
	assert.Equal(t, gkeys("stg.b", "stg.d"), r.Descendants(gkeys("stg.a")))
	_, ok := r.Node("stg.c")
	assert.False(t, ok)
	n, ok := r.Node("stg.a")
	require.True(t, ok)
	assert.Equal(t, gnode("stg.a"), n)
}
