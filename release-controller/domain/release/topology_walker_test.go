package release_test

import (
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
)

func withUpstream(n release.Node, ups ...string) release.Node {
	n.UpstreamUniqueIDs = ups
	return n
}

func TestTopologyWalker_LinearChain(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "a"},
		withUpstream(release.Node{UniqueID: "b"}, "a"),
		withUpstream(release.Node{UniqueID: "c"}, "b"),
	}
	got := release.DescendantsClosure(topo, []string{"a"})
	assert.ElementsMatch(t, []string{"a", "b", "c"}, got)
}

func TestTopologyWalker_DiamondNoDuplicates(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "a"},
		withUpstream(release.Node{UniqueID: "b"}, "a"),
		withUpstream(release.Node{UniqueID: "c"}, "a"),
		withUpstream(release.Node{UniqueID: "d"}, "b", "c"),
	}
	got := release.DescendantsClosure(topo, []string{"a"})
	assert.ElementsMatch(t, []string{"a", "b", "c", "d"}, got)
}

func TestTopologyWalker_TwoChangedNodesShareDescendant(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "a"},
		{UniqueID: "b"},
		withUpstream(release.Node{UniqueID: "c"}, "a", "b"),
	}
	got := release.DescendantsClosure(topo, []string{"a", "b"})
	assert.ElementsMatch(t, []string{"a", "b", "c"}, got)
}

func TestTopologyWalker_LeafChange(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "a"},
		withUpstream(release.Node{UniqueID: "b"}, "a"),
	}
	got := release.DescendantsClosure(topo, []string{"b"})
	assert.ElementsMatch(t, []string{"b"}, got)
}

func TestTopologyWalker_DisconnectedNodesIgnored(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "a"},
		withUpstream(release.Node{UniqueID: "b"}, "a"),
		{UniqueID: "x"},
	}
	got := release.DescendantsClosure(topo, []string{"a"})
	assert.ElementsMatch(t, []string{"a", "b"}, got)
	assert.NotContains(t, got, "x")
}

func TestTopologyWalker_UnknownChangedNodeIgnored(t *testing.T) {
	topo := release.Topology{{UniqueID: "a"}}
	got := release.DescendantsClosure(topo, []string{"unknown"})
	assert.Empty(t, got)
}

func TestTopologyWalker_TopologicalOrder(t *testing.T) {
	topo := release.Topology{
		withUpstream(release.Node{UniqueID: "c"}, "b"),
		withUpstream(release.Node{UniqueID: "b"}, "a"),
		{UniqueID: "a"},
	}
	got := release.DescendantsClosure(topo, []string{"a"})
	pos := map[string]int{}
	for i, id := range got {
		pos[id] = i
	}
	assert.Less(t, pos["a"], pos["b"])
	assert.Less(t, pos["b"], pos["c"])
}
