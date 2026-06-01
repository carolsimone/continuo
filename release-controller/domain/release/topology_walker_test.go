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

func TestTopologyWalker_CycleDetected_Panics(t *testing.T) {
	// a -> b -> a (cycle)
	topo := release.Topology{
		withUpstream(release.Node{UniqueID: "a"}, "b"),
		withUpstream(release.Node{UniqueID: "b"}, "a"),
	}
	assert.Panics(t, func() {
		release.DescendantsClosure(topo, []string{"a"})
	}, "expected panic on cyclic topology")
}

func TestAncestorsClosure_IntraServiceOnly(t *testing.T) {
	// svcA: a1 -> a2 -> a3 (a3 depends on a2 depends on a1)
	// svcB: b1 is a cross-service upstream of a3 and must not be followed.
	topo := release.Topology{
		{UniqueID: "a1", ServiceName: "svcA"},
		{UniqueID: "a2", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a1"}},
		{UniqueID: "a3", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a2", "b1"}},
		{UniqueID: "b1", ServiceName: "svcB"},
	}
	got := release.AncestorsClosure(topo, []string{"a3"})
	// a3 + intra-service ancestors a2, a1; b1 excluded (cross-service).
	assert.Equal(t, []string{"a1", "a2", "a3"}, got)
}

func TestAncestorsClosure_SeedsNotInTopoIgnored(t *testing.T) {
	topo := release.Topology{{UniqueID: "a1", ServiceName: "svcA"}}
	got := release.AncestorsClosure(topo, []string{"ghost"})
	assert.Empty(t, got)
}

func TestAncestorsClosure_MultiSeedUnionDeduped(t *testing.T) {
	// svcA: a1 <- a2 <- a3, a1 <- a4
	// Seeds a3 and a4 share ancestor a1; the union must be deduplicated and
	// topologically ordered (upstreams before downstreams).
	topo := release.Topology{
		{UniqueID: "a1", ServiceName: "svcA"},
		{UniqueID: "a2", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a1"}},
		{UniqueID: "a3", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a2"}},
		{UniqueID: "a4", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a1"}},
	}
	got := release.AncestorsClosure(topo, []string{"a3", "a4"})
	// Expected: a1 first (root), then a2 and a4 (both depend only on a1),
	// then a3 (depends on a2). Ties between a2 and a4 break lexically.
	assert.Equal(t, []string{"a1", "a2", "a4", "a3"}, got)
}

func TestAncestorsClosure_CycleDetected_Panics(t *testing.T) {
	// a -> b -> a (cycle within same service)
	topo := release.Topology{
		withUpstream(release.Node{UniqueID: "a", ServiceName: "svcA"}, "b"),
		withUpstream(release.Node{UniqueID: "b", ServiceName: "svcA"}, "a"),
	}
	assert.Panics(t, func() {
		release.AncestorsClosure(topo, []string{"a"})
	}, "expected panic on cyclic topology")
}
