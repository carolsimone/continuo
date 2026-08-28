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

func TestFullAncestorsClosure_CrossesServiceBoundaries(t *testing.T) {
	// svcA: a1 -> a2 -> a3; svcB: b1 is a CROSS-service upstream of a3 and MUST
	// be followed under self-contained validation.
	topo := release.Topology{
		{UniqueID: "a1", ServiceName: "svcA"},
		{UniqueID: "a2", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a1"}},
		{UniqueID: "a3", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a2", "b1"}},
		{UniqueID: "b1", ServiceName: "svcB"},
	}
	got := release.FullAncestorsClosure(topo, []string{"a3"})
	assert.ElementsMatch(t, []string{"a1", "a2", "a3", "b1"}, got)
	pos := map[string]int{}
	for i, id := range got {
		pos[id] = i
	}
	assert.Less(t, pos["a1"], pos["a2"])
	assert.Less(t, pos["a2"], pos["a3"])
	assert.Less(t, pos["b1"], pos["a3"])
}

func TestFullAncestorsClosure_SeedsNotInTopoIgnored(t *testing.T) {
	topo := release.Topology{{UniqueID: "a1", ServiceName: "svcA"}}
	got := release.FullAncestorsClosure(topo, []string{"ghost"})
	assert.Empty(t, got)
}

func TestFullAncestorsClosure_MultiSeedUnionDeduped(t *testing.T) {
	// svcA: a1 <- a2 <- a3, a1 <- a4
	// Seeds a3 and a4 share ancestor a1; the union must be deduplicated and
	// topologically ordered (upstreams before downstreams).
	topo := release.Topology{
		{UniqueID: "a1", ServiceName: "svcA"},
		{UniqueID: "a2", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a1"}},
		{UniqueID: "a3", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a2"}},
		{UniqueID: "a4", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a1"}},
	}
	got := release.FullAncestorsClosure(topo, []string{"a3", "a4"})
	// Expected: a1 first (root), then a2 and a4 (both depend only on a1),
	// then a3 (depends on a2). Ties between a2 and a4 break lexically.
	assert.Equal(t, []string{"a1", "a2", "a4", "a3"}, got)
}

func TestFullAncestorsClosure_CycleDetected_Panics(t *testing.T) {
	// a -> b -> a (cycle within same service)
	topo := release.Topology{
		withUpstream(release.Node{UniqueID: "a", ServiceName: "svcA"}, "b"),
		withUpstream(release.Node{UniqueID: "b", ServiceName: "svcA"}, "a"),
	}
	assert.Panics(t, func() {
		release.FullAncestorsClosure(topo, []string{"a"})
	}, "expected panic on cyclic topology")
}

func TestInSetUpstreams_IncludesCrossServiceWhenInSet(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "a1", ServiceName: "svcA"},
		{UniqueID: "a2", ServiceName: "svcA", UpstreamUniqueIDs: []string{"a1", "b1"}},
		{UniqueID: "b1", ServiceName: "svcB"},
	}
	got := release.InSetUpstreams(topo, "a2", map[string]bool{"a1": true, "a2": true, "b1": true})
	assert.Equal(t, []string{"a1", "b1"}, got)

	got = release.InSetUpstreams(topo, "a2", map[string]bool{"a1": true, "a2": true})
	assert.Equal(t, []string{"a1"}, got)
}

func TestUnbuildableCrossServiceUpstreams_FlagsDanglingEdge(t *testing.T) {
	candidate := release.Topology{
		{UniqueID: "a2", ServiceName: "svcA", UpstreamUniqueIDs: []string{"b_known", "b_ghost"}},
		{UniqueID: "b_known", ServiceName: "svcB"},
	}
	got := release.UnbuildableCrossServiceUpstreams(candidate, []string{"a2"})
	assert.Equal(t, []release.CrossServiceEdge{{Node: "a2", Upstream: "b_ghost"}}, got)
}

func TestUnbuildableCrossServiceUpstreams_NoneWhenAllInCandidate(t *testing.T) {
	candidate := release.Topology{
		{UniqueID: "a2", ServiceName: "svcA", UpstreamUniqueIDs: []string{"b1"}},
		{UniqueID: "b1", ServiceName: "svcB"},
	}
	assert.Empty(t, release.UnbuildableCrossServiceUpstreams(candidate, []string{"a2"}))
}

func TestChangedAncestors_FiltersTransitiveAncestorsToChangedOnes(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "s.u"},
		{UniqueID: "s.m", UpstreamUniqueIDs: []string{"s.u"}},
		{UniqueID: "s.x", UpstreamUniqueIDs: []string{"s.m"}},
		{UniqueID: "s.other"},
	}
	changed := map[string]bool{"s.u": true, "s.x": true, "s.other": true}

	got := release.ChangedAncestors(topo, "s.x", changed)

	want := []string{"s.u"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("want %v (m unchanged, x is itself, other unrelated), got %v", want, got)
	}
	if release.ChangedAncestors(topo, "s.unknown", changed) != nil {
		t.Fatal("unknown node must yield nil")
	}
}
