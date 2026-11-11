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
		{UniqueID: "s.u", ServiceName: "svcU", OriginalFilePath: "models/u.sql"},
		{UniqueID: "s.m", UpstreamUniqueIDs: []string{"s.u"}},
		{UniqueID: "s.x", UpstreamUniqueIDs: []string{"s.m"}},
		{UniqueID: "s.other"},
	}
	changed := map[string]bool{"s.u": true, "s.x": true, "s.other": true}

	got := release.ChangedAncestors(topo, "s.x", changed)

	want := []release.ChangedAncestor{{NodeID: "s.u", FilePath: "models/u.sql", Service: "svcU", Depth: 2}}
	assert.Equal(t, want, got, "m unchanged, x is itself, other unrelated; u is two hops up through m")
	assert.Nil(t, release.ChangedAncestors(topo, "s.unknown", changed), "unknown node must yield nil")
}

// TestChangedAncestors_CarriesTheLocationTheCandidateDeclares pins why the
// ancestors travel as objects rather than ids: the fix has to edit the file
// THIS release declares for the ancestor. A node moved or renamed in the
// candidate is at its old path in the promoted graph, so an id alone would send
// the fix to a file that no longer holds the node.
func TestChangedAncestors_CarriesTheLocationTheCandidateDeclares(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "s.u", ServiceName: "svcB", OriginalFilePath: "models/marts/u_renamed.sql"},
		{UniqueID: "s.x", ServiceName: "svcA", UpstreamUniqueIDs: []string{"s.u"}},
	}

	got := release.ChangedAncestors(topo, "s.x", map[string]bool{"s.u": true})

	assert.Equal(t, []release.ChangedAncestor{
		{NodeID: "s.u", FilePath: "models/marts/u_renamed.sql", Service: "svcB", Depth: 1},
	}, got)
}

// TestChangedAncestors_DepthIsTheMinimumHopCount pins depth on a diamond: u
// is reachable from x in one hop (direct) and in two (through m), and depth
// reports the shorter path; a farther changed ancestor reports its own hops.
func TestChangedAncestors_DepthIsTheMinimumHopCount(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "s.far", ServiceName: "svcB", OriginalFilePath: "models/far.sql"},
		{UniqueID: "s.u", ServiceName: "svcB", OriginalFilePath: "models/u.sql", UpstreamUniqueIDs: []string{"s.far"}},
		{UniqueID: "s.m", ServiceName: "svcA", UpstreamUniqueIDs: []string{"s.u"}},
		{UniqueID: "s.x", ServiceName: "svcA", UpstreamUniqueIDs: []string{"s.u", "s.m"}},
	}
	changed := map[string]bool{"s.u": true, "s.far": true}

	got := release.ChangedAncestors(topo, "s.x", changed)

	assert.Equal(t, []release.ChangedAncestor{
		{NodeID: "s.far", FilePath: "models/far.sql", Service: "svcB", Depth: 2},
		{NodeID: "s.u", FilePath: "models/u.sql", Service: "svcB", Depth: 1},
	}, got)
}

// ingestedProbe is the node topology-controller writes for a table that a
// reader names and no node builds: no content, no service, no upstreams.
func ingestedProbe() release.Node {
	return release.Node{
		UniqueID: "raw.ingest_probe", SchemaName: "raw", TableName: "ingest_probe",
		ResolvedRelationID: "raw.ingest_probe", NodeType: "ingested",
		UpstreamUniqueIDs: []string{},
	}
}

// TestDerivedChangedNodeIDs_NeverReturnsAnIngestedNode pins that an ingested
// node is never "changed": it has no content, so it never seeds a validation
// build whether it is new or already in production.
func TestDerivedChangedNodeIDs_NeverReturnsAnIngestedNode(t *testing.T) {
	cand := release.Topology{
		ingestedProbe(),
		{UniqueID: "a.m", NodeType: "dbt-model", ContentHash: "h", UpstreamUniqueIDs: []string{"raw.ingest_probe"}},
	}

	assert.Equal(t, []string{"a.m"}, release.DerivedChangedNodeIDs(cand, nil), "a new ingested node is not a seed")
	assert.Empty(t, release.DerivedChangedNodeIDs(cand, release.Topology{{UniqueID: "a.m", ContentHash: "h"}}),
		"a new ingested node does not change its unchanged reader")
	assert.Empty(t, release.DerivedChangedNodeIDs(cand, release.Topology{ingestedProbe(), {UniqueID: "a.m", ContentHash: "h"}}),
		"an ingested node already in production is not a seed either")
}

// TestDerivedChangedNodeIDs_ATableANodeStartsBuildingIsChanged pins the other
// direction: when a node starts to build a table that production holds as an
// ingested node, the candidate node is a real node with content and is changed.
func TestDerivedChangedNodeIDs_ATableANodeStartsBuildingIsChanged(t *testing.T) {
	prod := release.Topology{ingestedProbe()}
	cand := release.Topology{{UniqueID: "raw.ingest_probe", NodeType: "dbt-model", ServiceName: "svc", ContentHash: "h"}}

	assert.Equal(t, []string{"raw.ingest_probe"}, release.DerivedChangedNodeIDs(cand, prod))
}

// TestDerivedChangedNodeIDs_ATableANodeStopsBuildingIsNotChanged pins the
// reverse direction: when production builds a table and the candidate holds it
// as an ingested node, the candidate node has no content to validate, so it is
// not changed even though its content hash differs from production's.
func TestDerivedChangedNodeIDs_ATableANodeStopsBuildingIsNotChanged(t *testing.T) {
	prod := release.Topology{{UniqueID: "raw.x", NodeType: "dbt-model", ServiceName: "svc", ContentHash: "h"}}
	cand := release.Topology{{UniqueID: "raw.x", NodeType: "ingested", SchemaName: "raw", TableName: "x", ResolvedRelationID: "raw.x"}}

	assert.Empty(t, release.DerivedChangedNodeIDs(cand, prod))
}

// TestFullAncestorsClosure_StopsAtAnIngestedNode pins that an ingested
// ancestor is never part of a build closure, so it is never cloned or built.
func TestFullAncestorsClosure_StopsAtAnIngestedNode(t *testing.T) {
	topo := release.Topology{
		ingestedProbe(),
		{UniqueID: "a.m", NodeType: "dbt-model", UpstreamUniqueIDs: []string{"raw.ingest_probe"}},
	}

	assert.Equal(t, []string{"a.m"}, release.FullAncestorsClosure(topo, []string{"a.m"}))
	assert.Empty(t, release.FullAncestorsClosure(topo, []string{"raw.ingest_probe"}),
		"an ingested seed yields nothing to build")
}

// TestFullAncestorsClosure_ReachesAnAncestorBeyondTheIngestedEdge pins that
// only the ingested edge is dropped: the real ancestors of the same node are
// still followed.
func TestFullAncestorsClosure_ReachesAnAncestorBeyondTheIngestedEdge(t *testing.T) {
	topo := release.Topology{
		ingestedProbe(),
		{UniqueID: "a.up", NodeType: "dbt-model"},
		{UniqueID: "a.m", NodeType: "dbt-model", UpstreamUniqueIDs: []string{"raw.ingest_probe", "a.up"}},
	}

	assert.Equal(t, []string{"a.up", "a.m"}, release.FullAncestorsClosure(topo, []string{"a.m"}))
}

// TestChangedAncestors_NeverNamesAnIngestedNode pins that a failing node below
// a reader of an ingested table names only the changed reader as the place to
// look: the changed set comes from DerivedChangedNodeIDs, which never holds an
// ingested node, so no ingested id is offered as a fix target.
func TestChangedAncestors_NeverNamesAnIngestedNode(t *testing.T) {
	cand := release.Topology{
		ingestedProbe(),
		{UniqueID: "a.m", NodeType: "dbt-model", ServiceName: "svcA", OriginalFilePath: "models/m.sql", ContentHash: "h2", UpstreamUniqueIDs: []string{"raw.ingest_probe"}},
		{UniqueID: "a.x", NodeType: "dbt-model", ServiceName: "svcA", ContentHash: "hx", UpstreamUniqueIDs: []string{"a.m"}},
	}
	changed := map[string]bool{}
	for _, id := range release.DerivedChangedNodeIDs(cand, nil) {
		changed[id] = true
	}

	got := release.ChangedAncestors(cand, "a.x", changed)

	assert.Equal(t, []release.ChangedAncestor{
		{NodeID: "a.m", FilePath: "models/m.sql", Service: "svcA", Depth: 1},
	}, got, "only the reader a.m: the ingested node raw.ingest_probe sits two hops up and is never changed")
	for _, a := range got {
		assert.NotEqual(t, "raw.ingest_probe", a.NodeID)
	}
}

// TestUnbuildableCrossServiceUpstreams_AnIngestedUpstreamIsKnown pins that an
// edge to an ingested node is not a dangling reference: the node is in the
// candidate topology even though it is never built.
func TestUnbuildableCrossServiceUpstreams_AnIngestedUpstreamIsKnown(t *testing.T) {
	topo := release.Topology{
		ingestedProbe(),
		{UniqueID: "a.m", NodeType: "dbt-model", UpstreamUniqueIDs: []string{"raw.ingest_probe"}},
	}

	assert.Empty(t, release.UnbuildableCrossServiceUpstreams(topo, []string{"a.m"}))
}
