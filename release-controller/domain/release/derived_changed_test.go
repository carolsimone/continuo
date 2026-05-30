package release_test

import (
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
)

func TestDerivedChangedNodeIDs_UnchangedHashExcluded(t *testing.T) {
	prod := release.Topology{{UniqueID: "a", ContentHash: "h1"}}
	cand := release.Topology{{UniqueID: "a", ContentHash: "h1"}}
	assert.Empty(t, release.DerivedChangedNodeIDs(cand, prod))
}

func TestDerivedChangedNodeIDs_ChangedHashIncluded(t *testing.T) {
	prod := release.Topology{{UniqueID: "a", ContentHash: "h1"}}
	cand := release.Topology{{UniqueID: "a", ContentHash: "h2"}}
	assert.Equal(t, []string{"a"}, release.DerivedChangedNodeIDs(cand, prod))
}

func TestDerivedChangedNodeIDs_NewNodeIncluded(t *testing.T) {
	prod := release.Topology{{UniqueID: "a", ContentHash: "h1"}}
	cand := release.Topology{
		{UniqueID: "a", ContentHash: "h1"},
		{UniqueID: "b", ContentHash: "h2"},
	}
	assert.Equal(t, []string{"b"}, release.DerivedChangedNodeIDs(cand, prod))
}

// A node present in prod but absent from the candidate (removed) must not be
// reported as a seed: only candidate nodes can seed a validation run.
func TestDerivedChangedNodeIDs_RemovedNodeNotASeed(t *testing.T) {
	prod := release.Topology{
		{UniqueID: "a", ContentHash: "h1"},
		{UniqueID: "gone", ContentHash: "h9"},
	}
	cand := release.Topology{{UniqueID: "a", ContentHash: "h1"}}
	assert.Empty(t, release.DerivedChangedNodeIDs(cand, prod))
}

// Bootstrap: empty prod topology -> every candidate node is new -> all returned.
func TestDerivedChangedNodeIDs_EmptyProdValidatesAll(t *testing.T) {
	cand := release.Topology{
		{UniqueID: "a", ContentHash: "h1"},
		{UniqueID: "b", ContentHash: "h2"},
	}
	assert.Equal(t, []string{"a", "b"}, release.DerivedChangedNodeIDs(cand, nil))
}

// A node whose hash equals prod's is skipped even when both hashes are empty.
// Today this is unreachable (dbt always assigns a checksum to model/seed/snapshot,
// the only resource types the parser admits), but pinning it surfaces the risk if
// SUPPORTED_RESOURCE_TYPES ever admits a checksum-less resource: such a node would
// silently skip validation, and the diff would then need to treat "" as changed.
func TestDerivedChangedNodeIDs_BothEmptyHashTreatedAsUnchanged(t *testing.T) {
	prod := release.Topology{{UniqueID: "a", ContentHash: ""}}
	cand := release.Topology{{UniqueID: "a", ContentHash: ""}}
	assert.Empty(t, release.DerivedChangedNodeIDs(cand, prod))
}

// Output order follows candidate order for determinism.
func TestDerivedChangedNodeIDs_DeterministicCandidateOrder(t *testing.T) {
	prod := release.Topology{}
	cand := release.Topology{
		{UniqueID: "z", ContentHash: "1"},
		{UniqueID: "m", ContentHash: "2"},
		{UniqueID: "a", ContentHash: "3"},
	}
	got := release.DerivedChangedNodeIDs(cand, prod)
	assert.Equal(t, []string{"z", "m", "a"}, got)
}
