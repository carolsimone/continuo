package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prodSnapshot is a release id and the production topology it names.
type prodSnapshot struct {
	releaseID string
	topo      release.Topology
}

func buildProdSnapshot(releaseID string, nodes ...release.Node) prodSnapshot {
	return prodSnapshot{releaseID: releaseID, topo: release.Topology(nodes)}
}

func TestSeedProductionPointers_SeedsAllProjects(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// Snapshot with two projects: service-1 (2 nodes, tag t1) and service-2 (1 node, tag t2).
	snap := buildProdSnapshot("rel-1",
		release.Node{UniqueID: "n1", ProjectName: "service-1", ImageTag: "t1"},
		release.Node{UniqueID: "n2", ProjectName: "service-1", ImageTag: "t1-dup"}, // second node; first tag wins
		release.Node{UniqueID: "n3", ProjectName: "service-2", ImageTag: "t2"},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
		"service-2": "s3://bucket/service-2/manifest.json",
	}

	store := newFakeStore()
	repo := &fakeProductionPointerRepo{store: store}

	n, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	pointer1, err := repo.Get(context.Background(), "service-1")
	require.NoError(t, err)
	require.NotNil(t, pointer1)
	assert.Equal(t, "service-1", pointer1.ProjectName())
	assert.Equal(t, "rel-1", pointer1.ReleaseID())
	assert.Equal(t, "s3://bucket/service-1/manifest.json", pointer1.ManifestS3Key())
	assert.Equal(t, "t1", pointer1.ImageTag(), "first non-empty image tag among project nodes")
	assert.Equal(t, now, pointer1.UpdatedAt())

	pointer2, err := repo.Get(context.Background(), "service-2")
	require.NoError(t, err)
	require.NotNil(t, pointer2)
	assert.Equal(t, "t2", pointer2.ImageTag())
	assert.Equal(t, "s3://bucket/service-2/manifest.json", pointer2.ManifestS3Key())
}

func TestSeedProductionPointers_MissingKeyReturnsError(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	snap := buildProdSnapshot("rel-1",
		release.Node{UniqueID: "n1", ProjectName: "service-1", ImageTag: "t1"},
		release.Node{UniqueID: "n2", ProjectName: "service-2", ImageTag: "t2"},
	)
	// existingKeys only covers service-1; service-2 is missing.
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
	}

	store := newFakeStore()
	repo := &fakeProductionPointerRepo{store: store}

	_, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, existingKeys, repo, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service-2")
}

func TestSeedProductionPointers_SkipsNodesWithEmptyProjectName(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	snap := buildProdSnapshot("rel-1",
		release.Node{UniqueID: "n1", ProjectName: "", ImageTag: "t1"}, // no project name — skipped
		release.Node{UniqueID: "n2", ProjectName: "service-1", ImageTag: "t1"},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
	}

	store := newFakeStore()
	repo := &fakeProductionPointerRepo{store: store}

	n, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "node with empty project_name must be skipped")
}

// An unknown-external node belongs to no project, so it never gets a production
// pointer and never asks for a manifest key.
func TestSeedProductionPointers_SkipsAnUnknownExternalNode(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	snap := buildProdSnapshot("rel-1",
		release.Node{UniqueID: "raw.orders", SchemaName: "raw", TableName: "orders", NodeType: "unknown-external"},
		release.Node{UniqueID: "n2", ProjectName: "service-1", ImageTag: "t1", NodeType: "dbt-model", UpstreamUniqueIDs: []string{"raw.orders"}},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
	}

	store := newFakeStore()
	repo := &fakeProductionPointerRepo{store: store}

	n, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only service-1 is a project")
	pointers, err := repo.List(context.Background())
	require.NoError(t, err)
	require.Len(t, pointers, 1, "no pointer is written for the unknown-external node")
	assert.Equal(t, "service-1", pointers[0].ProjectName())
}

func TestSeedProductionPointers_IdempotentOnRerun(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	snap := buildProdSnapshot("rel-1",
		release.Node{UniqueID: "n1", ProjectName: "service-1", ImageTag: "t1"},
	)
	existingKeys := map[string]string{"service-1": "s3://bucket/service-1/manifest.json"}

	store := newFakeStore()
	repo := &fakeProductionPointerRepo{store: store}

	n1, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 1, n1)

	// Running again should succeed and still report 1 project.
	n2, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 1, n2)

	pointer, _ := repo.Get(context.Background(), "service-1")
	require.NotNil(t, pointer)
	assert.Equal(t, "rel-1", pointer.ReleaseID(), "second check must not change the pointer")
}

func TestSeedProductionPointers_EmptyTopologyReturnsZero(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	snap := buildProdSnapshot("rel-1") // no nodes
	store := newFakeStore()
	repo := &fakeProductionPointerRepo{store: store}

	n, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, map[string]string{}, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestSeedProductionPointers_DerivesManifestKindPerProject(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// service-1 is a dbt project; service-2 is a python project.
	snap := buildProdSnapshot("rel-1",
		release.Node{UniqueID: "n1", ProjectName: "service-1", ImageTag: "t1", NodeType: "dbt-model"},
		release.Node{UniqueID: "n2", ProjectName: "service-2", ImageTag: "t2", NodeType: "python-node"},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
		"service-2": "s3://bucket/service-2/rel-1/odcs-bundle.json",
	}

	store := newFakeStore()
	repo := &fakeProductionPointerRepo{store: store}

	n, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	pointer1, err := repo.Get(context.Background(), "service-1")
	require.NoError(t, err)
	require.NotNil(t, pointer1)
	assert.Equal(t, release.ManifestKindDbt, pointer1.ManifestKind())

	pointer2, err := repo.Get(context.Background(), "service-2")
	require.NoError(t, err)
	require.NotNil(t, pointer2)
	assert.Equal(t, release.ManifestKindPython, pointer2.ManifestKind())
}

func TestSeedProductionPointers_CsvProjectClassifiedPython(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// service-1 has only python-csv nodes: it must classify as python, same
	// as a project made only of python-nodes.
	snap := buildProdSnapshot("rel-1",
		release.Node{UniqueID: "n1", ProjectName: "service-1", ImageTag: "t1", NodeType: "python-csv"},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/rel-1/odcs-bundle.json",
	}

	store := newFakeStore()
	repo := &fakeProductionPointerRepo{store: store}

	n, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	pointer, err := repo.Get(context.Background(), "service-1")
	require.NoError(t, err)
	require.NotNil(t, pointer)
	assert.Equal(t, release.ManifestKindPython, pointer.ManifestKind())
}

// A python project's pointer must name an odcs-bundle.json: the only artifact
// any parser reads for a python project. One that names anything else (a
// contract.yaml) is an error naming the project, and nothing is written,
// including the pointer of the clean project beside it.
func TestSeedProductionPointers_AContractYAMLKeyIsAnError(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	snap := buildProdSnapshot("rel-1",
		release.Node{UniqueID: "n1", ProjectName: "service-odcs", ImageTag: "t1", NodeType: "python-node"},
		release.Node{UniqueID: "n2", ProjectName: "service-legacy", ImageTag: "t2", NodeType: "python-node"},
	)
	existingKeys := map[string]string{
		"service-odcs":   "s3://bucket/service-odcs/rel-1/odcs-bundle.json",
		"service-legacy": "s3://bucket/service-legacy/rel-1/contract.yaml",
	}

	store := newFakeStore()
	repo := &fakeProductionPointerRepo{store: store}

	n, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, existingKeys, repo, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service-legacy")
	assert.Contains(t, err.Error(), "odcs-bundle.json")
	assert.NotContains(t, err.Error(), "service-odcs")
	assert.Equal(t, 0, n)

	odcs, err := repo.Get(context.Background(), "service-odcs")
	require.NoError(t, err)
	assert.Nil(t, odcs, "no project must be written when one pointer is refused")
	legacy, err := repo.Get(context.Background(), "service-legacy")
	require.NoError(t, err)
	assert.Nil(t, legacy)
}

func TestSeedProductionPointers_MixedKindProjectErrorsAndWritesNothing(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// service-1 mixes a dbt node and a python node — this must be rejected
	// before anything is written, including the clean service-2 pointer.
	snap := buildProdSnapshot("rel-1",
		release.Node{UniqueID: "n1", ProjectName: "service-1", ImageTag: "t1", NodeType: "dbt-model"},
		release.Node{UniqueID: "n2", ProjectName: "service-1", ImageTag: "t1", NodeType: "python-node"},
		release.Node{UniqueID: "n3", ProjectName: "service-2", ImageTag: "t2", NodeType: "dbt-model"},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
		"service-2": "s3://bucket/service-2/manifest.json",
	}

	store := newFakeStore()
	repo := &fakeProductionPointerRepo{store: store}

	n, err := handlers.SeedProductionPointers(context.Background(), snap.releaseID, snap.topo, existingKeys, repo, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service-1")
	assert.Equal(t, 0, n)

	pointer1, err := repo.Get(context.Background(), "service-1")
	require.NoError(t, err)
	assert.Nil(t, pointer1, "mixed-kind project must not be written")

	pointer2, err := repo.Get(context.Background(), "service-2")
	require.NoError(t, err)
	assert.Nil(t, pointer2, "no project must be written when validation fails")
}
