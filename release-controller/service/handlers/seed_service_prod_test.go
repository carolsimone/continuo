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

func buildCurrentProd(releaseID string, nodes ...release.Node) *release.CurrentProd {
	return release.RehydrateCurrentProd(releaseID, release.Topology(nodes), time.Unix(1, 0).UTC())
}

func TestSeedServiceProd_SeedsAllServices(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// Snapshot with two services: service-1 (2 nodes, tag t1) and service-2 (1 node, tag t2).
	cp := buildCurrentProd("rel-1",
		release.Node{UniqueID: "n1", ServiceName: "service-1", ImageTag: "t1"},
		release.Node{UniqueID: "n2", ServiceName: "service-1", ImageTag: "t1-dup"}, // second node; first tag wins
		release.Node{UniqueID: "n3", ServiceName: "service-2", ImageTag: "t2"},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
		"service-2": "s3://bucket/service-2/manifest.json",
	}

	store := newFakeStore()
	repo := &fakeServiceProdRepo{store: store}

	n, err := handlers.SeedServiceProd(context.Background(), cp, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	sp1, err := repo.Get(context.Background(), "service-1")
	require.NoError(t, err)
	require.NotNil(t, sp1)
	assert.Equal(t, "service-1", sp1.ServiceName())
	assert.Equal(t, "rel-1", sp1.ReleaseID())
	assert.Equal(t, "s3://bucket/service-1/manifest.json", sp1.ManifestS3Key())
	assert.Equal(t, "t1", sp1.ImageTag(), "first non-empty image tag among service nodes")
	assert.Equal(t, now, sp1.UpdatedAt())

	sp2, err := repo.Get(context.Background(), "service-2")
	require.NoError(t, err)
	require.NotNil(t, sp2)
	assert.Equal(t, "t2", sp2.ImageTag())
	assert.Equal(t, "s3://bucket/service-2/manifest.json", sp2.ManifestS3Key())
}

func TestSeedServiceProd_MissingKeyReturnsError(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	cp := buildCurrentProd("rel-1",
		release.Node{UniqueID: "n1", ServiceName: "service-1", ImageTag: "t1"},
		release.Node{UniqueID: "n2", ServiceName: "service-2", ImageTag: "t2"},
	)
	// existingKeys only covers service-1; service-2 is missing.
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
	}

	store := newFakeStore()
	repo := &fakeServiceProdRepo{store: store}

	_, err := handlers.SeedServiceProd(context.Background(), cp, existingKeys, repo, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service-2")
}

func TestSeedServiceProd_SkipsNodesWithEmptyServiceName(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	cp := buildCurrentProd("rel-1",
		release.Node{UniqueID: "n1", ServiceName: "", ImageTag: "t1"}, // no service name — skipped
		release.Node{UniqueID: "n2", ServiceName: "service-1", ImageTag: "t1"},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
	}

	store := newFakeStore()
	repo := &fakeServiceProdRepo{store: store}

	n, err := handlers.SeedServiceProd(context.Background(), cp, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "node with empty service_name must be skipped")
}

func TestSeedServiceProd_IdempotentOnRerun(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	cp := buildCurrentProd("rel-1",
		release.Node{UniqueID: "n1", ServiceName: "service-1", ImageTag: "t1"},
	)
	existingKeys := map[string]string{"service-1": "s3://bucket/service-1/manifest.json"}

	store := newFakeStore()
	repo := &fakeServiceProdRepo{store: store}

	n1, err := handlers.SeedServiceProd(context.Background(), cp, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 1, n1)

	// Running again should succeed and still report 1 service.
	n2, err := handlers.SeedServiceProd(context.Background(), cp, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 1, n2)

	sp, _ := repo.Get(context.Background(), "service-1")
	require.NotNil(t, sp)
	assert.Equal(t, "rel-1", sp.ReleaseID(), "second run must not change the pointer")
}

func TestSeedServiceProd_EmptyTopologyReturnsZero(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	cp := buildCurrentProd("rel-1") // no nodes
	store := newFakeStore()
	repo := &fakeServiceProdRepo{store: store}

	n, err := handlers.SeedServiceProd(context.Background(), cp, map[string]string{}, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestSeedServiceProd_DerivesManifestKindPerService(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// service-1 is a dbt service; service-2 is a python service.
	cp := buildCurrentProd("rel-1",
		release.Node{UniqueID: "n1", ServiceName: "service-1", ImageTag: "t1", NodeType: "dbt-model"},
		release.Node{UniqueID: "n2", ServiceName: "service-2", ImageTag: "t2", NodeType: "python-model"},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
		"service-2": "s3://bucket/service-2/contract.yaml",
	}

	store := newFakeStore()
	repo := &fakeServiceProdRepo{store: store}

	n, err := handlers.SeedServiceProd(context.Background(), cp, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	sp1, err := repo.Get(context.Background(), "service-1")
	require.NoError(t, err)
	require.NotNil(t, sp1)
	assert.Equal(t, release.ManifestKindDbt, sp1.ManifestKind())

	sp2, err := repo.Get(context.Background(), "service-2")
	require.NoError(t, err)
	require.NotNil(t, sp2)
	assert.Equal(t, release.ManifestKindPython, sp2.ManifestKind())
}

func TestSeedServiceProd_CsvServiceClassifiedPython(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// service-1 has only python-csv nodes: it must classify as python, same
	// as a service made only of python-model nodes.
	cp := buildCurrentProd("rel-1",
		release.Node{UniqueID: "n1", ServiceName: "service-1", ImageTag: "t1", NodeType: "python-csv"},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/contract.yaml",
	}

	store := newFakeStore()
	repo := &fakeServiceProdRepo{store: store}

	n, err := handlers.SeedServiceProd(context.Background(), cp, existingKeys, repo, now)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	sp, err := repo.Get(context.Background(), "service-1")
	require.NoError(t, err)
	require.NotNil(t, sp)
	assert.Equal(t, release.ManifestKindPython, sp.ManifestKind())
}

func TestSeedServiceProd_MixedKindServiceErrorsAndWritesNothing(t *testing.T) {
	now := time.Unix(1_000_000, 0).UTC()

	// service-1 mixes a dbt node and a python node — this must be rejected
	// before anything is written, including the clean service-2 pointer.
	cp := buildCurrentProd("rel-1",
		release.Node{UniqueID: "n1", ServiceName: "service-1", ImageTag: "t1", NodeType: "dbt-model"},
		release.Node{UniqueID: "n2", ServiceName: "service-1", ImageTag: "t1", NodeType: "python-model"},
		release.Node{UniqueID: "n3", ServiceName: "service-2", ImageTag: "t2", NodeType: "dbt-model"},
	)
	existingKeys := map[string]string{
		"service-1": "s3://bucket/service-1/manifest.json",
		"service-2": "s3://bucket/service-2/manifest.json",
	}

	store := newFakeStore()
	repo := &fakeServiceProdRepo{store: store}

	n, err := handlers.SeedServiceProd(context.Background(), cp, existingKeys, repo, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service-1")
	assert.Equal(t, 0, n)

	sp1, err := repo.Get(context.Background(), "service-1")
	require.NoError(t, err)
	assert.Nil(t, sp1, "mixed-kind service must not be written")

	sp2, err := repo.Get(context.Background(), "service-2")
	require.NoError(t, err)
	assert.Nil(t, sp2, "no service must be written when validation fails")
}
