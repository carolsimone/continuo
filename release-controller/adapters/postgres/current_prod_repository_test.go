//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCurrentProdRepository_GetEmptyReturnsZeroValue(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewCurrentProdRepository(db)
	cp, err := repo.Get(context.Background())
	require.NoError(t, err)
	assert.Empty(t, cp.ReleaseID())
	assert.True(t, cp.Topology().IsZero())
	assert.Equal(t, int64(0), cp.PromotionSeq())
}

func TestCurrentProdRepository_UpsertAndGet(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewCurrentProdRepository(db)
	ref := release.TopologyRef{URI: "s3://b/tenants/default/topologies/sha-xyz/topology.json.gz", SHA256: "f00d", NodeCount: 7}
	require.NoError(t, repo.Upsert(context.Background(),
		release.RehydrateCurrentProd("sha-xyz", ref, 3, time.Unix(500, 0).UTC())))

	got, err := repo.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "sha-xyz", got.ReleaseID())
	assert.Equal(t, ref, got.Topology())
	assert.Equal(t, int64(3), got.PromotionSeq())
	assert.Equal(t, time.Unix(500, 0).UTC(), got.UpdatedAt().UTC())
}

// A row that references no topology artifact reads with a zero reference: its
// NULL reference columns map to a zero TopologyRef.
func TestCurrentProdRepository_RowWithoutAnArtifactHasNoTopologyRef(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, updated_at) VALUES (1, '', now())`)
	require.NoError(t, err)

	got, err := postgres.NewCurrentProdRepository(db).Get(context.Background())
	require.NoError(t, err)
	assert.Empty(t, got.ReleaseID())
	assert.True(t, got.Topology().IsZero())
}
