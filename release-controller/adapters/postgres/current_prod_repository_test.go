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

// A row written before topology artifacts existed reads with the release it
// names and no topology reference.
func TestCurrentProdRepository_LegacyRowHasNoTopologyRef(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, 'rLegacy', '[{"unique_id":"a"}]'::jsonb, now())`)
	require.NoError(t, err)

	got, err := postgres.NewCurrentProdRepository(db).Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "rLegacy", got.ReleaseID())
	assert.True(t, got.Topology().IsZero())
}

// Every write by reference clears the legacy snapshot, so the JSONB never
// disagrees with the release current_prod names.
func TestCurrentProdRepository_UpsertClearsTheLegacySnapshot(t *testing.T) {
	db := openTestDB(t)
	_, err := db.Exec(`INSERT INTO current_prod (id, release_id, topology_snapshot, updated_at)
		VALUES (1, 'rLegacy', '[{"unique_id":"a"}]'::jsonb, now())`)
	require.NoError(t, err)

	ref := release.TopologyRef{URI: "s3://b/k", SHA256: "f00d", NodeCount: 1}
	require.NoError(t, postgres.NewCurrentProdRepository(db).Upsert(context.Background(),
		release.RehydrateCurrentProd("rLegacy", ref, 1, time.Unix(500, 0).UTC())))

	var snapshotIsNull bool
	require.NoError(t, db.Get(&snapshotIsNull, `SELECT topology_snapshot IS NULL FROM current_prod WHERE id = 1`))
	assert.True(t, snapshotIsNull)
}
