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
}

func TestCurrentProdRepository_UpsertAndGet(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewCurrentProdRepository(db)
	cp := release.RehydrateCurrentProd("sha-xyz",
		release.Topology{{UniqueID: "a"}},
		time.Unix(500, 0).UTC())
	require.NoError(t, repo.Upsert(context.Background(), cp))

	got, err := repo.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "sha-xyz", got.ReleaseID())
	assert.Len(t, got.TopologySnapshot(), 1)
}
