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

func TestServiceProdRepository_UpsertListGet(t *testing.T) {
	db := openTestDB(t)
	repo := postgres.NewServiceProdRepository(db)
	ctx := context.Background()

	now := time.Unix(1_000_000, 0).UTC()

	svcA := release.NewServiceProd("svc-a", "sha-1", "s3://bucket/svc-a/v1.json", "image-a:sha-1", now)
	svcB := release.NewServiceProd("svc-b", "sha-2", "s3://bucket/svc-b/v1.json", "image-b:sha-2", now)

	require.NoError(t, repo.Upsert(ctx, svcA))
	require.NoError(t, repo.Upsert(ctx, svcB))

	// List returns both rows.
	all, err := repo.List(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 2)

	// Re-upserting svc-a with new values is idempotent on the primary key
	// and updates the row; the list length stays at 2.
	updatedA := release.NewServiceProd("svc-a", "sha-99", "s3://bucket/svc-a/v2.json", "image-a:sha-99", now.Add(time.Hour))
	require.NoError(t, repo.Upsert(ctx, updatedA))

	all, err = repo.List(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 2, "re-upsert must not insert a duplicate row")

	// Get returns the updated values for svc-a.
	got, err := repo.Get(ctx, "svc-a")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "sha-99", got.ReleaseID())
	assert.Equal(t, "s3://bucket/svc-a/v2.json", got.ManifestS3Key())
	assert.Equal(t, "image-a:sha-99", got.ImageTag())

	// Get for a missing service returns (nil, nil) — not an error.
	missing, err := repo.Get(ctx, "svc-does-not-exist")
	require.NoError(t, err)
	assert.Nil(t, missing)
}
