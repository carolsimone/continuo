//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func poolFixture(poolKey string, now time.Time) model.WorkerPool {
	return model.WorkerPool{
		PoolKey:     poolKey,
		ServiceName: "finance",
		ImageTag:    "sha-abc",
		RuntimeManifest: pkgmodel.RuntimeManifestRef{
			RuntimeManifestURI:                "s3://continuo/artifacts/finance/manifest.msgpack",
			RuntimeManifestSHA256:             "a1b2",
			RuntimeManifestDBTVersion:         "1.12.0b1",
			RuntimeManifestParseContextSHA256: "c3d4",
		},
		CredentialSHA256: "credential-digest",
		DesiredReplicas:  2,
		LastActivityAt:   now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

func TestWorkerPoolRepository_AddAndGetRoundTrip(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewWorkerPoolRepository(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	pool := poolFixture("pool-abc", now)
	require.NoError(t, repo.Add(ctx, pool))

	loaded, err := repo.Get(ctx, "pool-abc")
	require.NoError(t, err)
	require.NotNil(t, loaded)

	assert.Equal(t, "pool-abc", loaded.PoolKey)
	assert.Equal(t, "finance", loaded.ServiceName)
	assert.Equal(t, "sha-abc", loaded.ImageTag)
	assert.Equal(t, pool.RuntimeManifest, loaded.RuntimeManifest)
	assert.Equal(t, "credential-digest", loaded.CredentialSHA256)
	assert.Equal(t, 2, loaded.DesiredReplicas)
	assert.True(t, loaded.Ready())
	assert.Empty(t, loaded.InitializationError)
}

// TestWorkerPoolRepository_GetUnknownPoolIsNotAnError lets the authenticator
// treat an unregistered pool exactly like a wrong credential.
func TestWorkerPoolRepository_GetUnknownPoolIsNotAnError(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	loaded, err := postgres.NewWorkerPoolRepository(db).Get(context.Background(), "pool-missing")

	require.NoError(t, err)
	assert.Nil(t, loaded)
}

// TestWorkerPoolRepository_SaveRoundTripsTheInitializationError pins that a
// pool's readiness survives a write and a read: a NULL column and an empty
// string must both read as ready.
func TestWorkerPoolRepository_SaveRoundTripsTheInitializationError(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewWorkerPoolRepository(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	pool := poolFixture("pool-abc", now)
	require.NoError(t, repo.Add(ctx, pool))

	pool.RecordInitializationFailure("artifact_rejected", "sha256 mismatch", now)
	pool.DesiredReplicas = 1
	require.NoError(t, repo.Save(ctx, pool))

	loaded, err := repo.Get(ctx, "pool-abc")
	require.NoError(t, err)
	assert.False(t, loaded.Ready())
	assert.Equal(t, "artifact_rejected: sha256 mismatch", loaded.InitializationError)
	assert.Equal(t, 1, loaded.DesiredReplicas)

	pool.ClearInitializationError(now)
	require.NoError(t, repo.Save(ctx, pool))

	loaded, err = repo.Get(ctx, "pool-abc")
	require.NoError(t, err)
	assert.True(t, loaded.Ready())
	assert.Empty(t, loaded.InitializationError)
}

// TestWorkerPoolRepository_SaveUnknownPoolFails stops a report for a pool that
// was never registered from silently doing nothing.
func TestWorkerPoolRepository_SaveUnknownPoolFails(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	now := time.Now().UTC().Truncate(time.Millisecond)
	err := postgres.NewWorkerPoolRepository(db).Save(context.Background(), poolFixture("pool-missing", now))

	require.Error(t, err)
}

func TestWorkerPoolRepository_ListReturnsEveryPool(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewWorkerPoolRepository(db)

	now := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, repo.Add(ctx, poolFixture("pool-b", now)))
	require.NoError(t, repo.Add(ctx, poolFixture("pool-a", now)))

	pools, err := repo.List(ctx)
	require.NoError(t, err)
	require.Len(t, pools, 2)
	assert.Equal(t, "pool-a", pools[0].PoolKey)
	assert.Equal(t, "pool-b", pools[1].PoolKey)
}

func TestWorkerPoolRepository_ListEmptyIsNotAnError(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	pools, err := postgres.NewWorkerPoolRepository(db).List(context.Background())

	require.NoError(t, err)
	assert.Empty(t, pools)
}
