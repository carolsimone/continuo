//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A fence is kept for the replay horizon, by the database clock: a dispatch
// of the run redriven from the dead letters within it still meets the fence.
// Fencing a run twice keeps one row.
func TestCancelledRuns_DeleteOlderThanKeepsTheReplayHorizon(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()
	repo := postgres.NewCancelledRunsRepository(db)
	young, old := uuid.New(), uuid.New()
	require.NoError(t, repo.Insert(ctx, young))
	require.NoError(t, repo.Insert(ctx, young))
	require.NoError(t, repo.Insert(ctx, old))
	_, err := db.Exec(`UPDATE cancelled_runs SET cancelled_at = NOW() - interval '29 days 23 hours' WHERE run_id = $1`, young)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE cancelled_runs SET cancelled_at = NOW() - interval '30 days 1 hour' WHERE run_id = $1`, old)
	require.NoError(t, err)

	n, err := repo.DeleteOlderThan(ctx, pkgmodel.ReplayHorizon)

	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "only the fence older than the horizon goes")
	fenced, err := repo.Exists(ctx, young)
	require.NoError(t, err)
	assert.True(t, fenced, "a run cancelled within the horizon stays fenced")
	fenced, err = repo.Exists(ctx, old)
	require.NoError(t, err)
	assert.False(t, fenced)
}
