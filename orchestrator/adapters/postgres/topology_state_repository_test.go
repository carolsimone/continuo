package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopologyStateRepository_IncrementGeneration_Monotonic(t *testing.T) {
	db := newTestDB(t)

	// Reset the singleton to 0 for test isolation
	_, err := db.ExecContext(context.Background(), `UPDATE topology_state SET topology_generation = 0`)
	require.NoError(t, err)

	repo := postgres.NewTopologyStateRepository(db)
	ctx := context.Background()

	n1, err := repo.IncrementGeneration(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n1)

	n2, err := repo.IncrementGeneration(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n2)

	current, err := repo.GetGeneration(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), current)
}
