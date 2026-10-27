//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaColumns_NamesOnlyTheMissingOnesInOrder(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	missing, err := postgres.NewSchemaColumns(db).Missing(context.Background(),
		[]string{"deployments.no_such_column", "deployments.facts", "no_such_table.facts", "deployments.job_name"})
	require.NoError(t, err)
	assert.Equal(t, []string{"deployments.no_such_column", "no_such_table.facts"}, missing)
}

// The migrated execution database holds every column the startup gate asks for.
func TestSchemaColumns_MigratedDatabaseHoldsEveryRequiredColumn(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	missing, err := postgres.NewSchemaColumns(db).Missing(context.Background(), handlers.RequiredColumns)
	require.NoError(t, err)
	assert.Empty(t, missing)
}
