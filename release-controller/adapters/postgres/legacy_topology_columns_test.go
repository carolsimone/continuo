//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// On the Flyway-built schema, with release migration V26 applied, no legacy
// topology column remains and release-controller may start.
func TestLegacyTopologyColumns_NoneRemainOnTheMigratedSchema(t *testing.T) {
	db := openTestDB(t)
	cols := postgres.NewLegacyTopologyColumns(db)

	got, err := cols.Present(context.Background())
	require.NoError(t, err)
	assert.Empty(t, got)
	require.NoError(t, handlers.CheckLegacyTopologyColumnsDropped(context.Background(), cols))
}

// On a database still at V25, the state a refused or not-yet-applied V26
// leaves, all three columns exist and release-controller refuses to start.
func TestLegacyTopologyColumns_RefusesASchemaBeforeV26(t *testing.T) {
	db := releaseDBBefore(t, legacyColumnDrop)
	cols := postgres.NewLegacyTopologyColumns(db)

	got, err := cols.Present(context.Background())
	require.NoError(t, err)
	assert.Equal(t, allLegacyColumns, got)

	err = handlers.CheckLegacyTopologyColumnsDropped(context.Background(), cols)
	require.Error(t, err)
	assert.ErrorIs(t, err, handlers.ErrLegacyTopologyColumnsPresent)
}
