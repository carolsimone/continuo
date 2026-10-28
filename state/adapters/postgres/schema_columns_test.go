//go:build integration

package postgres_test

import (
	"context"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The migrated state database has every column state checks for at startup,
// so a column a migration renames or drops fails here rather than as a
// service that refuses to boot.
func TestSchemaColumns_TheMigratedDatabasePassesTheStartupSchemaCheck(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, svchandlers.CheckSchema(context.Background(), postgres.NewSchemaColumns(db)))
}

// state reads the schedule names from the live release it stores, so the
// state database holds no schedule catalog: neither schedule_catalog nor
// schedule_catalog_state exists, a run stores no service metadata, and the
// columns state requires at startup name none of them.
func TestStateSchema_TheCatalogIsGone(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	var tables []string
	require.NoError(t, db.SelectContext(ctx, &tables, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('schedule_catalog', 'schedule_catalog_state')`))
	assert.Empty(t, tables, "the schedule catalog's tables are dropped")

	missing, err := postgres.NewSchemaColumns(db).Missing(ctx, []string{"runs.service_metadata", "runs.schedule_name"})
	require.NoError(t, err)
	assert.Equal(t, []string{"runs.service_metadata"}, missing, "runs keeps its schedule name and drops its service metadata")

	for _, c := range svchandlers.RequiredColumns {
		assert.False(t, strings.HasPrefix(c, "schedule_catalog"), "state requires %s", c)
		assert.NotEqual(t, "runs.service_metadata", c, "state requires %s", c)
	}
}
