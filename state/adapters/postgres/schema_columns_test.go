//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/stretchr/testify/require"
)

// The migrated state database has every column state checks for at startup,
// so a column a migration renames or drops fails here rather than as a
// service that refuses to boot.
func TestSchemaColumns_TheMigratedDatabasePassesTheStartupSchemaCheck(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, svchandlers.CheckSchema(context.Background(), postgres.NewSchemaColumns(db)))
}
