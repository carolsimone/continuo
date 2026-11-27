//go:build integration

package postgres_test

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/carolsimone/continuo/release-controller/adapters/postgres"
)

// The Flyway-built release database holds the release schema.
func TestReleaseSchema_TheMigratedDatabaseIsInitialized(t *testing.T) {
	db := connectTestDB(t)
	defer func() { _ = db.Close() }()

	initialized, err := postgres.NewReleaseSchema(db).Initialized(context.Background())
	require.NoError(t, err)
	assert.True(t, initialized)
}

// A reachable database the release migrations never built, here the server's
// own postgres database, has none of the release tables.
func TestReleaseSchema_ADatabaseTheMigrationsHaveNotBuiltIsNot(t *testing.T) {
	dsn := os.Getenv("RELEASE_TEST_PG_DSN")
	if dsn == "" {
		testdeps.Unavailable(t, "RELEASE_TEST_PG_DSN not set")
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	u.Path = "/postgres"
	db, err := sqlx.Connect("postgres", u.String())
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	initialized, err := postgres.NewReleaseSchema(db).Initialized(context.Background())
	require.NoError(t, err)
	assert.False(t, initialized)
}
