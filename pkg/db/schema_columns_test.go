//go:build integration

package db_test

import (
	"context"
	"testing"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMissingColumns_NamesOnlyTheMissingOnesInOrder(t *testing.T) {
	cfg := pkgconfig.LoadPostgres(&pkgconfig.Validator{})
	if cfg.Host == "" {
		testdeps.Unavailable(t, "POSTGRES_HOST not set; run `make test-go SERVICE=pkg`")
	}
	db, err := pkgdb.Open(context.Background(), cfg, pkgconfig.PoolConfig{MaxOpenConns: 2, MaxIdleConns: 1})
	require.NoError(t, err)
	defer db.Close()

	missing, err := pkgdb.MissingColumns(context.Background(), db,
		[]string{"flyway_schema_history.no_such_column", "flyway_schema_history.version", "no_such_table.version"})
	require.NoError(t, err)
	assert.Equal(t, []string{"flyway_schema_history.no_such_column", "no_such_table.version"}, missing)
}
