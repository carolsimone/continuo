//go:build integration

package postgres_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/stretchr/testify/require"
)

func TestCandidateSchemaCleaner_DropsAndIsIdempotent(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	_, err := db.ExecContext(ctx, `CREATE SCHEMA "_candidate_relX"; CREATE TABLE "_candidate_relX".t (id int);`)
	require.NoError(t, err)

	c := postgres.NewCandidateSchemaCleaner(db, slog.Default())
	require.NoError(t, c.DropCandidateSchema(ctx, "_candidate_relX"))

	var exists bool
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name=$1)`, "_candidate_relX").Scan(&exists))
	require.False(t, exists)

	// Idempotent on a missing schema.
	require.NoError(t, c.DropCandidateSchema(ctx, "_candidate_relX"))
}

func TestCandidateSchemaCleaner_RejectsUnsafeName(t *testing.T) {
	c := postgres.NewCandidateSchemaCleaner(nil, slog.Default())
	require.Error(t, c.DropCandidateSchema(context.Background(), "public; DROP TABLE x"))
}
