//go:build integration

package postgres_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/stretchr/testify/require"
)

func TestCandidateSchemaCreator_CreatesAndIsIdempotent(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	c := postgres.NewCandidateSchemaCreator(db, slog.Default())
	require.NoError(t, c.EnsureCandidateSchema(ctx, "_candidate_relA"))

	var exists bool
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name=$1)`, "_candidate_relA").Scan(&exists))
	require.True(t, exists)

	// Calling again on an existing schema is a no-op, not an error.
	require.NoError(t, c.EnsureCandidateSchema(ctx, "_candidate_relA"))
}

func TestCandidateSchemaCreator_RejectsUnsafeName(t *testing.T) {
	c := postgres.NewCandidateSchemaCreator(nil, slog.Default())
	require.Error(t, c.EnsureCandidateSchema(context.Background(), "public; DROP TABLE x"))
}

// TestCandidateSchemaCreator_ConcurrentCreatesDoNotRace is the regression guard
// for the seed-validation race: many root validation jobs would create the
// shared candidate schema in parallel, and `CREATE SCHEMA IF NOT EXISTS` is not
// atomic, so concurrent creators raised a unique-violation on pg_namespace. The
// creator serializes them with a transaction-scoped advisory lock, so every
// concurrent call must succeed and the schema must exist exactly once.
//
// Each goroutine opens its own session against the same database (the advisory
// lock is held per-session/transaction), reproducing the multi-pod scenario
// where each dbt job has an independent connection.
func TestCandidateSchemaCreator_ConcurrentCreatesDoNotRace(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	ctx := context.Background()

	const schema = "_candidate_relRace"
	const concurrency = 16

	creator := postgres.NewCandidateSchemaCreator(db, slog.Default())

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start // release all goroutines together to maximize contention
			errs[idx] = creator.EnsureCandidateSchema(ctx, schema)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "concurrent EnsureCandidateSchema #%d must not race", i)
	}

	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name=$1`, schema).Scan(&count))
	require.Equal(t, 1, count, "schema must exist exactly once after concurrent creates")
}
