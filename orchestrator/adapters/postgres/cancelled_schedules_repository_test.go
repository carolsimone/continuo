package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/orchestrator/adapters/postgres"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	host := envOrDefault("POSTGRES_HOST", "postgres")
	port := envOrDefault("POSTGRES_PORT", "5432")
	db_name := envOrDefault("POSTGRES_DB", "continuo_orchestrator")
	user := envOrDefault("POSTGRES_USER", "continuo_svc")
	password := envOrDefault("POSTGRES_PASSWORD", "continuo")
	connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		host, port, db_name, user, password)
	db, err := sqlx.Connect("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func TestCancelledSchedulesRepository_InsertAndExists(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewCancelledSchedulesRepository(db)
	ctx := context.Background()

	id := uuid.New()

	exists, err := repo.Exists(ctx, id)
	require.NoError(t, err)
	assert.False(t, exists)

	require.NoError(t, repo.Insert(ctx, id))

	exists, err = repo.Exists(ctx, id)
	require.NoError(t, err)
	assert.True(t, exists)

	// Idempotent second insert
	require.NoError(t, repo.Insert(ctx, id))

	// Cleanup
	t.Cleanup(func() {
		db.ExecContext(ctx, `DELETE FROM cancelled_schedules WHERE schedule_id = $1`, id)
	})
}

func TestCancelledSchedulesRepository_DeleteExpired(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewCancelledSchedulesRepository(db)
	ctx := context.Background()

	old := uuid.New()
	fresh := uuid.New()

	require.NoError(t, repo.Insert(ctx, old))
	require.NoError(t, repo.Insert(ctx, fresh))

	// Backdate the old row
	_, err := db.ExecContext(ctx,
		`UPDATE cancelled_schedules SET cancelled_at = $1 WHERE schedule_id = $2`,
		time.Now().Add(-25*time.Hour), old)
	require.NoError(t, err)

	n, err := repo.DeleteExpired(ctx, 24*time.Hour)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, int64(1))

	exists, _ := repo.Exists(ctx, old)
	assert.False(t, exists, "old row should be deleted")

	exists, _ = repo.Exists(ctx, fresh)
	assert.True(t, exists, "fresh row should remain")

	// Cleanup
	t.Cleanup(func() {
		db.ExecContext(ctx, `DELETE FROM cancelled_schedules WHERE schedule_id = $1`, fresh)
	})
}
