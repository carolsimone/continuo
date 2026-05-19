package outbox_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTableDDL = `
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS test_outbox (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_processing_id  UUID NULL,
    aggregate_type         TEXT NOT NULL,
    aggregate_id           UUID NOT NULL,
    event_type             TEXT NOT NULL,
    payload                JSONB NOT NULL,
    stream_name            TEXT NOT NULL,
    status                 TEXT NOT NULL DEFAULT 'pending',
    retry_count            INT  NOT NULL DEFAULT 0,
    max_retries            INT  NOT NULL DEFAULT 3,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at           TIMESTAMPTZ NULL,
    error_message          TEXT NULL,
    CONSTRAINT test_outbox_status_check
        CHECK (status IN ('pending','processed','failed'))
);
`

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// dbForTest reads OUTBOX_TEST_DSN; skips the test if unset, so unit-only runs pass.
func dbForTest(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_DSN not set; skipping Postgres integration test")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)
	_, err = db.Exec(`DROP TABLE IF EXISTS test_outbox`)
	require.NoError(t, err)
	_, err = db.Exec(testTableDDL)
	require.NoError(t, err)
	t.Cleanup(func() { db.Exec(`DROP TABLE IF EXISTS test_outbox`); db.Close() })
	return db
}

func TestPostgresRepository_CreateAndGetPending(t *testing.T) {
	db := dbForTest(t)
	repo := outbox.NewPostgresRepository(db, "test_outbox", newTestLogger())

	entry := &outbox.Entry{
		AggregateType: "task",
		AggregateID:   uuid.New(),
		EventType:     "x",
		Payload:       []byte(`{"k":"v"}`),
		StreamName:    "x:v1",
	}
	require.NoError(t, repo.Create(context.Background(), entry))
	assert.NotEqual(t, uuid.Nil, entry.ID)

	// GetPendingBatch must run inside a tx (SKIP LOCKED locks)
	tx, err := db.Beginx()
	require.NoError(t, err)
	txRepo := outbox.NewPostgresRepository(tx, "test_outbox", newTestLogger())
	pending, err := txRepo.GetPendingBatch(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, entry.ID, pending[0].ID)
	assert.Equal(t, "pending", pending[0].Status)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(pending[0].Payload, &payload))
	assert.Equal(t, "v", payload["k"])
	require.NoError(t, tx.Rollback())
}

func TestPostgresRepository_MarkProcessed(t *testing.T) {
	db := dbForTest(t)
	repo := outbox.NewPostgresRepository(db, "test_outbox", newTestLogger())
	entry := &outbox.Entry{
		AggregateType: "task", AggregateID: uuid.New(),
		EventType: "x", Payload: []byte(`{}`), StreamName: "x:v1",
	}
	require.NoError(t, repo.Create(context.Background(), entry))
	require.NoError(t, repo.MarkProcessed(context.Background(), entry.ID))

	var status string
	var processedAt *string
	require.NoError(t, db.QueryRow(`SELECT status, processed_at::text FROM test_outbox WHERE id=$1`, entry.ID).Scan(&status, &processedAt))
	assert.Equal(t, "processed", status)
	require.NotNil(t, processedAt)
}

func TestPostgresRepository_MarkFailed(t *testing.T) {
	db := dbForTest(t)
	repo := outbox.NewPostgresRepository(db, "test_outbox", newTestLogger())
	entry := &outbox.Entry{
		AggregateType: "task", AggregateID: uuid.New(),
		EventType: "x", Payload: []byte(`{}`), StreamName: "x:v1",
	}
	require.NoError(t, repo.Create(context.Background(), entry))
	require.NoError(t, repo.MarkFailed(context.Background(), entry.ID, "boom"))

	var status, errMsg string
	require.NoError(t, db.QueryRow(`SELECT status, error_message FROM test_outbox WHERE id=$1`, entry.ID).Scan(&status, &errMsg))
	assert.Equal(t, "failed", status)
	assert.Equal(t, "boom", errMsg)
}

func TestPostgresRepository_IncrementRetryDoesNotChangeStatus(t *testing.T) {
	db := dbForTest(t)
	repo := outbox.NewPostgresRepository(db, "test_outbox", newTestLogger())
	entry := &outbox.Entry{
		AggregateType: "task", AggregateID: uuid.New(),
		EventType: "x", Payload: []byte(`{}`), StreamName: "x:v1",
	}
	require.NoError(t, repo.Create(context.Background(), entry))
	require.NoError(t, repo.IncrementRetry(context.Background(), entry.ID))

	var status string
	var rc int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM test_outbox WHERE id=$1`, entry.ID).Scan(&status, &rc))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 1, rc)
}

func TestPostgresRepository_SkipLockedIsolatesConcurrentBatches(t *testing.T) {
	db := dbForTest(t)
	repo := outbox.NewPostgresRepository(db, "test_outbox", newTestLogger())

	// Create 3 pending rows.
	for i := 0; i < 3; i++ {
		require.NoError(t, repo.Create(context.Background(), &outbox.Entry{
			AggregateType: "task", AggregateID: uuid.New(),
			EventType: "x", Payload: []byte(`{}`), StreamName: "x:v1",
		}))
	}

	tx1, err := db.Beginx()
	require.NoError(t, err)
	defer tx1.Rollback()
	tx2, err := db.Beginx()
	require.NoError(t, err)
	defer tx2.Rollback()

	batch1, err := outbox.NewPostgresRepository(tx1, "test_outbox", newTestLogger()).GetPendingBatch(context.Background(), 10)
	require.NoError(t, err)
	batch2, err := outbox.NewPostgresRepository(tx2, "test_outbox", newTestLogger()).GetPendingBatch(context.Background(), 10)
	require.NoError(t, err)

	// Combined coverage = 3 rows, disjoint by ID (SKIP LOCKED ensures no row is claimed by both transactions).
	seen := map[uuid.UUID]int{}
	for _, e := range batch1 {
		seen[e.ID]++
	}
	for _, e := range batch2 {
		seen[e.ID]++
	}
	for id, n := range seen {
		assert.Equal(t, 1, n, "row %s claimed by both txs (SKIP LOCKED broken)", id)
	}
	assert.Equal(t, 3, len(seen))
}
