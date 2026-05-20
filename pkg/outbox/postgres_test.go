package outbox_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/testmigrations"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testOutboxTable is the canonical outbox table the pkg/outbox repository
// tests run against. We apply orchestrator's full Flyway migration set to
// stand it up, so this test schema stays in lock-step with production and
// can never silently drift.
const testOutboxTable = "orchestrator_outbox"

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// orchestratorMigrationDir resolves <repo>/db/migration/orchestrator/ from
// this source file's location, so tests work both inside the service
// container (source mounted at /src) and on a developer machine running
// `go test ./pkg/outbox/...` from the repo root.
func orchestratorMigrationDir() (string, error) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed — cannot locate pkg/outbox/postgres_test.go")
	}
	// thisFile = <repo>/pkg/outbox/postgres_test.go
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
	return filepath.Join(repoRoot, "db", "migration", "orchestrator"), nil
}

// dbForTest reads OUTBOX_TEST_DSN; skips the test if unset, so unit-only
// runs pass. Each test drops every orchestrator-owned table, reapplies the
// real migration set from db/migration/orchestrator/, and truncates the
// outbox table between tests for isolation.
func dbForTest(t *testing.T) *sqlx.DB {
	t.Helper()
	dsn := os.Getenv("OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_DSN not set; skipping Postgres integration test")
	}
	db, err := sqlx.Connect("postgres", dsn)
	require.NoError(t, err)

	// Drop orchestrator artefacts so the migration set sees a clean slate.
	// Order matters because of FK dependencies into message_processing.
	for _, stmt := range []string{
		`DROP TABLE IF EXISTS orchestrator_outbox CASCADE`,
		`DROP TABLE IF EXISTS outbox CASCADE`,
		`DROP TABLE IF EXISTS published_messages CASCADE`,
		`DROP TABLE IF EXISTS rejected_topology_messages CASCADE`,
		`DROP TABLE IF EXISTS topology_state CASCADE`,
		`DROP TABLE IF EXISTS cancelled_schedules CASCADE`,
		`DROP TABLE IF EXISTS message_processing CASCADE`,
	} {
		_, err = db.Exec(stmt)
		require.NoError(t, err, "drop: %s", stmt)
	}

	dir, err := orchestratorMigrationDir()
	require.NoError(t, err)
	require.NoError(t, testmigrations.Apply(db.DB, dir))

	t.Cleanup(func() {
		_, _ = db.Exec(`TRUNCATE ` + testOutboxTable)
		db.Close()
	})
	return db
}

func TestPostgresRepository_CreateAndGetPending(t *testing.T) {
	db := dbForTest(t)
	repo := outbox.NewPostgresRepository(db, testOutboxTable, newTestLogger())

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
	txRepo := outbox.NewPostgresRepository(tx, testOutboxTable, newTestLogger())
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
	repo := outbox.NewPostgresRepository(db, testOutboxTable, newTestLogger())
	entry := &outbox.Entry{
		AggregateType: "task", AggregateID: uuid.New(),
		EventType: "x", Payload: []byte(`{}`), StreamName: "x:v1",
	}
	require.NoError(t, repo.Create(context.Background(), entry))
	require.NoError(t, repo.MarkProcessed(context.Background(), entry.ID))

	var status string
	var processedAt *string
	require.NoError(t, db.QueryRow(`SELECT status, processed_at::text FROM orchestrator_outbox WHERE id=$1`, entry.ID).Scan(&status, &processedAt))
	assert.Equal(t, "processed", status)
	require.NotNil(t, processedAt)
}

func TestPostgresRepository_MarkFailed(t *testing.T) {
	db := dbForTest(t)
	repo := outbox.NewPostgresRepository(db, testOutboxTable, newTestLogger())
	entry := &outbox.Entry{
		AggregateType: "task", AggregateID: uuid.New(),
		EventType: "x", Payload: []byte(`{}`), StreamName: "x:v1",
	}
	require.NoError(t, repo.Create(context.Background(), entry))
	require.NoError(t, repo.MarkFailed(context.Background(), entry.ID, "boom"))

	var status, errMsg string
	require.NoError(t, db.QueryRow(`SELECT status, error_message FROM orchestrator_outbox WHERE id=$1`, entry.ID).Scan(&status, &errMsg))
	assert.Equal(t, "failed", status)
	assert.Equal(t, "boom", errMsg)
}

func TestPostgresRepository_IncrementRetryDoesNotChangeStatus(t *testing.T) {
	db := dbForTest(t)
	repo := outbox.NewPostgresRepository(db, testOutboxTable, newTestLogger())
	entry := &outbox.Entry{
		AggregateType: "task", AggregateID: uuid.New(),
		EventType: "x", Payload: []byte(`{}`), StreamName: "x:v1",
	}
	require.NoError(t, repo.Create(context.Background(), entry))
	require.NoError(t, repo.IncrementRetry(context.Background(), entry.ID))

	var status string
	var rc int
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM orchestrator_outbox WHERE id=$1`, entry.ID).Scan(&status, &rc))
	assert.Equal(t, "pending", status)
	assert.Equal(t, 1, rc)
}

func TestPostgresRepository_SkipLockedIsolatesConcurrentBatches(t *testing.T) {
	db := dbForTest(t)
	repo := outbox.NewPostgresRepository(db, testOutboxTable, newTestLogger())

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

	batch1, err := outbox.NewPostgresRepository(tx1, testOutboxTable, newTestLogger()).GetPendingBatch(context.Background(), 10)
	require.NoError(t, err)
	batch2, err := outbox.NewPostgresRepository(tx2, testOutboxTable, newTestLogger()).GetPendingBatch(context.Background(), 10)
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
