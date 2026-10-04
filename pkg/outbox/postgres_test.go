package outbox_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testOutboxTable is the canonical outbox table these tests run against: the
// one `make test-deps-up` migrated into continuo_execution.
const testOutboxTable = "execution_outbox"

// testDatabase is the database whose Flyway schema holds the canonical
// execution_outbox and message_processing tables.
const testDatabase = "continuo_execution"

// testStreamPrefix starts the stream_name of every message_processing row
// these tests insert, so cleanup removes exactly those rows.
const testStreamPrefix = "pkg-outbox-test"

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// testPostgresConfig is the POSTGRES_* connection config with the database
// pinned to testDatabase, whatever POSTGRES_DB says.
func testPostgresConfig() pkgconfig.PostgresConfig {
	cfg := pkgconfig.LoadPostgres(&pkgconfig.Validator{})
	cfg.DB = testDatabase
	return cfg
}

// dbForTest connects to testDatabase with the POSTGRES_* host and credentials
// `make test-go SERVICE=pkg` sets, and never changes its schema. The processor
// under test claims every row in the table, so the suite empties execution_outbox
// before and after each test; make test-go runs one suite at a time, and no
// outbox relay (for example an execution-controller) may run against
// continuo_execution while the suite runs.
func dbForTest(t *testing.T) *sqlx.DB {
	t.Helper()
	cfg := testPostgresConfig()
	if cfg.Host == "" {
		testdeps.Unavailable(t, "POSTGRES_HOST not set; run `make test-go SERVICE=pkg`")
	}
	db, err := sqlx.Connect("postgres", cfg.DSN())
	if err != nil {
		testdeps.Unavailable(t, "postgres unreachable: %v", err)
	}
	var present bool
	require.NoError(t, db.Get(&present, `SELECT to_regclass('`+testOutboxTable+`') IS NOT NULL`))
	require.True(t, present, "%s is missing: run `make test-deps-up` to apply the Flyway migrations", testOutboxTable)
	clean := func() {
		_, _ = db.Exec(`DELETE FROM ` + testOutboxTable)
		_, _ = db.Exec(`DELETE FROM message_processing WHERE stream_name LIKE $1`, testStreamPrefix+"%")
	}
	clean()
	t.Cleanup(func() { clean(); db.Close() })
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
	require.NoError(t, db.QueryRow(`SELECT status, processed_at::text FROM `+testOutboxTable+` WHERE id=$1`, entry.ID).Scan(&status, &processedAt))
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
	require.NoError(t, db.QueryRow(`SELECT status, error_message FROM `+testOutboxTable+` WHERE id=$1`, entry.ID).Scan(&status, &errMsg))
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
	require.NoError(t, db.QueryRow(`SELECT status, retry_count FROM `+testOutboxTable+` WHERE id=$1`, entry.ID).Scan(&status, &rc))
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

// seedRowReturningEntry seeds a fresh 'pending' row with next_attempt_at left
// NULL (due now) and returns its id.
func seedRowReturningEntry(t *testing.T, db *sqlx.DB) uuid.UUID {
	return seedRow(t, db, 0)
}

// seedScheduledRow seeds a row already in the 'scheduled' state (as
// ScheduleRetry produces after a transient failure), with next_attempt_at set
// to now+delta, and returns its id. A negative delta yields a due row.
func seedScheduledRow(t *testing.T, db *sqlx.DB, delta time.Duration) uuid.UUID {
	t.Helper()
	id := seedRow(t, db, 0)
	_, err := db.Exec(
		`UPDATE `+testOutboxTable+` SET status = 'scheduled', next_attempt_at = clock_timestamp() + make_interval(secs => $1) WHERE id = $2`,
		delta.Seconds(), id,
	)
	require.NoError(t, err)
	return id
}

func TestGetPendingBatch_SkipsRowNotYetDue(t *testing.T) {
	db := dbForTest(t)
	repo := outbox.NewPostgresRepository(db, testOutboxTable, newTestLogger())
	freshPending := seedRowReturningEntry(t, db)          // next_attempt_at NULL => due now
	dueScheduled := seedScheduledRow(t, db, -time.Minute) // scheduled, deadline already passed
	notDue := seedScheduledRow(t, db, time.Hour)          // scheduled, deadline in the future

	batch, err := repo.GetPendingBatch(context.Background(), 10)
	require.NoError(t, err)

	ids := map[uuid.UUID]bool{}
	for _, e := range batch {
		ids[e.ID] = true
	}
	assert.True(t, ids[freshPending], "fresh pending row with NULL next_attempt_at must be selected")
	assert.True(t, ids[dueScheduled], "scheduled row past its deadline must be selected")
	assert.False(t, ids[notDue], "scheduled row with future next_attempt_at must be skipped")
}

func TestScheduleRetry_SetsNextAttemptAndMovesToScheduled(t *testing.T) {
	db := dbForTest(t)
	// ScheduleRetry is on the concrete repo; the processor uses it internally.
	repo := outbox.NewPostgresRepositoryForTest(db, testOutboxTable, newTestLogger())
	id := seedRowReturningEntry(t, db)

	before := time.Now()
	require.NoError(t, repo.ScheduleRetry(context.Background(), id, 30*time.Second, "connection refused"))

	var status, errMsg string
	var rc int
	var next *time.Time
	require.NoError(t, db.QueryRow(
		`SELECT status, retry_count, error_message, next_attempt_at FROM `+testOutboxTable+` WHERE id=$1`, id,
	).Scan(&status, &rc, &errMsg, &next))
	// ScheduleRetry moves the row to 'scheduled' (not 'pending') so a
	// previous-version replica's status='pending' reader cannot reclaim it
	// before next_attempt_at elapses (rolling-deploy safety).
	assert.Equal(t, "scheduled", status)
	assert.Equal(t, 1, rc)
	assert.Equal(t, "connection refused", errMsg)
	require.NotNil(t, next)
	// next_attempt_at is stamped from the DB's clock_timestamp() (statement
	// execution time), not NOW() (tx-start time) or the host clock, so assert
	// the window tolerantly rather than an exact offset.
	assert.True(t, next.After(before), "next_attempt_at should be after test start")
	assert.True(t, next.Before(before.Add(35*time.Second)), "next_attempt_at should be roughly now+30s")

	// Must not be terminal.
	assert.NotEqual(t, "failed", status)
	assert.NotEqual(t, "processed", status)
}

// The claim can read idx_<table>_claimable in index order: the index's partial
// predicate matches the claim's WHERE and its key matches the ORDER BY, so no
// sort is needed. Sequential scans and sorts are disabled for the plan so the
// check does not depend on the table's statistics: on a table last analyzed
// while empty the planner otherwise prefers a bitmap scan and an explicit sort.
func TestClaimQueryUsesTheClaimIndex(t *testing.T) {
	db := dbForTest(t)
	tx, err := db.Beginx()
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.Exec(`SET LOCAL enable_seqscan = off`)
	require.NoError(t, err)
	_, err = tx.Exec(`SET LOCAL enable_sort = off`)
	require.NoError(t, err)
	var plan string
	require.NoError(t, tx.Get(&plan, `EXPLAIN (FORMAT JSON) `+outbox.ClaimQueryForTest(testOutboxTable), 100))
	assert.Contains(t, plan, `"Index Name": "idx_execution_outbox_claimable"`)
	assert.NotContains(t, plan, `"Node Type": "Sort"`)
}
