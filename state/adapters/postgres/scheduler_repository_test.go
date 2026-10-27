package postgres_test

import (
	"context"
	"database/sql"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"io"
	"log/slog"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgdb "github.com/carolsimone/continuo/pkg/db"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDB(tb testing.TB) *sqlx.DB {
	tb.Helper()
	db, err := pkgdb.Open(context.Background(), pkgconfig.LoadPostgres(&pkgconfig.Validator{}), pkgconfig.PoolConfig{MaxOpenConns: 10, MaxIdleConns: 2})
	require.NoError(tb, err)
	tb.Cleanup(func() { db.Close() })
	return db
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func createScheduler(t *testing.T, repo postgres.RunTableRepository, scheduleName string) *postgres.RunRow {
	t.Helper()
	runRow := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           uuid.New(),
		ScheduleName:         scheduleName,
		Status:               run.RunStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
	}
	err := repo.Create(context.Background(), runRow)
	require.NoError(t, err)
	return runRow
}

func TestSchedulerRepository_UpdateInitializationStatusTx_UpdatesStatus(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())

	runRow := createScheduler(t, repo, "test-schedule-"+uuid.New().String())
	defer db.ExecContext(context.Background(), "DELETE FROM runs WHERE run_id = $1", runRow.ScheduleID)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	err = repo.UpdateInitializationStatusTx(context.Background(), tx, pkgevents.DefaultTenantID, runRow.ScheduleID, "pending")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	updated, err := repo.GetByID(context.Background(), pkgevents.DefaultTenantID, runRow.ScheduleID)
	require.NoError(t, err)
	assert.Equal(t, "pending", updated.InitializationStatus)
}

func TestSchedulerRepository_CreateTx_InsertsRunRow(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())

	scheduleID := uuid.New()
	runRow := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule-" + uuid.New().String(),
		Status:               run.RunStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		ServiceMetadataRaw:   []byte(`{"svc-a":{"image_tag":""}}`),
	}
	defer db.ExecContext(context.Background(), "DELETE FROM runs WHERE run_id = $1", scheduleID)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	err = repo.CreateTx(context.Background(), tx, runRow)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(context.Background(), pkgevents.DefaultTenantID, scheduleID)
	require.NoError(t, err)
	assert.Equal(t, run.RunStatusPending, got.Status)
	assert.Equal(t, "pending", got.InitializationStatus)
	gotMeta, metaErr := got.GetServiceMetadata()
	require.NoError(t, metaErr)
	assert.Equal(t, map[string]run.ServiceMetadata{"svc-a": {ImageTag: ""}}, gotMeta)
}

// TestSchedulerRepository_CreateTx_ExistingIDIsANoOp inserts the same run id
// twice in one transaction. The second insert writes nothing, returns
// ErrRunExists, and leaves the transaction usable.
func TestSchedulerRepository_CreateTx_ExistingIDIsANoOp(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())
	ctx := context.Background()

	id := uuid.New()
	runRow := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           id,
		ScheduleName:         "exists-" + id.String()[:8],
		Status:               run.RunStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "in_progress",
	}
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", id)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, repo.CreateTx(ctx, tx, runRow))
	assert.ErrorIs(t, repo.CreateTx(ctx, tx, runRow), postgres.ErrRunExists)

	// The transaction is still usable: a failed statement would have aborted it.
	_, err = repo.LoadByIDForUpdateTx(ctx, tx, pkgevents.DefaultTenantID, id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	var rows int
	require.NoError(t, db.GetContext(ctx, &rows, "SELECT count(*) FROM runs WHERE run_id = $1", id))
	assert.Equal(t, 1, rows)
}

// TestSchedulerRepository_CreateTx_ActiveNameConflictStillRaises confirms the
// run_id conflict target does not swallow the one-active-run-per-schedule
// index: a different id with the same active schedule_name still fails.
func TestSchedulerRepository_CreateTx_ActiveNameConflictStillRaises(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())
	ctx := context.Background()

	name := "active-" + uuid.New().String()[:8]
	first, second := uuid.New(), uuid.New()
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id IN ($1, $2)", first, second)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	newRunRow := func(id uuid.UUID) *postgres.RunRow {
		return &postgres.RunRow{
			TenantID:             pkgevents.DefaultTenantID,
			ScheduleID:           id,
			ScheduleName:         name,
			Status:               run.RunStatusPending,
			CreatedAt:            time.Now(),
			InitializationStatus: "in_progress",
		}
	}
	require.NoError(t, repo.CreateTx(ctx, tx, newRunRow(first)))
	assert.ErrorIs(t, repo.CreateTx(ctx, tx, newRunRow(second)), postgres.ErrActiveRunConflict)
}

func TestSchedulerRepository_SetTotalTaskCountTx(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())
	id := uuid.New()
	require.NoError(t, repo.Create(context.Background(), &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           id,
		ScheduleName:         "s1-" + id.String(),
		Status:               run.RunStatusPending,
		InitializationStatus: "pending",
		CreatedAt:            time.Now(),
	}))
	defer db.ExecContext(context.Background(), "DELETE FROM runs WHERE run_id = $1", id)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, repo.SetTotalTaskCountTx(context.Background(), tx, pkgevents.DefaultTenantID, id, 7))
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(context.Background(), pkgevents.DefaultTenantID, id)
	require.NoError(t, err)
	assert.True(t, got.TotalTaskCount.Valid)
	assert.Equal(t, int32(7), got.TotalTaskCount.Int32)
}

func TestSchedulerRepository_TaskCountColumns(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())

	id := uuid.New()
	runRow := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           id,
		ScheduleName:         "test_schedule",
		Status:               run.RunStatusPending,
		InitializationStatus: "pending",
		TotalTaskCount:       sql.NullInt32{Int32: 5, Valid: true},
		TerminalTaskCount:    2,
		CreatedAt:            time.Now(),
	}
	defer db.ExecContext(context.Background(), "DELETE FROM runs WHERE run_id = $1", id)
	require.NoError(t, repo.Create(context.Background(), runRow))

	got, err := repo.GetByID(context.Background(), pkgevents.DefaultTenantID, id)
	require.NoError(t, err)
	assert.Equal(t, int32(5), got.TotalTaskCount.Int32)
	assert.True(t, got.TotalTaskCount.Valid)
	assert.Equal(t, int32(2), got.TerminalTaskCount)
}

func TestSchedulerRepository_CreateAndGet_RoundTripsKindAndSourceRunID(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())

	ctx := context.Background()
	sourceRunID := uuid.New()
	// Schedule names are varchar(50); keep prefix + 8-char suffix safely under limit.
	shortSuffix := func() string { return uuid.New().String()[:8] }
	runRow := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           uuid.New(),
		ScheduleName:         "krt-" + shortSuffix(),
		Status:               run.RunStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		Kind:                 "rerun",
		SourceRunID:          &sourceRunID,
	}
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", runRow.ScheduleID)
	require.NoError(t, repo.Create(ctx, runRow))

	got, err := repo.GetByID(ctx, pkgevents.DefaultTenantID, runRow.ScheduleID)
	require.NoError(t, err)
	assert.Equal(t, "rerun", got.Kind)
	require.NotNil(t, got.SourceRunID)
	assert.Equal(t, sourceRunID, *got.SourceRunID)

	// Default kind for a RunRow constructed without setting Kind.
	defaultRun := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           uuid.New(),
		ScheduleName:         "kdf-" + shortSuffix(),
		Status:               run.RunStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		Kind:                 "cron",
	}
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", defaultRun.ScheduleID)
	require.NoError(t, repo.Create(ctx, defaultRun))
	gotDefault, err := repo.GetByID(ctx, pkgevents.DefaultTenantID, defaultRun.ScheduleID)
	require.NoError(t, err)
	assert.Equal(t, "cron", gotDefault.Kind)
	assert.Nil(t, gotDefault.SourceRunID)
}

func TestRunTableRepository_CancelTx(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())
	ctx := context.Background()

	id := uuid.New()
	require.NoError(t, repo.Create(ctx, &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           id,
		ScheduleName:         "test-cancel-" + id.String(),
		Status:               run.RunStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
	}))
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", id)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	cancelledAt := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, repo.CancelTx(ctx, tx, pkgevents.DefaultTenantID, id, run.RunStatusCancelled, "test-user", "test reason", cancelledAt))
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(ctx, pkgevents.DefaultTenantID, id)
	require.NoError(t, err)
	assert.Equal(t, run.RunStatusCancelled, got.Status)
	assert.NotNil(t, got.CancelledAt)
	assert.Equal(t, "test-user", *got.CancelledBy)
	assert.Equal(t, "test reason", *got.CancellationReason)
	assert.NotNil(t, got.CompletedAt, "CancelTx must stamp completed_at (cancellation is terminal)")
	// CancelTx persists the supplied instant verbatim to both cancelled_at and
	// completed_at (one timestamp per logical event), so the stored row equals
	// the value the caller produced.
	require.NotNil(t, got.CancelledAt)
	assert.True(t, got.CancelledAt.Equal(cancelledAt), "cancelled_at must equal the supplied instant")
	assert.True(t, got.CompletedAt.Equal(cancelledAt), "completed_at must equal the supplied instant")

	// Second cancel on already-cancelled row must return ErrRunNotCancellable.
	tx2, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx2.Rollback()
	assert.ErrorIs(t, repo.CancelTx(ctx, tx2, pkgevents.DefaultTenantID, id, run.RunStatusCancelled, "test-user", "duplicate", time.Now().UTC()), postgres.ErrRunNotCancellable)
}

// A cancel that waits for its stops writes the run cancelling with who
// cancelled it, why and when, and no completed_at: the run has not ended.
func TestRunTableRepository_CancelTx_CancellingLeavesCompletedAtNull(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())
	ctx := context.Background()
	id := uuid.New()
	require.NoError(t, repo.Create(ctx, &postgres.RunRow{
		TenantID:   pkgevents.DefaultTenantID,
		ScheduleID: id, ScheduleName: "cxl-cancelling-" + id.String()[:8], Status: run.RunStatusRunning,
		CreatedAt: time.Now(), InitializationStatus: "completed",
	}))
	t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", id) })

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	cancelledAt := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, repo.CancelTx(ctx, tx, pkgevents.DefaultTenantID, id, run.RunStatusCancelling, "test-user", "test reason", cancelledAt))
	assert.Error(t, repo.CancelTx(ctx, tx, pkgevents.DefaultTenantID, id, run.RunStatusFailed, "test-user", "", cancelledAt),
		"a cancel writes only cancelling or cancelled")
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(ctx, pkgevents.DefaultTenantID, id)
	require.NoError(t, err)
	assert.Equal(t, run.RunStatusCancelling, got.Status)
	require.NotNil(t, got.CancelledAt)
	assert.True(t, got.CancelledAt.Equal(cancelledAt))
	assert.Equal(t, "test-user", *got.CancelledBy)
	assert.Equal(t, "test reason", *got.CancellationReason)
	assert.Nil(t, got.CompletedAt, "a cancelling run has not ended")
}

// TestRunTableRepository_CancelTx_RefusesEveryTerminalStatus drives
// CancelTx against a run in every status. Only an active run is cancelled; a
// run in any terminal status, skipped included, returns ErrRunNotCancellable and
// its row is left exactly as it was.
func TestRunTableRepository_CancelTx_RefusesEveryTerminalStatus(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())
	ctx := context.Background()

	for _, status := range []run.RunStatus{
		run.RunStatusPending,
		run.RunStatusRunning,
		run.RunStatusSucceeded,
		run.RunStatusFailed,
		run.RunStatusCancelled,
		run.RunStatusSkipped,
	} {
		t.Run(string(status), func(t *testing.T) {
			id := uuid.New()
			require.NoError(t, repo.Create(ctx, &postgres.RunRow{
				TenantID:             pkgevents.DefaultTenantID,
				ScheduleID:           id,
				ScheduleName:         "cxl-" + string(status) + "-" + id.String()[:8],
				Status:               status,
				CreatedAt:            time.Now(),
				InitializationStatus: "completed",
			}))
			t.Cleanup(func() {
				db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", id)
			})

			tx, err := db.BeginTxx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			err = repo.CancelTx(ctx, tx, pkgevents.DefaultTenantID, id, run.RunStatusCancelled, "test-user", "table test", time.Now().UTC())
			require.NoError(t, tx.Commit())

			got, getErr := repo.GetByID(ctx, pkgevents.DefaultTenantID, id)
			require.NoError(t, getErr)
			if status.IsTerminal() {
				assert.ErrorIs(t, err, postgres.ErrRunNotCancellable)
				assert.Equal(t, status, got.Status, "a terminal run keeps its status")
				assert.Nil(t, got.CancelledAt, "a terminal run is not stamped cancelled")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, run.RunStatusCancelled, got.Status)
			assert.NotNil(t, got.CancelledAt)
		})
	}
}

// TestRunTableRepository_TerminalCountSurvivesRetryFlow walks the full
// FAILED → RUNNING → FAILED → RUNNING → FAILED transition sequence that a
// retry-exhausting task produces in production. The aggregate increments the
// counter in memory on each FAILED and decrements it on each FAILED→RUNNING
// retry, then SaveRun writes the absolute value via SetTerminalTaskCountTx.
// If SetTerminalTaskCountTx silently ignores decrements (the pre-fix
// GREATEST behaviour), the final stored count is 3 instead of the correct 1.
//
// This is the unit-level guard against the inflation bug that broke
// TestE2E_FailurePath_RerunRebasesBothFailureSubtrees in PR #61 CI.
func TestRunTableRepository_TerminalCountSurvivesRetryFlow(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())
	ctx := context.Background()

	id := uuid.New()
	require.NoError(t, repo.Create(ctx, &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           id,
		ScheduleName:         "retry-flow-" + id.String(),
		Status:               run.RunStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
		TotalTaskCount:       sql.NullInt32{Int32: 1, Valid: true},
	}))
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", id)

	// Each row models one SaveRun call from the aggregate after a task event.
	// The "want" column is what terminal_task_count must equal in the DB
	// AFTER the SaveRun commit for the corresponding event.
	steps := []struct {
		event      string
		writeValue int32
		want       int32
	}{
		{"RUNNING→FAILED(0): aggregate ++", 1, 1},
		{"FAILED→RUNNING(1): aggregate --", 0, 0}, // decrement MUST land
		{"RUNNING→FAILED(1): aggregate ++", 1, 1},
		{"FAILED→RUNNING(2): aggregate --", 0, 0}, // decrement MUST land
		{"RUNNING→FAILED(2): aggregate ++", 1, 1}, // final terminal
	}

	for _, s := range steps {
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, repo.SetTerminalTaskCountTx(ctx, tx, pkgevents.DefaultTenantID, id, s.writeValue), s.event)
		require.NoError(t, tx.Commit(), s.event)

		got, err := repo.GetByID(ctx, pkgevents.DefaultTenantID, id)
		require.NoError(t, err, s.event)
		assert.Equal(t, s.want, got.TerminalTaskCount,
			"after event %q: stored terminal_task_count must be %d", s.event, s.want)
	}
}

// TestRunTableRepository_SetTerminalTaskCountTx_AbsoluteWrite asserts
// that SetTerminalTaskCountTx writes the value absolutely — including
// decreasing writes. This replaces the deleted _GREATEST test, which guarded
// the old monotonic-MAX semantics that the aggregate refactor obsoleted.
func TestRunTableRepository_SetTerminalTaskCountTx_AbsoluteWrite(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewRunTableRepository(db, discardLogger())
	ctx := context.Background()

	id := uuid.New()
	require.NoError(t, repo.Create(ctx, &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           id,
		ScheduleName:         "abs-" + id.String()[:8],
		Status:               run.RunStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "in_progress",
	}))
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", id)

	// Set to 5, then 3 (decrease), then 7 (increase). Each must land verbatim.
	for _, want := range []int32{5, 3, 7} {
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, repo.SetTerminalTaskCountTx(ctx, tx, pkgevents.DefaultTenantID, id, want))
		require.NoError(t, tx.Commit())

		got, err := repo.GetByID(ctx, pkgevents.DefaultTenantID, id)
		require.NoError(t, err)
		assert.Equal(t, want, got.TerminalTaskCount, "absolute write must land verbatim")
	}
}
