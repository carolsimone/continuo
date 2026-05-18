package postgres_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := database.GetPostgresConnection()
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func createScheduler(t *testing.T, repo postgres.SchedulerTrackerRepository, scheduleName string) *postgres.SchedulerTracker {
	t.Helper()
	tracker := &postgres.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         scheduleName,
		Status:               run.SchedulerStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
	}
	err := repo.Create(context.Background(), tracker)
	require.NoError(t, err)
	return tracker
}

func TestSchedulerRepository_UpdateTx_UpdatesStatusAndTimestamps(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())

	tracker := createScheduler(t, repo, "test-schedule-"+uuid.New().String())
	defer db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", tracker.ScheduleID)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	now := time.Now()
	tracker.Status = run.SchedulerStatusSucceeded
	tracker.LastHeartbeatAt = &now

	err = repo.UpdateTx(context.Background(), tx, tracker)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	updated, err := repo.GetByID(context.Background(), tracker.ScheduleID)
	require.NoError(t, err)
	assert.Equal(t, run.SchedulerStatusSucceeded, updated.Status)
}

func TestSchedulerRepository_UpdateInitializationStatusTx_UpdatesStatus(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())

	tracker := createScheduler(t, repo, "test-schedule-"+uuid.New().String())
	defer db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", tracker.ScheduleID)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	err = repo.UpdateInitializationStatusTx(context.Background(), tx, tracker.ScheduleID, "pending")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	updated, err := repo.GetByID(context.Background(), tracker.ScheduleID)
	require.NoError(t, err)
	assert.Equal(t, "pending", updated.InitializationStatus)
}

func TestSchedulerRepository_CreateTx_InsertsTracker(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())

	scheduleID := uuid.New()
	tracker := &postgres.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule-" + uuid.New().String(),
		Status:               run.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		ServiceMetadataRaw:   []byte(`{"svc-a":{"manifest_version":"v3","image_tag":""}}`),
	}
	defer db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	err = repo.CreateTx(context.Background(), tx, tracker)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, run.SchedulerStatusPending, got.Status)
	assert.Equal(t, "pending", got.InitializationStatus)
	assert.Equal(t, map[string]run.ServiceMetadata{"svc-a": {ManifestVersion: "v3", ImageTag: ""}}, got.GetServiceMetadata())
}

func TestSchedulerRepository_IncrementTerminalCountTx(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	id := uuid.New()
	require.NoError(t, repo.Create(context.Background(), &postgres.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "s1-" + id.String(),
		Status:               run.SchedulerStatusPending,
		InitializationStatus: "pending",
		TotalTaskCount:       sql.NullInt32{Int32: 3, Valid: true},
		CreatedAt:            time.Now(),
	}))
	defer db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	terminal, total, err := repo.IncrementTerminalCountTx(context.Background(), tx, id)
	require.NoError(t, err)
	assert.Equal(t, int32(1), terminal)
	assert.Equal(t, int32(3), total)
	require.NoError(t, tx.Commit())
}

func TestSchedulerRepository_DecrementTerminalCountTx(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	id := uuid.New()
	require.NoError(t, repo.Create(context.Background(), &postgres.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "s1-" + id.String(),
		Status:               run.SchedulerStatusPending,
		InitializationStatus: "pending",
		TerminalTaskCount:    3,
		CreatedAt:            time.Now(),
	}))
	defer db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, repo.DecrementTerminalCountTx(context.Background(), tx, id, 2))
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, int32(1), got.TerminalTaskCount)
}

func TestSchedulerRepository_SetTotalTaskCountTx(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	id := uuid.New()
	require.NoError(t, repo.Create(context.Background(), &postgres.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "s1-" + id.String(),
		Status:               run.SchedulerStatusPending,
		InitializationStatus: "pending",
		CreatedAt:            time.Now(),
	}))
	defer db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, repo.SetTotalTaskCountTx(context.Background(), tx, id, 7))
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(context.Background(), id)
	require.NoError(t, err)
	assert.True(t, got.TotalTaskCount.Valid)
	assert.Equal(t, int32(7), got.TotalTaskCount.Int32)
}

func TestSchedulerRepository_TaskCountColumns(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())

	id := uuid.New()
	tracker := &postgres.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "test_schedule",
		Status:               run.SchedulerStatusPending,
		InitializationStatus: "pending",
		TotalTaskCount:       sql.NullInt32{Int32: 5, Valid: true},
		TerminalTaskCount:    2,
		CreatedAt:            time.Now(),
	}
	defer db.ExecContext(context.Background(), "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)
	require.NoError(t, repo.Create(context.Background(), tracker))

	got, err := repo.GetByID(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, int32(5), got.TotalTaskCount.Int32)
	assert.True(t, got.TotalTaskCount.Valid)
	assert.Equal(t, int32(2), got.TerminalTaskCount)
}

func TestSchedulerRepository_CreateAndGet_RoundTripsKindAndSourceRunID(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())

	ctx := context.Background()
	sourceRunID := uuid.New()
	// Schedule names are varchar(50); keep prefix + 8-char suffix safely under limit.
	shortSuffix := func() string { return uuid.New().String()[:8] }
	tracker := &postgres.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "krt-" + shortSuffix(),
		Status:               run.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		Kind:                 "rerun",
		SourceRunID:          &sourceRunID,
	}
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", tracker.ScheduleID)
	require.NoError(t, repo.Create(ctx, tracker))

	got, err := repo.GetByID(ctx, tracker.ScheduleID)
	require.NoError(t, err)
	assert.Equal(t, "rerun", got.Kind)
	require.NotNil(t, got.SourceRunID)
	assert.Equal(t, sourceRunID, *got.SourceRunID)

	// Default kind for a tracker constructed without setting Kind.
	defaultTracker := &postgres.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "kdf-" + shortSuffix(),
		Status:               run.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		Kind:                 "cron",
	}
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", defaultTracker.ScheduleID)
	require.NoError(t, repo.Create(ctx, defaultTracker))
	gotDefault, err := repo.GetByID(ctx, defaultTracker.ScheduleID)
	require.NoError(t, err)
	assert.Equal(t, "cron", gotDefault.Kind)
	assert.Nil(t, gotDefault.SourceRunID)

	// UpdateTx round-trip: flip kind to "rerun" and persist via UpdateTx.
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	defaultTracker.Kind = "rerun"
	flippedSource := uuid.New()
	defaultTracker.SourceRunID = &flippedSource
	require.NoError(t, repo.UpdateTx(ctx, tx, defaultTracker))
	require.NoError(t, tx.Commit())

	afterUpdate, err := repo.GetByID(ctx, defaultTracker.ScheduleID)
	require.NoError(t, err)
	assert.Equal(t, "rerun", afterUpdate.Kind)
	require.NotNil(t, afterUpdate.SourceRunID)
	assert.Equal(t, flippedSource, *afterUpdate.SourceRunID)
}

func TestSchedulerTrackerRepository_CancelTx(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	ctx := context.Background()

	id := uuid.New()
	require.NoError(t, repo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "test-cancel-" + id.String(),
		Status:               run.SchedulerStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
	}))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, repo.CancelTx(ctx, tx, id, "test-user", "test reason"))
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, run.SchedulerStatusCancelled, got.Status)
	assert.NotNil(t, got.CancelledAt)
	assert.Equal(t, "test-user", *got.CancelledBy)
	assert.Equal(t, "test reason", *got.CancellationReason)

	// Second cancel on already-cancelled row must return ErrNotCancellable.
	tx2, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx2.Rollback()
	assert.ErrorIs(t, repo.CancelTx(ctx, tx2, id, "test-user", "duplicate"), postgres.ErrNotCancellable)
}

// TestSchedulerTrackerRepository_SetTerminalTaskCountTx_GREATEST verifies that
// SetTerminalTaskCountTx never reduces an already-correct terminal_task_count.
// A duplicate or stale delivery with a lower count must leave the stored value
// unchanged; a higher count must advance it.
func TestSchedulerTrackerRepository_SetTerminalTaskCountTx_GREATEST(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	ctx := context.Background()

	id := uuid.New()
	require.NoError(t, repo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "greatest-test-" + id.String(),
		Status:               run.SchedulerStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "in_progress",
	}))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)

	// Seed to 5.
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, repo.SetTerminalTaskCountTx(ctx, tx, id, 5))
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, int32(5), got.TerminalTaskCount)

	// A lower value must not regress the count.
	tx2, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, repo.SetTerminalTaskCountTx(ctx, tx2, id, 3))
	require.NoError(t, tx2.Commit())

	got, err = repo.GetByID(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, int32(5), got.TerminalTaskCount, "lower value must not reduce terminal_task_count")

	// A higher value must advance the count.
	tx3, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, repo.SetTerminalTaskCountTx(ctx, tx3, id, 7))
	require.NoError(t, tx3.Commit())

	got, err = repo.GetByID(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, int32(7), got.TerminalTaskCount, "higher value must advance terminal_task_count")
}

// TestSchedulerTrackerRepository_TerminalCountSurvivesRetryFlow walks the full
// FAILED → RUNNING → FAILED → RUNNING → FAILED transition sequence that a
// retry-exhausting task produces in production. The aggregate increments the
// counter in memory on each FAILED and decrements it on each FAILED→RUNNING
// retry, then SaveRun writes the absolute value via SetTerminalTaskCountTx.
// If SetTerminalTaskCountTx silently ignores decrements (the pre-fix
// GREATEST behaviour), the final stored count is 3 instead of the correct 1.
//
// This is the unit-level guard against the inflation bug that broke
// TestE2E_FailurePath_RerunRebasesBothFailureSubtrees in PR #61 CI.
func TestSchedulerTrackerRepository_TerminalCountSurvivesRetryFlow(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	ctx := context.Background()

	id := uuid.New()
	require.NoError(t, repo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "retry-flow-" + id.String(),
		Status:               run.SchedulerStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
		TotalTaskCount:       sql.NullInt32{Int32: 1, Valid: true},
	}))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", id)

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
		require.NoError(t, repo.SetTerminalTaskCountTx(ctx, tx, id, s.writeValue), s.event)
		require.NoError(t, tx.Commit(), s.event)

		got, err := repo.GetByID(ctx, id)
		require.NoError(t, err, s.event)
		assert.Equal(t, s.want, got.TerminalTaskCount,
			"after event %q: stored terminal_task_count must be %d", s.event, s.want)
	}
}
