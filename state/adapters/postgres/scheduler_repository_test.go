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
	"github.com/carolsimone/continuo/state/domain/model"
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

func createScheduler(t *testing.T, repo postgres.SchedulerTrackerRepository, scheduleName string) *model.SchedulerTracker {
	t.Helper()
	tracker := &model.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         scheduleName,
		Status:               model.SchedulerStatusRunning,
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
	tracker.Status = model.SchedulerStatusSucceeded
	tracker.LastHeartbeatAt = &now

	err = repo.UpdateTx(context.Background(), tx, tracker)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	updated, err := repo.GetByID(context.Background(), tracker.ScheduleID)
	require.NoError(t, err)
	assert.Equal(t, model.SchedulerStatusSucceeded, updated.Status)
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
	tracker := &model.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule-" + uuid.New().String(),
		Status:               model.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
		ManifestVersionsRaw:  []byte(`{"svc-a":"v3"}`),
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
	assert.Equal(t, model.SchedulerStatusPending, got.Status)
	assert.Equal(t, "pending", got.InitializationStatus)
	assert.Equal(t, map[string]string{"svc-a": "v3"}, got.GetManifestVersions())
}

func TestSchedulerRepository_IncrementTerminalCountTx(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	id := uuid.New()
	require.NoError(t, repo.Create(context.Background(), &model.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "s1-" + id.String(),
		Status:               model.SchedulerStatusPending,
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
	require.NoError(t, repo.Create(context.Background(), &model.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "s1-" + id.String(),
		Status:               model.SchedulerStatusPending,
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
	require.NoError(t, repo.Create(context.Background(), &model.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "s1-" + id.String(),
		Status:               model.SchedulerStatusPending,
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
	tracker := &model.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "test_schedule",
		Status:               model.SchedulerStatusPending,
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

func TestSchedulerTrackerRepository_CancelTx(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	ctx := context.Background()

	id := uuid.New()
	require.NoError(t, repo.Create(ctx, &model.SchedulerTracker{
		ScheduleID:           id,
		ScheduleName:         "test-cancel-" + id.String(),
		Status:               model.SchedulerStatusRunning,
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
	assert.Equal(t, model.SchedulerStatusCancelled, got.Status)
	assert.NotNil(t, got.CancelledAt)
	assert.Equal(t, "test-user", *got.CancelledBy)
	assert.Equal(t, "test reason", *got.CancellationReason)
}
