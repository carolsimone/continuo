package postgres_test

import (
	"context"
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
