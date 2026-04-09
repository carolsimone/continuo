package test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupPostgres starts a PostgreSQL testcontainer and runs migrations
func setupPostgres(t *testing.T) (*sqlx.DB, func()) {
	ctx := context.Background()

	// Start PostgreSQL container
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	postgresContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "Failed to start PostgreSQL container")

	// Get container host and port
	host, err := postgresContainer.Host(ctx)
	require.NoError(t, err, "Failed to get container host")

	port, err := postgresContainer.MappedPort(ctx, "5432")
	require.NoError(t, err, "Failed to get container port")

	// Force IPv4 for macOS compatibility
	if host == "localhost" {
		host = "127.0.0.1"
	}

	// Create connection string
	connStr := "host=" + host + " port=" + port.Port() + " user=testuser password=testpass dbname=testdb sslmode=disable"

	// Retry connection to database (port mapping may take a moment)
	var db *sqlx.DB
	maxRetries := 10
	for i := 0; i < maxRetries; i++ {
		db, err = sqlx.Connect("postgres", connStr)
		if err == nil {
			break
		}
		t.Logf("Connection attempt %d/%d failed, retrying...", i+1, maxRetries)
		time.Sleep(500 * time.Millisecond)
	}
	require.NoError(t, err, "Failed to connect to database after retries")

	// Run migration - create scheduler_tracker table
	migrationSQL := `
		CREATE TABLE scheduler_tracker (
			schedule_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			schedule_name       VARCHAR(50) NOT NULL,
			status              VARCHAR(20) NOT NULL CHECK (status IN (
										'pending',
										'running',
										'succeeded',
										'failed',
										'cancelled'
									)),
			created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at          TIMESTAMPTZ,
			completed_at        TIMESTAMPTZ,
			last_heartbeat_at   TIMESTAMPTZ,
			cancelled_at        TIMESTAMPTZ,
			cancelled_by        VARCHAR(255),
			cancellation_reason TEXT,
			initialization_status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (initialization_status IN (
									'pending',
									'in_progress',
									'completed',
									'failed'
								)),
			manifest_versions   JSONB NOT NULL DEFAULT '{}',
			CONSTRAINT valid_timestamps CHECK (
				(started_at IS NULL OR started_at >= created_at) AND
				(completed_at IS NULL OR completed_at >= started_at)
			)
		);

		CREATE INDEX idx_scheduler_tracker_init_status ON scheduler_tracker(schedule_id, initialization_status);

		CREATE TABLE task_tracker (
			task_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			schedule_id         UUID NOT NULL REFERENCES scheduler_tracker(schedule_id) ON DELETE CASCADE,
			created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			service_name        VARCHAR(100) NOT NULL,
			schema_name         VARCHAR(100) NOT NULL,
			table_name          VARCHAR(100) NOT NULL,
			status              VARCHAR(20) NOT NULL CHECK (status IN (
										'pending',
										'running',
										'succeeded',
										'failed',
										'cancelled'
									)),
			retry_count         INTEGER NOT NULL DEFAULT 0,
			max_retries         INTEGER NOT NULL,
			job_name            VARCHAR(63) NOT NULL,
			cancelled_at        TIMESTAMPTZ,
			cancelled_by        VARCHAR(255)
		);

		CREATE INDEX idx_task_tracker_schedule_id ON task_tracker(schedule_id);
		CREATE INDEX idx_task_tracker_status ON task_tracker(status);
		CREATE INDEX idx_task_tracker_job_name ON task_tracker(job_name);

		CREATE TABLE task_execution (
			ID                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			task_id             UUID NOT NULL REFERENCES task_tracker(task_id) ON DELETE CASCADE,
			created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at          TIMESTAMPTZ,
			completed_at        TIMESTAMPTZ,
			execution_time_seconds DECIMAL(10, 3),
			executor_id         VARCHAR(100),
			k8s_job_name        VARCHAR(253),
			error_message       TEXT,
			cancelled_at        TIMESTAMPTZ,
			cancelled_by        VARCHAR(255),
			cancellation_reason TEXT,
			CONSTRAINT valid_execution_timestamps CHECK (
				(started_at IS NULL OR started_at >= created_at) AND
				(completed_at IS NULL OR completed_at >= started_at)
			)
		);

		CREATE INDEX idx_task_execution_task_id ON task_execution(task_id);
		CREATE INDEX idx_task_execution_created_at ON task_execution(created_at DESC);
	`

	_, err = db.Exec(migrationSQL)
	require.NoError(t, err, "Failed to run migration")

	// Cleanup function
	cleanup := func() {
		db.Close()
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return db, cleanup
}

// TestSchedulerTrackerRepository_Create tests creating a scheduler_tracker record
func TestSchedulerTrackerRepository_Create(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewSchedulerTrackerRepository(db, logger)

	// Create test data
	scheduleID := uuid.New()
	tracker := &model.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule",
		Status:               model.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}

	// Execute
	err := repo.Create(context.Background(), tracker)

	// Assert
	require.NoError(t, err, "Failed to create scheduler_tracker")

	// Verify the record exists in the database
	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM scheduler_tracker WHERE schedule_id = $1", scheduleID)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "Expected 1 record in database")
}

// TestSchedulerTrackerRepository_Create_DuplicateKey tests duplicate key handling
func TestSchedulerTrackerRepository_Create_DuplicateKey(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewSchedulerTrackerRepository(db, logger)

	// Create test data
	scheduleID := uuid.New()
	tracker := &model.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule",
		Status:               model.SchedulerStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}

	// Create first record
	err := repo.Create(context.Background(), tracker)
	require.NoError(t, err, "Failed to create first scheduler_tracker")

	// Try to create duplicate
	err = repo.Create(context.Background(), tracker)

	// Assert
	assert.ErrorIs(t, err, postgres.ErrDuplicateKey, "Expected ErrDuplicateKey")
}

// TestSchedulerTrackerRepository_GetByID tests retrieving a scheduler_tracker by ID
func TestSchedulerTrackerRepository_GetByID(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewSchedulerTrackerRepository(db, logger)

	// Create test data
	scheduleID := uuid.New()
	startedAt := time.Now().Add(-1 * time.Hour)
	tracker := &model.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule-get",
		Status:               model.SchedulerStatusRunning,
		CreatedAt:            time.Now().Add(-2 * time.Hour),
		StartedAt:            &startedAt,
		InitializationStatus: "pending",
	}

	// Create record
	err := repo.Create(context.Background(), tracker)
	require.NoError(t, err, "Failed to create scheduler_tracker")

	// Execute GetByID
	retrieved, err := repo.GetByID(context.Background(), scheduleID)

	// Assert
	require.NoError(t, err, "Failed to get scheduler_tracker")
	require.NotNil(t, retrieved, "Retrieved tracker is nil")
	assert.Equal(t, scheduleID, retrieved.ScheduleID)
	assert.Equal(t, "test-schedule-get", retrieved.ScheduleName)
	assert.Equal(t, model.SchedulerStatusRunning, retrieved.Status)
	assert.NotNil(t, retrieved.StartedAt)
}

// TestSchedulerTrackerRepository_GetByID_NotFound tests error handling for non-existent record
func TestSchedulerTrackerRepository_GetByID_NotFound(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewSchedulerTrackerRepository(db, logger)

	// Try to get non-existent record
	nonExistentID := uuid.New()
	retrieved, err := repo.GetByID(context.Background(), nonExistentID)

	// Assert
	assert.ErrorIs(t, err, postgres.ErrNotFound, "Expected ErrNotFound")
	assert.Nil(t, retrieved, "Expected nil tracker")
}

// TestSchedulerTrackerRepository_Create_WithNullableFields tests creating with nullable fields
func TestSchedulerTrackerRepository_Create_WithNullableFields(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewSchedulerTrackerRepository(db, logger)

	// Create test data with nullable fields set
	scheduleID := uuid.New()
	cancelledBy := "test-user"
	cancellationReason := "Test cancellation"
	cancelledAt := time.Now()

	tracker := &model.SchedulerTracker{
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule-cancelled",
		Status:               model.SchedulerStatusCancelled,
		CreatedAt:            time.Now(),
		CancelledAt:          &cancelledAt,
		CancelledBy:          &cancelledBy,
		CancellationReason:   &cancellationReason,
		InitializationStatus: "pending",
	}

	// Execute
	err := repo.Create(context.Background(), tracker)
	require.NoError(t, err, "Failed to create scheduler_tracker")

	// Retrieve and verify
	retrieved, err := repo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err, "Failed to get scheduler_tracker")
	assert.NotNil(t, retrieved.CancelledBy)
	assert.Equal(t, "test-user", *retrieved.CancelledBy)
	assert.NotNil(t, retrieved.CancellationReason)
	assert.Equal(t, "Test cancellation", *retrieved.CancellationReason)
}

// ---- Schedule Catalog Repository Tests ----

func setupCatalogSchema(t *testing.T, db *sqlx.DB) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schedule_catalog (
			schedule_name VARCHAR(50) PRIMARY KEY,
			first_seen_at TIMESTAMPTZ NOT NULL,
			last_seen_at  TIMESTAMPTZ NOT NULL,
			removed_at    TIMESTAMPTZ,
			manifest_versions JSONB NOT NULL DEFAULT '{}'
		)
	`)
	require.NoError(t, err, "Failed to create schedule_catalog table")
}

func TestScheduleCatalogRepository_UpsertAll_Insert(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	setupCatalogSchema(t, db)

	repo := postgres.NewScheduleCatalogRepository(db, slog.Default())
	ctx := context.Background()
	manifestVersions := map[string]string{"svc-a": "v3", "svc-b": "v5"}

	err := repo.UpsertAll(ctx, []string{"daily", "hourly"}, manifestVersions)
	require.NoError(t, err)

	active, err := repo.ListActive(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"daily", "hourly"}, active)

	versions, err := repo.GetManifestVersions(ctx, "daily")
	require.NoError(t, err)
	assert.Equal(t, manifestVersions, versions)
}

func TestScheduleCatalogRepository_UpsertAll_ReactivatesRemoved(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	setupCatalogSchema(t, db)

	repo := postgres.NewScheduleCatalogRepository(db, slog.Default())
	ctx := context.Background()

	// Insert and then soft-delete "daily"
	require.NoError(t, repo.UpsertAll(ctx, []string{"daily"}, map[string]string{"svc-a": "v1"}))
	require.NoError(t, repo.SoftDeleteAbsent(ctx, []string{})) // delete all

	// Upsert again — should reactivate
	require.NoError(t, repo.UpsertAll(ctx, []string{"daily"}, map[string]string{"svc-a": "v2"}))

	active, err := repo.ListActive(ctx)
	require.NoError(t, err)
	assert.Contains(t, active, "daily")

	versions, err := repo.GetManifestVersions(ctx, "daily")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"svc-a": "v2"}, versions)
}

func TestScheduleCatalogRepository_SoftDeleteAbsent(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	setupCatalogSchema(t, db)

	repo := postgres.NewScheduleCatalogRepository(db, slog.Default())
	ctx := context.Background()

	require.NoError(t, repo.UpsertAll(ctx, []string{"daily", "hourly", "weekly"}, map[string]string{"svc-a": "v1"}))
	// Payload now only contains daily and hourly — weekly should be soft-deleted
	require.NoError(t, repo.SoftDeleteAbsent(ctx, []string{"daily", "hourly"}))

	active, err := repo.ListActive(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"daily", "hourly"}, active)
	assert.NotContains(t, active, "weekly")
}

func TestScheduleCatalogRepository_ExistsActive(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	setupCatalogSchema(t, db)

	repo := postgres.NewScheduleCatalogRepository(db, slog.Default())
	ctx := context.Background()

	require.NoError(t, repo.UpsertAll(ctx, []string{"daily"}, map[string]string{"svc-a": "v1"}))

	exists, err := repo.ExistsActive(ctx, "daily")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = repo.ExistsActive(ctx, "unknown")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestScheduleCatalogRepository_ExistsActive_SoftDeletedReturnsFalse(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()
	setupCatalogSchema(t, db)

	repo := postgres.NewScheduleCatalogRepository(db, slog.Default())
	ctx := context.Background()

	require.NoError(t, repo.UpsertAll(ctx, []string{"daily"}, map[string]string{"svc-a": "v1"}))
	require.NoError(t, repo.SoftDeleteAbsent(ctx, []string{}))

	exists, err := repo.ExistsActive(ctx, "daily")
	require.NoError(t, err)
	assert.False(t, exists)
}

// ---- GetLastRunPerSchedule Tests ----

func TestSchedulerRepository_GetLastRunPerSchedule(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	repo := postgres.NewSchedulerTrackerRepository(db, slog.Default())
	ctx := context.Background()

	// Insert two runs for "daily", one for "hourly"
	dailyOld := &model.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "daily",
		Status:               model.SchedulerStatusSucceeded,
		CreatedAt:            time.Now().Add(-2 * time.Hour),
		InitializationStatus: "completed",
	}
	dailyNew := &model.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "daily",
		Status:               model.SchedulerStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}
	hourly := &model.SchedulerTracker{
		ScheduleID:           uuid.New(),
		ScheduleName:         "hourly",
		Status:               model.SchedulerStatusFailed,
		CreatedAt:            time.Now().Add(-30 * time.Minute),
		InitializationStatus: "completed",
	}
	require.NoError(t, repo.Create(ctx, dailyOld))
	require.NoError(t, repo.Create(ctx, dailyNew))
	require.NoError(t, repo.Create(ctx, hourly))

	lastRuns, err := repo.GetLastRunPerSchedule(ctx)
	require.NoError(t, err)

	assert.Contains(t, lastRuns, "daily")
	assert.Equal(t, model.SchedulerStatusRunning, lastRuns["daily"].Status)
	assert.True(t, lastRuns["daily"].IsRunning)

	assert.Contains(t, lastRuns, "hourly")
	assert.Equal(t, model.SchedulerStatusFailed, lastRuns["hourly"].Status)
	assert.False(t, lastRuns["hourly"].IsRunning)
}

func TestSchedulerTrackerRepository_GetActiveScheduler(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	repo := postgres.NewSchedulerTrackerRepository(db, logger)
	ctx := context.Background()

	t.Run("returns nil when no active run exists", func(t *testing.T) {
		result, err := repo.GetActiveScheduler(ctx, "nonexistent-schedule")
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("returns tracker when pending run exists", func(t *testing.T) {
		schedID := uuid.New()
		tracker := &model.SchedulerTracker{
			ScheduleID:           schedID,
			ScheduleName:         "active-pending",
			Status:               model.SchedulerStatusPending,
			CreatedAt:            time.Now(),
			InitializationStatus: "pending",
		}
		require.NoError(t, repo.Create(ctx, tracker))

		result, err := repo.GetActiveScheduler(ctx, "active-pending")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, schedID, result.ScheduleID)
		assert.Equal(t, model.SchedulerStatusPending, result.Status)
	})

	t.Run("returns tracker when running run exists", func(t *testing.T) {
		schedID := uuid.New()
		tracker := &model.SchedulerTracker{
			ScheduleID:           schedID,
			ScheduleName:         "active-running",
			Status:               model.SchedulerStatusRunning,
			CreatedAt:            time.Now(),
			InitializationStatus: "in_progress",
		}
		require.NoError(t, repo.Create(ctx, tracker))

		result, err := repo.GetActiveScheduler(ctx, "active-running")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, schedID, result.ScheduleID)
	})

	t.Run("returns nil after run is cancelled", func(t *testing.T) {
		schedID := uuid.New()
		tracker := &model.SchedulerTracker{
			ScheduleID:           schedID,
			ScheduleName:         "post-cancel",
			Status:               model.SchedulerStatusRunning,
			CreatedAt:            time.Now(),
			InitializationStatus: "pending",
		}
		require.NoError(t, repo.Create(ctx, tracker))
		require.NoError(t, repo.Cancel(ctx, schedID, "test", ""))

		result, err := repo.GetActiveScheduler(ctx, "post-cancel")
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}

func TestSchedulerTrackerRepository_Cancel_AlreadyTerminal(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	repo := postgres.NewSchedulerTrackerRepository(db, logger)
	ctx := context.Background()

	schedID := uuid.New()
	tracker := &model.SchedulerTracker{
		ScheduleID:           schedID,
		ScheduleName:         "already-terminal",
		Status:               model.SchedulerStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}
	require.NoError(t, repo.Create(ctx, tracker))

	// First cancel succeeds
	require.NoError(t, repo.Cancel(ctx, schedID, "user", ""))

	// Second cancel returns ErrNotCancellable
	err := repo.Cancel(ctx, schedID, "user", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, postgres.ErrNotCancellable)
}
