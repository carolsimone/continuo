package test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
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

	// Apply migrations from db/migration/state/V*.sql so the test schema
	// stays in lock-step with production. See state/test/migrations.go.
	require.NoError(t, ApplyMigrations(db.DB), "Failed to apply migrations")

	// Cleanup function
	cleanup := func() {
		db.Close()
		if err := postgresContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return db, cleanup
}

// TestRunRepository_Create tests creating a run record
func TestRunRepository_Create(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewRunRepository(db, logger)

	// Create test data
	scheduleID := uuid.New()
	runRow := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule",
		Status:               run.RunStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}

	// Execute
	err := repo.Create(context.Background(), runRow)

	// Assert
	require.NoError(t, err, "Failed to create run")

	// Verify the record exists in the database
	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM runs WHERE run_id = $1", scheduleID)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "Expected 1 record in database")
}

// TestRunRepository_Create_DuplicateKey tests duplicate key handling
func TestRunRepository_Create_DuplicateKey(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewRunRepository(db, logger)

	// Create test data
	scheduleID := uuid.New()
	runRow := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule",
		Status:               run.RunStatusPending,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}

	// Create first record
	err := repo.Create(context.Background(), runRow)
	require.NoError(t, err, "Failed to create first run")

	// Try to create duplicate
	err = repo.Create(context.Background(), runRow)

	// Assert
	assert.ErrorIs(t, err, postgres.ErrDuplicateKey, "Expected ErrDuplicateKey")
}

// TestRunRepository_GetByID tests retrieving a run by ID
func TestRunRepository_GetByID(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewRunRepository(db, logger)

	// Create test data
	scheduleID := uuid.New()
	startedAt := time.Now().Add(-1 * time.Hour)
	runRow := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule-get",
		Status:               run.RunStatusRunning,
		CreatedAt:            time.Now().Add(-2 * time.Hour),
		StartedAt:            &startedAt,
		InitializationStatus: "pending",
	}

	// Create record
	err := repo.Create(context.Background(), runRow)
	require.NoError(t, err, "Failed to create run")

	// Execute GetByID
	retrieved, err := repo.GetByID(context.Background(), pkgevents.DefaultTenantID, scheduleID)

	// Assert
	require.NoError(t, err, "Failed to get run")
	require.NotNil(t, retrieved, "Retrieved run is nil")
	assert.Equal(t, scheduleID, retrieved.ScheduleID)
	assert.Equal(t, "test-schedule-get", retrieved.ScheduleName)
	assert.Equal(t, run.RunStatusRunning, retrieved.Status)
	assert.NotNil(t, retrieved.StartedAt)
}

// TestRunRepository_GetByID_NotFound tests error handling for non-existent record
func TestRunRepository_GetByID_NotFound(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewRunRepository(db, logger)

	// Try to get non-existent record
	nonExistentID := uuid.New()
	retrieved, err := repo.GetByID(context.Background(), pkgevents.DefaultTenantID, nonExistentID)

	// Assert
	assert.ErrorIs(t, err, postgres.ErrNotFound, "Expected ErrNotFound")
	assert.Nil(t, retrieved, "Expected nil run")
}

// TestRunRepository_Create_WithNullableFields tests creating with nullable fields
func TestRunRepository_Create_WithNullableFields(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	repo := postgres.NewRunRepository(db, logger)

	// Create test data with nullable fields set
	scheduleID := uuid.New()
	cancelledBy := "test-user"
	cancellationReason := "Test cancellation"
	cancelledAt := time.Now()

	runRow := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           scheduleID,
		ScheduleName:         "test-schedule-cancelled",
		Status:               run.RunStatusCancelled,
		CreatedAt:            time.Now(),
		CancelledAt:          &cancelledAt,
		CancelledBy:          &cancelledBy,
		CancellationReason:   &cancellationReason,
		InitializationStatus: "pending",
	}

	// Execute
	err := repo.Create(context.Background(), runRow)
	require.NoError(t, err, "Failed to create run")

	// Retrieve and verify
	retrieved, err := repo.GetByID(context.Background(), pkgevents.DefaultTenantID, scheduleID)
	require.NoError(t, err, "Failed to get run")
	assert.NotNil(t, retrieved.CancelledBy)
	assert.Equal(t, "test-user", *retrieved.CancelledBy)
	assert.NotNil(t, retrieved.CancellationReason)
	assert.Equal(t, "Test cancellation", *retrieved.CancellationReason)
}

// ---- Schedule Catalog Repository Tests ----
// schedule_catalog is created by db/migration/state/V*.sql, which setupPostgres
// applies via ApplyMigrations — these tests just consume it.

func TestCatalogRepository_ExistsActive(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	repo := postgres.NewCatalogRepository(db, slog.Default())
	ctx := context.Background()

	seedCatalog(t, db, repo, []string{"daily"})

	exists, err := repo.ExistsActive(ctx, pkgevents.DefaultTenantID, "daily")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = repo.ExistsActive(ctx, pkgevents.DefaultTenantID, "unknown")
	require.NoError(t, err)
	assert.False(t, exists)
}

// seedCatalog upserts the given schedule names as active rows via the tx-bound
// UpsertAllTx, committing in its own transaction. Used to set up catalog state
// for tests that exercise read-side methods.
func seedCatalog(t *testing.T, db *sqlx.DB, repo *postgres.CatalogRepository, names []string) {
	t.Helper()
	ctx := context.Background()
	meta := make(map[string]map[string]run.ServiceMetadata, len(names))
	for _, n := range names {
		meta[n] = map[string]run.ServiceMetadata{"svc-a": {ImageTag: ""}}
	}
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, repo.UpsertAllTx(ctx, tx, pkgevents.DefaultTenantID, names, meta))
	require.NoError(t, tx.Commit())
}

// softDeleteAllCatalog soft-deletes every active catalog row via the tx-bound
// SoftDeleteAbsentTx with an empty present-set, committing in its own
// transaction.
func softDeleteAllCatalog(t *testing.T, db *sqlx.DB, repo *postgres.CatalogRepository) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, repo.SoftDeleteAbsentTx(ctx, tx, pkgevents.DefaultTenantID, []string{}))
	require.NoError(t, tx.Commit())
}

func TestCatalogRepository_ExistsActive_SoftDeletedReturnsFalse(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	repo := postgres.NewCatalogRepository(db, slog.Default())
	ctx := context.Background()

	seedCatalog(t, db, repo, []string{"daily"})
	softDeleteAllCatalog(t, db, repo)

	exists, err := repo.ExistsActive(ctx, pkgevents.DefaultTenantID, "daily")
	require.NoError(t, err)
	assert.False(t, exists)
}

// ---- GetLastRunPerSchedule Tests ----

func TestSchedulerRepository_GetLastRunPerSchedule(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	repo := postgres.NewRunRepository(db, slog.Default())
	ctx := context.Background()

	// Insert two runs for "daily", one for "hourly"
	dailyOld := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           uuid.New(),
		ScheduleName:         "daily",
		Status:               run.RunStatusSucceeded,
		CreatedAt:            time.Now().Add(-2 * time.Hour),
		InitializationStatus: "completed",
	}
	dailyNew := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           uuid.New(),
		ScheduleName:         "daily",
		Status:               run.RunStatusRunning,
		CreatedAt:            time.Now(),
		InitializationStatus: "pending",
	}
	hourly := &postgres.RunRow{
		TenantID:             pkgevents.DefaultTenantID,
		ScheduleID:           uuid.New(),
		ScheduleName:         "hourly",
		Status:               run.RunStatusFailed,
		CreatedAt:            time.Now().Add(-30 * time.Minute),
		InitializationStatus: "completed",
	}
	require.NoError(t, repo.Create(ctx, dailyOld))
	require.NoError(t, repo.Create(ctx, dailyNew))
	require.NoError(t, repo.Create(ctx, hourly))

	lastRuns, err := repo.GetLastRunPerSchedule(ctx, pkgevents.DefaultTenantID)
	require.NoError(t, err)

	assert.Contains(t, lastRuns, "daily")
	assert.Equal(t, run.RunStatusRunning, lastRuns["daily"].Status)
	assert.True(t, lastRuns["daily"].IsActive)

	assert.Contains(t, lastRuns, "hourly")
	assert.Equal(t, run.RunStatusFailed, lastRuns["hourly"].Status)
	assert.False(t, lastRuns["hourly"].IsActive)
}

func TestRunRepository_GetActiveRun(t *testing.T) {
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	repo := postgres.NewRunRepository(db, logger)
	ctx := context.Background()

	t.Run("returns nil when no active run exists", func(t *testing.T) {
		result, err := repo.GetActiveRun(ctx, pkgevents.DefaultTenantID, "nonexistent-schedule")
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("returns the run when a pending run exists", func(t *testing.T) {
		schedID := uuid.New()
		runRow := &postgres.RunRow{
			TenantID:             pkgevents.DefaultTenantID,
			ScheduleID:           schedID,
			ScheduleName:         "active-pending",
			Status:               run.RunStatusPending,
			CreatedAt:            time.Now(),
			InitializationStatus: "pending",
		}
		require.NoError(t, repo.Create(ctx, runRow))

		result, err := repo.GetActiveRun(ctx, pkgevents.DefaultTenantID, "active-pending")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, schedID, result.RunID())
		assert.Equal(t, run.RunStatusPending, result.Status())
	})

	t.Run("returns the run when a running run exists", func(t *testing.T) {
		schedID := uuid.New()
		runRow := &postgres.RunRow{
			TenantID:             pkgevents.DefaultTenantID,
			ScheduleID:           schedID,
			ScheduleName:         "active-running",
			Status:               run.RunStatusRunning,
			CreatedAt:            time.Now(),
			InitializationStatus: "in_progress",
		}
		require.NoError(t, repo.Create(ctx, runRow))

		result, err := repo.GetActiveRun(ctx, pkgevents.DefaultTenantID, "active-running")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, schedID, result.RunID())
	})

	t.Run("returns nil after run is cancelled", func(t *testing.T) {
		schedID := uuid.New()
		runRow := &postgres.RunRow{
			TenantID:             pkgevents.DefaultTenantID,
			ScheduleID:           schedID,
			ScheduleName:         "post-cancel",
			Status:               run.RunStatusRunning,
			CreatedAt:            time.Now(),
			InitializationStatus: "pending",
		}
		require.NoError(t, repo.Create(ctx, runRow))
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, repo.CancelTx(ctx, tx, pkgevents.DefaultTenantID, schedID, run.RunStatusCancelled, "test", "", time.Now().UTC()))
		require.NoError(t, tx.Commit())

		result, err := repo.GetActiveRun(ctx, pkgevents.DefaultTenantID, "post-cancel")
		require.NoError(t, err)
		assert.Nil(t, result)
	})
}
