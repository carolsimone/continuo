package test

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// setupTestDB starts a PostgreSQL testcontainer with migrations
func setupTestDB(t *testing.T) (*sqlx.DB, *slog.Logger, func()) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	db, cleanup := setupPostgres(t)

	return db, logger, cleanup
}

// cleanupOutboxTable removes all test entries from the outbox table
func cleanupOutboxTable(t *testing.T, db *sqlx.DB) {
	_, err := db.Exec("DELETE FROM k8s_status_outbox WHERE service_name LIKE 'test-%'")
	if err != nil {
		t.Logf("Warning: Failed to cleanup outbox table: %v", err)
	}
}

// createStuckEntry creates a stuck outbox entry for testing with default service name
func createStuckEntry(t *testing.T, db *sqlx.DB, retryCount int, maxRetries int, createdAt time.Time) uuid.UUID {
	return createStuckEntryWithService(t, db, "test-stuck-resolver", retryCount, maxRetries, createdAt)
}

// createStuckEntryWithService creates a stuck outbox entry for testing with custom service name
func createStuckEntryWithService(t *testing.T, db *sqlx.DB, serviceName string, retryCount int, maxRetries int, createdAt time.Time) uuid.UUID {
	entryID := uuid.New()
	taskID := uuid.New()
	scheduleID := uuid.New()

	query := `
		INSERT INTO k8s_status_outbox (
			id, event_type, stream_name,
			task_id, schedule_id, schedule_name, service_name, schema_name,
			table_name, job_name,
			status, created_at, outbox_retry_count, max_retries, outbox_error_message
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
		)
	`

	_, err := db.Exec(query,
		entryID, "task_retry", "task.retry:v1",
		taskID, scheduleID, "test-schedule", serviceName, "public",
		"test-table", "test-job",
		string(model.OutboxStatusPending), createdAt, retryCount, maxRetries, "Original test error",
	)
	if err != nil {
		t.Fatalf("Failed to create stuck entry: %v", err)
	}

	return entryID
}

// getOutboxEntry retrieves an outbox entry by ID
func getOutboxEntry(t *testing.T, db *sqlx.DB, entryID uuid.UUID) *model.K8sStatusOutboxEntry {
	query := `
		SELECT id, event_type, stream_name,
		       task_id, schedule_id, schedule_name, service_name, schema_name,
		       table_name, job_name,
		       COALESCE(error_message, '') as error_message,
		       COALESCE(task_retry_count, 0) as task_retry_count,
		       COALESCE(check_after, 0) as check_after,
		       COALESCE(update_task_status, false) as update_task_status,
		       COALESCE(new_task_status, '') as new_task_status,
		       COALESCE(new_retry_count, 0) as new_retry_count,
		       COALESCE(create_execution, false) as create_execution,
		       execution_started_at, execution_completed_at,
		       COALESCE(execution_seconds, 0) as execution_seconds,
		       status, created_at, processed_at,
		       outbox_retry_count, max_retries,
		       COALESCE(outbox_error_message, '') as outbox_error_message
		FROM k8s_status_outbox
		WHERE id = $1
	`

	var entry model.K8sStatusOutboxEntry
	err := db.Get(&entry, query, entryID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		t.Fatalf("Failed to get outbox entry: %v", err)
	}

	return &entry
}

// TestStuckEntryResolver_GetStuckEntries tests the repository method
func TestStuckEntryResolver_GetStuckEntries(t *testing.T) {
	db, logger, cleanup := setupTestDB(t)
	defer cleanup()
	defer cleanupOutboxTable(t, db)

	ctx := context.Background()
	repo := postgres.NewOutboxRepository(db, logger)

	t.Run("NoStuckEntries", func(t *testing.T) {
		entries, err := repo.GetStuckEntries(ctx, 50, 60)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(entries) != 0 {
			t.Errorf("Expected 0 entries, got: %d", len(entries))
		}
	})

	t.Run("FindStuckEntries", func(t *testing.T) {
		// Create a stuck entry (retry_count >= max_retries, created 2 minutes ago)
		stuckTime := time.Now().Add(-2 * time.Minute)
		stuckID1 := createStuckEntry(t, db, 5, 3, stuckTime)
		stuckID2 := createStuckEntry(t, db, 4, 3, stuckTime.Add(-30*time.Second))

		// Create a non-stuck entry (retry_count < max_retries)
		createStuckEntry(t, db, 2, 3, stuckTime)

		// Create a stuck entry but too recent (< 60 seconds)
		createStuckEntry(t, db, 5, 3, time.Now().Add(-30*time.Second))

		// Query for stuck entries
		entries, err := repo.GetStuckEntries(ctx, 50, 60)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(entries) != 2 {
			t.Errorf("Expected 2 stuck entries, got: %d", len(entries))
		}

		// Verify entries are returned in FIFO order (oldest first)
		if len(entries) == 2 {
			if entries[0].ID != stuckID2 {
				t.Errorf("Expected oldest entry first, got: %s", entries[0].ID)
			}
			if entries[1].ID != stuckID1 {
				t.Errorf("Expected second oldest entry second, got: %s", entries[1].ID)
			}
		}
	})

	t.Run("RespectBatchLimit", func(t *testing.T) {
		cleanupOutboxTable(t, db)

		// Create 5 stuck entries
		stuckTime := time.Now().Add(-2 * time.Minute)
		for i := 0; i < 5; i++ {
			createStuckEntry(t, db, 5, 3, stuckTime.Add(time.Duration(-i)*time.Second))
		}

		// Query with limit of 3
		entries, err := repo.GetStuckEntries(ctx, 3, 60)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(entries) != 3 {
			t.Errorf("Expected 3 entries (batch limit), got: %d", len(entries))
		}
	})
}

// TestStuckEntryResolver_ForceMarkFailed tests the repository method
func TestStuckEntryResolver_ForceMarkFailed(t *testing.T) {
	db, logger, cleanup := setupTestDB(t)
	defer cleanup()
	defer cleanupOutboxTable(t, db)

	ctx := context.Background()
	repo := postgres.NewOutboxRepository(db, logger)

	t.Run("SuccessfulMarkFailed", func(t *testing.T) {
		entryID := createStuckEntry(t, db, 5, 3, time.Now().Add(-2*time.Minute))

		errorMsg := "STUCK_ENTRY_RESOLVED: Test resolution"
		err := repo.ForceMarkFailed(ctx, entryID, errorMsg)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		// Verify entry was marked as failed
		entry := getOutboxEntry(t, db, entryID)
		if entry == nil {
			t.Fatal("Entry not found after ForceMarkFailed")
		}

		if entry.Status != model.OutboxStatusFailed {
			t.Errorf("Expected status 'failed', got: %s", entry.Status)
		}

		if entry.OutboxErrorMessage != errorMsg {
			t.Errorf("Expected error message '%s', got: '%s'", errorMsg, entry.OutboxErrorMessage)
		}

		if entry.ProcessedAt == nil {
			t.Error("Expected processed_at to be set")
		}
	})

	t.Run("EntryNotFound", func(t *testing.T) {
		nonExistentID := uuid.New()
		err := repo.ForceMarkFailed(ctx, nonExistentID, "test")
		if err == nil {
			t.Error("Expected error for non-existent entry")
		}
	})

	t.Run("TimeoutBehavior", func(t *testing.T) {
		entryID := createStuckEntry(t, db, 5, 3, time.Now().Add(-2*time.Minute))

		// ForceMarkFailed has a 5-second timeout built-in
		// This test verifies it completes well within that time
		start := time.Now()
		err := repo.ForceMarkFailed(ctx, entryID, "timeout test")
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if duration > 1*time.Second {
			t.Errorf("ForceMarkFailed took too long: %v (expected < 1s)", duration)
		}
	})
}

// TestStuckEntryResolver_ResolverLogic tests the resolver component
func TestStuckEntryResolver_ResolverLogic(t *testing.T) {
	db, logger, cleanup := setupTestDB(t)
	defer cleanup()
	defer cleanupOutboxTable(t, db)

	ctx := context.Background()
	repo := postgres.NewOutboxRepository(db, logger)

	t.Run("DisabledResolver", func(t *testing.T) {
		config := &handlers.ResolverConfig{
			CheckIntervalSeconds:  0, // Disabled
			StuckThresholdSeconds: 60,
			BatchSize:             50,
			MaxResolveAttempts:    5,
		}

		resolver := handlers.NewStuckEntryResolver(repo, config, logger)

		// Run should return immediately when disabled
		errChan := make(chan error, 1)
		go func() {
			errChan <- resolver.Run(ctx)
		}()

		select {
		case err := <-errChan:
			if err != nil {
				t.Errorf("Expected nil error from disabled resolver, got: %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("Disabled resolver did not return immediately")
		}
	})

	t.Run("NoStuckEntries", func(t *testing.T) {
		cleanupOutboxTable(t, db)

		config := &handlers.ResolverConfig{
			CheckIntervalSeconds:  1,
			StuckThresholdSeconds: 60,
			BatchSize:             50,
			MaxResolveAttempts:    5,
		}

		resolver := handlers.NewStuckEntryResolver(repo, config, logger)

		// ResolveStuckEntries should handle empty result gracefully
		err := resolver.ResolveStuckEntries(ctx)
		if err != nil {
			t.Errorf("Expected no error with no stuck entries, got: %v", err)
		}
	})

	t.Run("SuccessfulResolution", func(t *testing.T) {
		cleanupOutboxTable(t, db)

		// Create stuck entries
		stuckTime := time.Now().Add(-2 * time.Minute)
		entryID1 := createStuckEntry(t, db, 5, 3, stuckTime)
		entryID2 := createStuckEntry(t, db, 4, 3, stuckTime.Add(-30*time.Second))

		config := &handlers.ResolverConfig{
			CheckIntervalSeconds:  1,
			StuckThresholdSeconds: 60,
			BatchSize:             50,
			MaxResolveAttempts:    5,
		}

		resolver := handlers.NewStuckEntryResolver(repo, config, logger)

		// Resolve stuck entries
		err := resolver.ResolveStuckEntries(ctx)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		// Verify both entries were marked as failed
		entry1 := getOutboxEntry(t, db, entryID1)
		entry2 := getOutboxEntry(t, db, entryID2)

		if entry1.Status != model.OutboxStatusFailed {
			t.Errorf("Entry1: Expected status 'failed', got: %s", entry1.Status)
		}

		if entry2.Status != model.OutboxStatusFailed {
			t.Errorf("Entry2: Expected status 'failed', got: %s", entry2.Status)
		}

		// Verify error messages contain resolution info
		if entry1.OutboxErrorMessage == "" {
			t.Error("Entry1: Expected error message to be set")
		}
		if entry2.OutboxErrorMessage == "" {
			t.Error("Entry2: Expected error message to be set")
		}

		t.Logf("Entry1 error message: %s", entry1.OutboxErrorMessage)
		t.Logf("Entry2 error message: %s", entry2.OutboxErrorMessage)
	})

	t.Run("ResolverRun", func(t *testing.T) {
		cleanupOutboxTable(t, db)

		// Create a stuck entry
		stuckTime := time.Now().Add(-2 * time.Minute)
		entryID := createStuckEntry(t, db, 5, 3, stuckTime)

		config := &handlers.ResolverConfig{
			CheckIntervalSeconds:  1, // Run every second for fast test
			StuckThresholdSeconds: 60,
			BatchSize:             50,
			MaxResolveAttempts:    5,
		}

		resolver := handlers.NewStuckEntryResolver(repo, config, logger)

		// Start resolver in background
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			resolver.Run(ctx)
		}()

		// Wait for resolver to process (2 seconds should be enough)
		time.Sleep(2 * time.Second)

		// Cancel context to stop resolver
		cancel()

		// Verify entry was resolved
		entry := getOutboxEntry(t, db, entryID)
		if entry.Status != model.OutboxStatusFailed {
			t.Errorf("Expected entry to be resolved, got status: %s", entry.Status)
		}
	})
}

// TestStuckEntryResolver_ConcurrentAccess tests SKIP LOCKED behavior
func TestStuckEntryResolver_ConcurrentAccess(t *testing.T) {
	db, logger, cleanup := setupTestDB(t)
	defer cleanup()
	defer cleanupOutboxTable(t, db)

	ctx := context.Background()

	t.Run("SkipLockedPreventsConflicts", func(t *testing.T) {
		cleanupOutboxTable(t, db)

		// Create 10 stuck entries with sufficient time gap
		stuckTime := time.Now().Add(-2 * time.Minute)
		for i := 0; i < 10; i++ {
			createStuckEntry(t, db, 5, 3, stuckTime.Add(time.Duration(-i)*10*time.Second))
		}

		repo := postgres.NewOutboxRepository(db, logger)
		config := &handlers.ResolverConfig{
			CheckIntervalSeconds:  1,
			StuckThresholdSeconds: 60,
			BatchSize:             50, // Large enough to get all entries
			MaxResolveAttempts:    5,
		}

		// Create two resolvers simulating multiple k8s-controller pods
		resolver1 := handlers.NewStuckEntryResolver(repo, config, logger)
		resolver2 := handlers.NewStuckEntryResolver(repo, config, logger)

		// Run both resolvers concurrently
		done1 := make(chan error, 1)
		done2 := make(chan error, 1)

		go func() {
			done1 <- resolver1.ResolveStuckEntries(ctx)
		}()

		// Small delay to ensure resolver1 starts first and acquires some locks
		time.Sleep(10 * time.Millisecond)

		go func() {
			done2 <- resolver2.ResolveStuckEntries(ctx)
		}()

		// Wait for both to complete
		err1 := <-done1
		err2 := <-done2

		if err1 != nil {
			t.Errorf("Resolver1 error: %v", err1)
		}
		if err2 != nil {
			t.Errorf("Resolver2 error: %v", err2)
		}

		// Verify all 10 entries were processed without conflicts
		var failedCount int
		err := db.Get(&failedCount, `
			SELECT COUNT(*)
			FROM k8s_status_outbox
			WHERE service_name = 'test-stuck-resolver'
			  AND status = 'failed'
		`)
		if err != nil {
			t.Fatalf("Failed to count failed entries: %v", err)
		}

		// Should be 10 - SKIP LOCKED ensures no duplicate processing
		if failedCount != 10 {
			t.Errorf("Expected all 10 entries to be failed, got: %d", failedCount)
		}
	})
}

// TestStuckEntryResolver_EdgeCases tests edge cases and error scenarios
func TestStuckEntryResolver_EdgeCases(t *testing.T) {
	db, logger, cleanup := setupTestDB(t)
	defer cleanup()
	defer cleanupOutboxTable(t, db)

	ctx := context.Background()
	repo := postgres.NewOutboxRepository(db, logger)

	t.Run("StuckThresholdBoundary", func(t *testing.T) {
		// Use unique service name for this test
		serviceName := "test-boundary-" + uuid.New().String()[:8]

		// Entry below threshold (should not be returned)
		createStuckEntryWithService(t, db, serviceName, 5, 3, time.Now().Add(-59*time.Second))

		// Entry past threshold (should be returned)
		entryID := createStuckEntryWithService(t, db, serviceName, 5, 3, time.Now().Add(-65*time.Second))

		entries, err := repo.GetStuckEntries(ctx, 50, 60)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		// Filter for our test entries only
		var testEntries []*model.K8sStatusOutboxEntry
		for _, e := range entries {
			if e.ServiceName == serviceName {
				testEntries = append(testEntries, e)
			}
		}

		if len(testEntries) != 1 {
			t.Errorf("Expected 1 entry with service %s, got: %d", serviceName, len(testEntries))
			for i, e := range testEntries {
				t.Logf("Entry %d: ID=%s, age=%v", i, e.ID, time.Since(e.CreatedAt))
			}
		}

		if len(testEntries) == 1 && testEntries[0].ID != entryID {
			t.Errorf("Got wrong entry: %s (expected %s)", testEntries[0].ID, entryID)
		}
	})

	t.Run("AlreadyProcessedEntry", func(t *testing.T) {
		cleanupOutboxTable(t, db)

		// Create entry that's already been processed
		entryID := uuid.New()
		taskID := uuid.New()
		scheduleID := uuid.New()

		query := `
			INSERT INTO k8s_status_outbox (
				id, event_type, stream_name,
				task_id, schedule_id, schedule_name, service_name, schema_name,
				table_name, job_name,
				status, created_at, processed_at, outbox_retry_count, max_retries
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
			)
		`

		processedTime := time.Now()
		_, err := db.Exec(query,
			entryID, "task_retry", "task.retry:v1",
			taskID, scheduleID, "test-schedule", "test-stuck-resolver", "public",
			"test-table", "test-job",
			string(model.OutboxStatusFailed), time.Now().Add(-2*time.Minute), processedTime, 5, 3,
		)
		if err != nil {
			t.Fatalf("Failed to create processed entry: %v", err)
		}

		// GetStuckEntries should not return processed entries
		entries, err := repo.GetStuckEntries(ctx, 50, 60)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if len(entries) != 0 {
			t.Errorf("Expected 0 entries (already processed), got: %d", len(entries))
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		config := &handlers.ResolverConfig{
			CheckIntervalSeconds:  1,
			StuckThresholdSeconds: 60,
			BatchSize:             50,
			MaxResolveAttempts:    5,
		}

		resolver := handlers.NewStuckEntryResolver(repo, config, logger)

		// Create a context that's already cancelled
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		// Run should return context.Canceled error
		err := resolver.Run(cancelledCtx)
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled error, got: %v", err)
		}
	})
}
