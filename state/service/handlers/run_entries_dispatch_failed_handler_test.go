package handlers_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/database"
	"github.com/carolsimone/continuo/state/domain/model"
	statehandlers "github.com/carolsimone/continuo/state/service/handlers"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupDispatchFailedTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	db, err := database.GetPostgresConnection()
	if err != nil {
		t.Skip("no test DB available:", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seedDispatchFailedScheduler(t *testing.T, db *sqlx.DB, scheduleID uuid.UUID, status model.SchedulerStatus) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			initialization_status, service_metadata, total_task_count, terminal_task_count
		) VALUES ($1, $2, $3, $4, $5, $6, NULL, 0)
	`,
		scheduleID,
		"test-schedule-"+scheduleID.String(),
		string(status),
		time.Now(),
		"in_progress",
		json.RawMessage(`{}`),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Exec(`DELETE FROM state_outbox WHERE aggregate_id = $1`, scheduleID)
		db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, scheduleID)
	})
}

func buildDispatchFailedPayload(t *testing.T, scheduleID uuid.UUID, scheduleName, reason string) string {
	t.Helper()
	evt := events.RunEntriesDispatchFailed{
		ScheduleID:   scheduleID.String(),
		ScheduleName: scheduleName,
		Reason:       events.DispatchFailedReason(reason),
	}
	b, err := json.Marshal(evt)
	require.NoError(t, err)
	return string(b)
}

// TestRunEntriesDispatchFailedHandler_FinalizesRunningSchedulerAsFailed verifies
// the happy path. The gRPC TriggerSingleNodeRun handler creates the
// scheduler_tracker row with status='running' before orchestrator emits the
// dispatch_failed event, so this seeds 'running' to match production.
func TestRunEntriesDispatchFailedHandler_FinalizesRunningSchedulerAsFailed(t *testing.T) {
	db := setupDispatchFailedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	seedDispatchFailedScheduler(t, db, scheduleID, model.SchedulerStatusRunning)
	t.Cleanup(func() { db.Exec(`DELETE FROM processed_events`) })

	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	handler := statehandlers.NewRunEntriesDispatchFailedHandler(db, schedulerRepo, outboxRepo, logger)

	payload := buildDispatchFailedPayload(t, scheduleID, "test-schedule", "target_not_found")
	messageID := scheduleID.String() + "-fail-1"

	shouldACK, err := handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	tracker, err := schedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, model.SchedulerStatusFailed, tracker.Status)

	pending, err := outboxRepo.ListPending(context.Background(), 50)
	require.NoError(t, err)
	var foundFinalized bool
	for _, e := range pending {
		if e.AggregateID == scheduleID && e.StreamName == "run.finalized:v1" {
			foundFinalized = true
			var fin events.RunFinalized
			require.NoError(t, json.Unmarshal(e.Payload, &fin))
			assert.Equal(t, "failed", fin.Status)
			assert.Equal(t, scheduleID.String(), fin.ScheduleID)
		}
	}
	assert.True(t, foundFinalized, "run.finalized:v1 outbox entry must be written")
}

// TestRunEntriesDispatchFailedHandler_IdempotentOnAlreadyTerminal verifies that
// re-delivery against an already-terminal scheduler is a no-op (no extra
// run.finalized:v1, no status change).
func TestRunEntriesDispatchFailedHandler_IdempotentOnAlreadyTerminal(t *testing.T) {
	db := setupDispatchFailedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	seedDispatchFailedScheduler(t, db, scheduleID, model.SchedulerStatusSucceeded)
	t.Cleanup(func() { db.Exec(`DELETE FROM processed_events`) })

	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	handler := statehandlers.NewRunEntriesDispatchFailedHandler(db, schedulerRepo, outboxRepo, logger)

	payload := buildDispatchFailedPayload(t, scheduleID, "test-schedule", "target_not_found")
	messageID := scheduleID.String() + "-fail-already-terminal"

	shouldACK, err := handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	tracker, err := schedulerRepo.GetByID(context.Background(), scheduleID)
	require.NoError(t, err)
	assert.Equal(t, model.SchedulerStatusSucceeded, tracker.Status,
		"already-terminal scheduler must not be overwritten by dispatch_failed")

	pending, err := outboxRepo.ListPending(context.Background(), 50)
	require.NoError(t, err)
	for _, e := range pending {
		if e.AggregateID == scheduleID && e.StreamName == "run.finalized:v1" {
			t.Fatalf("no run.finalized:v1 must be emitted for already-terminal scheduler, got entry %s", e.ID)
		}
	}
}

// TestRunEntriesDispatchFailedHandler_DedupOnRedelivery verifies that the
// processed_events guard prevents double-processing on Redis re-delivery.
func TestRunEntriesDispatchFailedHandler_DedupOnRedelivery(t *testing.T) {
	db := setupDispatchFailedTestDB(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	scheduleID := uuid.New()
	seedDispatchFailedScheduler(t, db, scheduleID, model.SchedulerStatusRunning)
	t.Cleanup(func() { db.Exec(`DELETE FROM processed_events`) })

	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	handler := statehandlers.NewRunEntriesDispatchFailedHandler(db, schedulerRepo, outboxRepo, logger)

	payload := buildDispatchFailedPayload(t, scheduleID, "test-schedule", "target_not_found")
	messageID := scheduleID.String() + "-fail-dedup"

	// First delivery
	shouldACK, err := handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Second delivery — same message ID, must be a no-op
	shouldACK, err = handler.Handle(context.Background(), messageID, payload)
	require.NoError(t, err)
	assert.True(t, shouldACK)

	// Exactly one run.finalized:v1 outbox entry must exist for this scheduler
	pending, err := outboxRepo.ListPending(context.Background(), 50)
	require.NoError(t, err)
	var finalizedCount int
	for _, e := range pending {
		if e.AggregateID == scheduleID && e.StreamName == "run.finalized:v1" {
			finalizedCount++
		}
	}
	assert.Equal(t, 1, finalizedCount, "duplicate message must not produce a second run.finalized:v1")

	dedupID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("run.entries.dispatch_failed:"+messageID))
	var count int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM processed_events WHERE event_id = $1`, dedupID,
	).Scan(&count))
	assert.Equal(t, 1, count, "processed_events must have exactly one entry for the message")
}
