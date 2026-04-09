package test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/test/fakes"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestOutboxProcessor_CheckDelayed_PublishesOutboxEntryID(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	db, cleanup := setupPostgres(t)
	defer cleanup()

	entryID := uuid.New()
	taskID := uuid.New()
	schedID := uuid.New()

	// Seed a check_delayed outbox entry
	_, err := db.ExecContext(ctx, `
		INSERT INTO k8s_status_outbox
			(id, event_type, stream_name,
			 task_id, schedule_id, schedule_name, service_name,
			 schema_name, table_name, job_name,
			 check_after, status)
		VALUES ($1, 'check_delayed', 'k8s.check:v1',
			$2, $3, 'sched', 'svc',
			'pub', 'tbl', 'job',
			0, 'pending')
	`, entryID, taskID, schedID)
	require.NoError(t, err)

	publisher := &fakes.FakeEventPublisher{}
	stateClient := &fakes.FakeStateClient{}
	outboxRepo := postgres.NewOutboxRepository(db, logger)
	processor := handlers.NewOutboxProcessor(outboxRepo, stateClient, publisher, logger)

	err = processor.ProcessOnce(ctx, 100)
	require.NoError(t, err)

	require.Equal(t, 1, publisher.PublishCallCount)
	vals := publisher.LastValues
	require.Equal(t, entryID.String(), vals["outbox_entry_id"],
		"check_delayed publish must include the outbox entry ID")
}
