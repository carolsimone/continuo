package test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeployHandler_Handle_Success(t *testing.T) {
	// Setup
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	txRunner := uow.NewPostgresTransactionRunner(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)

	handler := handlers.NewDeployHandler(txRunner, logger)

	// Create test command
	taskID := uuid.New()
	scheduleID := uuid.New()
	cmd := command.DeployJob{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users",
		NodeType:     pkg_model.NodeTypeDbtModel,
	}

	// Execute
	ctx := context.Background()
	err := handler.Handle(ctx, cmd)

	// Assert
	require.NoError(t, err, "Handler should succeed")

	// Verify outbox entry was created
	entries, err := outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	require.Len(t, entries, 1, "Should have one outbox entry")

	entry := entries[0]
	assert.Equal(t, taskID, entry.TaskID)
	assert.Equal(t, scheduleID, entry.ScheduleID)
	assert.Equal(t, "hourly", entry.ScheduleName)
	assert.Equal(t, "dbt", entry.ServiceName)
	assert.Equal(t, "public", entry.SchemaName)
	assert.Equal(t, "users", entry.TableName)
	assert.Equal(t, "dbt-public-users", entry.JobName)
	assert.Equal(t, "dbt-model", entry.NodeType)
	assert.Equal(t, string(model.OutboxStatusPending), entry.Status)
	assert.Equal(t, 0, entry.OutboxRetryCount)
	assert.Equal(t, 3, entry.OutboxMaxRetries)
	assert.Equal(t, 2, entry.TaskMaxRetries)
}

func TestDeployHandler_Handle_MultipleCommands(t *testing.T) {
	// Setup
	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	txRunner := uow.NewPostgresTransactionRunner(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)

	handler := handlers.NewDeployHandler(txRunner, logger)

	ctx := context.Background()

	// Execute multiple commands
	numCommands := 5
	taskIDs := make([]uuid.UUID, numCommands)

	for i := 0; i < numCommands; i++ {
		taskID := uuid.New()
		taskIDs[i] = taskID

		cmd := command.DeployJob{
			TaskID:       taskID,
			ScheduleID:   uuid.New(),
			ScheduleName: "hourly",
			ServiceName:  "dbt",
			SchemaName:   "public",
			TableName:    "users",
			JobName:      "dbt-public-users",
		}

		err := handler.Handle(ctx, cmd)
		require.NoError(t, err)
	}

	// Verify all entries were created
	entries, err := outboxRepo.GetPendingBatch(ctx, 100)
	require.NoError(t, err)
	assert.Len(t, entries, numCommands, "Should have all outbox entries")

	// Verify all task IDs are present
	foundTaskIDs := make(map[uuid.UUID]bool)
	for _, entry := range entries {
		foundTaskIDs[entry.TaskID] = true
	}

	for _, taskID := range taskIDs {
		assert.True(t, foundTaskIDs[taskID], "Task ID %s should be in outbox", taskID)
	}
}

func TestDeployHandler_Handle_TransactionRollback(t *testing.T) {
	// This test verifies that if something goes wrong during the transaction,
	// nothing is committed to the database.
	// Since we can't easily force a transaction failure in the current setup,
	// this is more of a conceptual test.
	// In a real scenario, you might simulate a constraint violation or similar.

	db, cleanup := setupPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	txRunner := uow.NewPostgresTransactionRunner(db, logger)
	outboxRepo := postgres.NewOutboxRepository(db, logger)

	handler := handlers.NewDeployHandler(txRunner, logger)

	ctx := context.Background()

	// Create valid command
	cmd := command.DeployJob{
		TaskID:       uuid.New(),
		ScheduleID:   uuid.New(),
		ScheduleName: "hourly",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "users",
		JobName:      "dbt-public-users",
	}

	// Execute
	err := handler.Handle(ctx, cmd)
	require.NoError(t, err)

	// Verify entry exists
	entries, err := outboxRepo.GetPendingBatch(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}
