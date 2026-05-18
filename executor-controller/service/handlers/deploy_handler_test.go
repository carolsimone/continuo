package handlers_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/carolsimone/continuo/executor-controller/adapters/postgres"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubOutboxRepo captures creates for assertion without a real DB.
type stubOutboxRepo struct {
	mu      sync.Mutex
	entries []*model.DeploymentOutboxEntry
}

func (r *stubOutboxRepo) Create(ctx context.Context, entry *model.DeploymentOutboxEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, entry)
	return nil
}

func (r *stubOutboxRepo) GetPendingBatch(_ context.Context, _ int) ([]*model.DeploymentOutboxEntry, error) {
	return nil, nil
}

func (r *stubOutboxRepo) MarkProcessed(_ context.Context, _ uuid.UUID) error { return nil }
func (r *stubOutboxRepo) MarkFailed(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
func (r *stubOutboxRepo) IncrementRetry(_ context.Context, _ uuid.UUID) error { return nil }

type stubTransaction struct {
	outboxRepo postgres.OutboxRepository
}

func (tx *stubTransaction) OutboxRepo() postgres.OutboxRepository { return tx.outboxRepo }

type stubTransactionRunner struct {
	tx uow.Transaction
}

func (r *stubTransactionRunner) WithinTransaction(_ context.Context, fn func(uow.Transaction) error) error {
	return fn(r.tx)
}

func TestDeployHandler_Handle_WritesOutboxWithPayloadTaskID(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	repo := &stubOutboxRepo{}
	runner := &stubTransactionRunner{tx: &stubTransaction{outboxRepo: repo}}
	h := handlers.NewDeployHandler(runner, logger)

	taskID := uuid.New()
	scheduleID := uuid.New()

	cmd := command.DeployJob{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: "daily",
		ServiceName:  "dbt",
		SchemaName:   "public",
		TableName:    "orders",
		JobName:      "dbt-public-orders",
		NodeType:     pkg_model.NodeTypeDbtModel,
	}

	err := h.Handle(context.Background(), cmd)
	require.NoError(t, err)

	// Verify the outbox entry was created with the payload task_id, not a generated one
	require.Len(t, repo.entries, 1)
	entry := repo.entries[0]
	assert.Equal(t, taskID, entry.TaskID, "outbox entry TaskID must equal payload TaskID (not auto-generated)")
	assert.Equal(t, scheduleID, entry.ScheduleID)
	assert.Equal(t, "dbt-public-orders", entry.JobName)
	assert.Equal(t, "dbt", entry.ServiceName)
	assert.Equal(t, "public", entry.SchemaName)
	assert.Equal(t, "orders", entry.TableName)
	assert.NotEqual(t, uuid.Nil, entry.ID, "outbox entry ID should be auto-generated")
}

func TestDeployHandler_Handle_DoesNotGenerateOwnTaskID(t *testing.T) {
	// Ensure the handler uses the TaskID from the incoming command, not a new uuid.
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	repo := &stubOutboxRepo{}
	runner := &stubTransactionRunner{tx: &stubTransaction{outboxRepo: repo}}
	h := handlers.NewDeployHandler(runner, logger)

	preAssignedTaskID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	cmd := command.DeployJob{
		TaskID:       preAssignedTaskID,
		ScheduleID:   uuid.New(),
		ScheduleName: "hourly",
		ServiceName:  "svc",
		SchemaName:   "schema",
		TableName:    "table",
		JobName:      "job-name",
		NodeType:     pkg_model.NodeTypeDbtModel,
	}

	err := h.Handle(context.Background(), cmd)
	require.NoError(t, err)

	require.Len(t, repo.entries, 1)
	assert.Equal(t, preAssignedTaskID, repo.entries[0].TaskID,
		"handler must propagate the pre-assigned task_id from the orchestrator payload")
}

func TestDeployHandler_Handle_AllowsConcurrentCalls(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	for range 2 {
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO deployment_outbox").WillReturnResult(sqlmock.NewResult(1, 1))
		mock.ExpectCommit()
	}

	runner := uow.NewPostgresTransactionRunner(sqlx.NewDb(db, "sqlmock"), slog.Default())
	h := handlers.NewDeployHandler(runner, slog.Default())

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, table := range []string{"orders", "customers"} {
		wg.Add(1)
		go func(table string) {
			defer wg.Done()
			errs <- h.Handle(context.Background(), command.DeployJob{
				TaskID:       uuid.New(),
				ScheduleID:   uuid.New(),
				ScheduleName: "daily",
				ServiceName:  "dbt",
				SchemaName:   "public",
				TableName:    table,
				JobName:      "job-" + table,
				NodeType:     pkg_model.NodeTypeDbtModel,
			})
		}(table)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}
