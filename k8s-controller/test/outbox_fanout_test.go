package test

// D1 invariant tests: a k8s handler that emits multi-effect business decisions
// must produce exactly N canonical outbox rows in a single atomic transaction.
//
// These tests use a real PostgreSQL instance (via testcontainers) to verify
// that the D1 design guarantee holds at the storage layer — not just in memory:
//
//   1. Happy path: handleSucceeded commits exactly 3 rows.
//   2. Rollback path: a Create error on the 3rd write rolls back all 3, leaving 0 rows.
//
// A future refactor that moves the 3 Creates outside the transaction, or splits
// them across two transactions, would fail test 2 — which is the point.

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	postgresadapter "github.com/carolsimone/continuo/k8s-controller/adapters/postgres"
	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	"github.com/carolsimone/continuo/k8s-controller/domain/model"
	"github.com/carolsimone/continuo/k8s-controller/service/handlers"
	"github.com/carolsimone/continuo/k8s-controller/service/uow"
	"github.com/carolsimone/continuo/k8s-controller/test/fakes"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// errInjected is the sentinel error returned by the injected failing outbox repo.
var errInjected = errors.New("injected Create failure")

// countingOutboxRepo wraps a real outbox repo and returns errInjected on the Nth Create call.
type countingOutboxRepo struct {
	real      pkgoutbox.Repository
	callCount int
	failOnN   int // 1-based: fail when callCount reaches this value
}

func (r *countingOutboxRepo) Create(ctx context.Context, e *pkgoutbox.Entry) error {
	r.callCount++
	if r.callCount == r.failOnN {
		return errInjected
	}
	return r.real.Create(ctx, e)
}

func (r *countingOutboxRepo) GetPendingBatch(ctx context.Context, limit int) ([]*pkgoutbox.Entry, error) {
	return r.real.GetPendingBatch(ctx, limit)
}

func (r *countingOutboxRepo) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	return r.real.MarkProcessed(ctx, id)
}

func (r *countingOutboxRepo) MarkFailed(ctx context.Context, id uuid.UUID, msg string) error {
	return r.real.MarkFailed(ctx, id, msg)
}

func (r *countingOutboxRepo) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	return r.real.IncrementRetry(ctx, id)
}

var _ pkgoutbox.Repository = (*countingOutboxRepo)(nil)

// injectingTransactionRunner wraps the real runner but replaces the OutboxRepo
// inside each transaction with the result of wrapOutbox.
type injectingTransactionRunner struct {
	real       uow.TransactionRunner
	wrapOutbox func(pkgoutbox.Repository) pkgoutbox.Repository
}

func (r *injectingTransactionRunner) WithinTransaction(ctx context.Context, fn func(uow.Transaction) error) error {
	return r.real.WithinTransaction(ctx, func(tx uow.Transaction) error {
		wrapped := &wrappedTransaction{
			tx:         tx,
			outboxRepo: r.wrapOutbox(tx.OutboxRepo()),
		}
		return fn(wrapped)
	})
}

type wrappedTransaction struct {
	tx         uow.Transaction
	outboxRepo pkgoutbox.Repository
}

func (w *wrappedTransaction) OutboxRepo() pkgoutbox.Repository {
	return w.outboxRepo
}

func (w *wrappedTransaction) ProcessedEventsRepo() postgresadapter.ProcessedEventsRepository {
	return w.tx.ProcessedEventsRepo()
}

var _ uow.Transaction = (*wrappedTransaction)(nil)
var _ uow.TransactionRunner = (*injectingTransactionRunner)(nil)

// fakeCancelledSchedulesRepoFanout always reports no cancellation.
type fakeCancelledSchedulesRepoFanout struct{}

func (f *fakeCancelledSchedulesRepoFanout) Insert(_ context.Context, _ uuid.UUID) error {
	return nil
}
func (f *fakeCancelledSchedulesRepoFanout) Exists(_ context.Context, _ uuid.UUID) (bool, error) {
	return false, nil
}
func (f *fakeCancelledSchedulesRepoFanout) DeleteExpired(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}

var _ postgresadapter.CancelledSchedulesRepository = (*fakeCancelledSchedulesRepoFanout)(nil)

// newSucceededHandler builds a CheckStatusHandler wired to the given transaction runner
// and a K8s stub that always returns JobStatusSucceeded.
func newSucceededHandler(txRunner uow.TransactionRunner, logger *slog.Logger) *handlers.CheckStatusHandler {
	now := time.Now()
	k8sClient := &fakes.FakeK8sClient{
		GetJobStatusFunc: func(_ context.Context, _, _ string) (*model.K8sPodResult, error) {
			return &model.K8sPodResult{
				Status:           model.JobStatusSucceeded,
				StartedAt:        &now,
				CompletedAt:      &now,
				ExecutionSeconds: 2.5,
			}, nil
		},
	}

	cfg := &handlers.HandlerConfig{
		K8sNamespace:          "default",
		CheckDelaySeconds:     30,
		ErrorMessageMaxLen:    4096,
		LogTailLines:          50,
		DefaultTaskMaxRetries: 3,
	}

	return handlers.NewCheckStatusHandler(
		k8sClient,
		txRunner,
		&fakes.FakeLogUploader{},
		cfg,
		&fakeCancelledSchedulesRepoFanout{},
		logger,
	)
}

// newSucceededCmd returns a minimal CheckJobStatus command for the given task ID.
func newSucceededCmd(taskID uuid.UUID) command.CheckJobStatus {
	return command.CheckJobStatus{
		TaskID:     taskID,
		ScheduleID: uuid.New(),
		JobName:    "job-fanout-test",
		RetryCount: 0,
		MaxRetries: 3,
	}
}

// countOutboxRows returns the number of rows in k8s_outbox for the given aggregate_id.
func countOutboxRows(t testing.TB, db *sqlx.DB, aggregateID uuid.UUID) int {
	t.Helper()
	var count int
	err := db.QueryRow(
		"SELECT COUNT(*) FROM k8s_outbox WHERE aggregate_id = $1",
		aggregateID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("countOutboxRows: %v", err)
	}
	return count
}

// eventTypesForAggregate returns the event_type values committed to k8s_outbox
// for the given aggregate_id, ordered by created_at.
func eventTypesForAggregate(t testing.TB, db *sqlx.DB, aggregateID uuid.UUID) []string {
	t.Helper()
	rows, err := db.Query(
		"SELECT event_type FROM k8s_outbox WHERE aggregate_id = $1 ORDER BY created_at",
		aggregateID,
	)
	if err != nil {
		t.Fatalf("eventTypesForAggregate query: %v", err)
	}
	defer rows.Close()

	var types []string
	for rows.Next() {
		var et string
		if err := rows.Scan(&et); err != nil {
			t.Fatalf("eventTypesForAggregate scan: %v", err)
		}
		types = append(types, et)
	}
	return types
}

// TestK8sFanout_HandleSucceeded_Commits3Rows is the D1 happy-path invariant:
// driving handleSucceeded through a real Postgres transaction must commit exactly
// 3 rows with the correct event_types in k8s_outbox.
func TestK8sFanout_HandleSucceeded_Commits3Rows(t *testing.T) {
	db, logger, cleanup := setupTestDB(t)
	defer cleanup()

	txRunner := uow.NewPostgresTransactionRunner(db, logger)
	handler := newSucceededHandler(txRunner, logger)

	taskID := uuid.New()
	if err := handler.Handle(context.Background(), newSucceededCmd(taskID)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	got := countOutboxRows(t, db, taskID)
	if got != 3 {
		t.Fatalf("D1 invariant violated: expected 3 rows in k8s_outbox, got %d", got)
	}

	types := eventTypesForAggregate(t, db, taskID)
	want := []string{"task_status_updated", "task_execution_recorded", "node_status_updated"}
	if len(types) != len(want) {
		t.Fatalf("event_types mismatch: want %v got %v", want, types)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Errorf("event_types[%d]: want %q got %q", i, want[i], types[i])
		}
	}
}

// TestK8sFanout_HandleSucceeded_AtomicRollback is the D1 rollback invariant:
// when the 3rd outbox Create fails, the entire transaction must roll back and
// leave 0 rows in k8s_outbox.
//
// This guards against accidentally splitting the 3 writes across separate
// transactions: if they were split, the first two rows would survive the
// failure and this test would detect it.
func TestK8sFanout_HandleSucceeded_AtomicRollback(t *testing.T) {
	db, logger, cleanup := setupTestDB(t)
	defer cleanup()

	realRunner := uow.NewPostgresTransactionRunner(db, logger)

	// Inject a failure on the 3rd Create call (node_status_updated).
	failOn3rd := &injectingTransactionRunner{
		real: realRunner,
		wrapOutbox: func(repo pkgoutbox.Repository) pkgoutbox.Repository {
			return &countingOutboxRepo{real: repo, failOnN: 3}
		},
	}

	handler := newSucceededHandler(failOn3rd, logger)

	taskID := uuid.New()
	err := handler.Handle(context.Background(), newSucceededCmd(taskID))
	if err == nil {
		t.Fatal("expected Handle to return an error when the 3rd Create is injected, got nil")
	}
	if !errors.Is(err, errInjected) {
		t.Fatalf("expected errInjected, got: %v", err)
	}

	// The transaction must have rolled back: 0 rows committed.
	got := countOutboxRows(t, db, taskID)
	if got != 0 {
		t.Fatalf("D1 atomicity violated: expected 0 rows after rollback, got %d (partial writes escaped the tx)", got)
	}
}
