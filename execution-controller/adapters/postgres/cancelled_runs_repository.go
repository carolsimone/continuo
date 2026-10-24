package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/google/uuid"
)

// cancelledExecutor is the read/write subset of sqlx.DB / sqlx.Tx that
// CancelledRunsRepository needs. Same shape as
// pkg/messageprocessing/postgres.go's executor.
type cancelledExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type cancelledRunsRepository struct {
	executor cancelledExecutor
}

// NewCancelledRunsRepository creates a repository.CancelledRunsRepository
// over cancelled_runs against any sqlx executor (*sqlx.DB for autocommit,
// *sqlx.Tx for transaction-scoped work).
func NewCancelledRunsRepository(executor cancelledExecutor) repository.CancelledRunsRepository {
	return &cancelledRunsRepository{executor: executor}
}

var _ repository.CancelledRunsRepository = (*cancelledRunsRepository)(nil)

// Insert fences runID, stamping cancelled_at with the database clock; a fence
// already set is stamped again, so its age counts from the latest cancel.
func (r *cancelledRunsRepository) Insert(ctx context.Context, runID uuid.UUID) error {
	_, err := r.executor.ExecContext(ctx, `
		INSERT INTO cancelled_runs (run_id) VALUES ($1)
		ON CONFLICT (run_id) DO UPDATE SET cancelled_at = now()`, runID)
	return err
}

// runLockSpace is the first key of a run's advisory lock (the two-key form,
// apart from every one-key advisory lock); the second is a hash of the run
// id. Two runs whose hashes collide share a lock, which only serialises them.
const runLockSpace = 0x52554e46

// HoldRunForDispatch takes runID's advisory lock shared for the rest of the
// transaction, and HoldRunForFence takes it exclusive: dispatches share it,
// and a fence excludes every dispatch. Each runs as its own statement before
// the statement it guards (the fence read, or the fence insert and the lock
// of the run's rows). Under READ COMMITTED every statement reads a snapshot
// taken when it starts, so a statement that follows a granted lock sees
// whatever the transaction that held the lock committed.
func (r *cancelledRunsRepository) HoldRunForDispatch(ctx context.Context, runID uuid.UUID) error {
	return r.lockRun(ctx, `SELECT pg_advisory_xact_lock_shared($1, hashtext($2))`, runID)
}

func (r *cancelledRunsRepository) HoldRunForFence(ctx context.Context, runID uuid.UUID) error {
	return r.lockRun(ctx, `SELECT pg_advisory_xact_lock($1, hashtext($2))`, runID)
}

// lockRun runs the advisory lock query q on runID's lock key.
func (r *cancelledRunsRepository) lockRun(ctx context.Context, q string, runID uuid.UUID) error {
	if _, err := r.executor.ExecContext(ctx, q, runLockSpace, runID.String()); err != nil {
		return fmt.Errorf("lock run %s: %w", runID, err)
	}
	return nil
}

func (r *cancelledRunsRepository) Exists(ctx context.Context, runID uuid.UUID) (bool, error) {
	var exists bool
	err := r.executor.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM cancelled_runs WHERE run_id = $1)`, runID).Scan(&exists)
	return exists, err
}

func (r *cancelledRunsRepository) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	res, err := r.executor.ExecContext(ctx,
		`DELETE FROM cancelled_runs WHERE cancelled_at < NOW() - make_interval(secs => $1)`, age.Seconds())
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
