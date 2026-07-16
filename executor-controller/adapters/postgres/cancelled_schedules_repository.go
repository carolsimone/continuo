package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/repository"
	"github.com/google/uuid"
)

// cancelledExecutor is the read/write subset of sqlx.DB / sqlx.Tx that
// CancelledSchedulesRepository needs. Same shape as
// pkg/messageprocessing/postgres.go's executor.
type cancelledExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type cancelledSchedulesRepository struct {
	executor cancelledExecutor
}

var _ repository.CancelledSchedulesRepository = (*cancelledSchedulesRepository)(nil)

// NewCancelledSchedulesRepository creates a
// repository.CancelledSchedulesRepository against any sqlx executor (*sqlx.DB
// for autocommit, *sqlx.Tx for transaction-scoped work).
func NewCancelledSchedulesRepository(executor cancelledExecutor) repository.CancelledSchedulesRepository {
	return &cancelledSchedulesRepository{executor: executor}
}

// LockSchedule takes the schedule-scoped advisory lock via pg_advisory_xact_lock.
// hashtext maps the schedule UUID to the integer key the lock takes; a collision
// only makes two unrelated schedules serialize, which costs latency and nothing
// else. The lock auto-releases at commit/rollback.
func (r *cancelledSchedulesRepository) LockSchedule(ctx context.Context, id uuid.UUID) error {
	if _, err := r.executor.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtext($1))`, id.String()); err != nil {
		return fmt.Errorf("lock schedule %s: %w", id, err)
	}
	return nil
}

func (r *cancelledSchedulesRepository) Insert(ctx context.Context, id uuid.UUID) error {
	_, err := r.executor.ExecContext(ctx,
		`INSERT INTO cancelled_schedules (schedule_id) VALUES ($1) ON CONFLICT DO NOTHING`, id)
	return err
}

func (r *cancelledSchedulesRepository) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	var exists bool
	err := r.executor.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM cancelled_schedules WHERE schedule_id = $1)`, id).Scan(&exists)
	return exists, err
}

func (r *cancelledSchedulesRepository) DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error) {
	res, err := r.executor.ExecContext(ctx,
		`DELETE FROM cancelled_schedules WHERE cancelled_at < $1`, time.Now().Add(-ttl))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
