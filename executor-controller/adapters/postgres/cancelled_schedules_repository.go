package postgres

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
)

// CancelledSchedulesRepository tracks schedule IDs whose deploys should be
// dropped on receipt. Implementations operate against the cancelled_schedules
// table (id, schedule_id UNIQUE, cancelled_at).
type CancelledSchedulesRepository interface {
	Insert(ctx context.Context, scheduleID uuid.UUID) error
	Exists(ctx context.Context, scheduleID uuid.UUID) (bool, error)
	DeleteExpired(ctx context.Context, ttl time.Duration) (int64, error)
}

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

// NewCancelledSchedulesRepository creates a CancelledSchedulesRepository
// against any sqlx executor (*sqlx.DB for autocommit, *sqlx.Tx for
// transaction-scoped work).
func NewCancelledSchedulesRepository(executor cancelledExecutor) CancelledSchedulesRepository {
	return &cancelledSchedulesRepository{executor: executor}
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
