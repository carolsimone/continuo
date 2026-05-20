package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/executor-controller/service/deployer"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
)

// deploymentsRepository is the Postgres adapter implementing
// deployer.Repository over the executor_deployments table.
type deploymentsRepository struct {
	exec   outbox.Executor
	logger *slog.Logger
}

// NewDeploymentsRepository constructs a deployer.Repository over
// executor_deployments. Pass *sqlx.DB for autocommit or *sqlx.Tx for
// transactional use; outbox.Executor abstracts both (QueryRowContext /
// SelectContext / ExecContext).
func NewDeploymentsRepository(exec outbox.Executor, logger *slog.Logger) deployer.Repository {
	return &deploymentsRepository{exec: exec, logger: logger}
}

func (r *deploymentsRepository) Create(ctx context.Context, d *deployer.Deployment) error {
	if d.ID == uuid.Nil {
		d.ID = uuid.New()
	}
	if d.Status == "" {
		d.Status = "pending"
	}
	if d.MaxRetries == 0 {
		d.MaxRetries = 3
	}
	if d.CreatedAt.IsZero() {
		d.CreatedAt = time.Now()
	}
	if d.NextAttemptAt.IsZero() {
		d.NextAttemptAt = d.CreatedAt
	}

	const query = `
		INSERT INTO executor_deployments (
			id, message_processing_id, task_id, schedule_id, job_params,
			status, retry_count, max_retries, next_attempt_at, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`
	if _, err := r.exec.ExecContext(ctx, query,
		d.ID, d.MessageProcessingID, d.TaskID, d.ScheduleID, d.JobParams,
		d.Status, d.RetryCount, d.MaxRetries, d.NextAttemptAt, d.CreatedAt,
	); err != nil {
		return fmt.Errorf("create executor_deployments row: %w", err)
	}
	return nil
}

func (r *deploymentsRepository) GetDueBatch(ctx context.Context, limit int) ([]*deployer.Deployment, error) {
	const query = `
		SELECT id, message_processing_id, task_id, schedule_id, job_params,
		       status, retry_count, max_retries, next_attempt_at,
		       created_at, deployed_at, error_message
		FROM executor_deployments
		WHERE status = 'pending' AND next_attempt_at <= NOW()
		ORDER BY next_attempt_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`
	var rows []*deployer.Deployment
	if err := r.exec.SelectContext(ctx, &rows, query, limit); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("get due deployments batch: %w", err)
	}
	return rows, nil
}

func (r *deploymentsRepository) MarkDeployed(ctx context.Context, id uuid.UUID) error {
	const query = `UPDATE executor_deployments SET status = 'deployed', deployed_at = NOW() WHERE id = $1`
	return r.exec1(ctx, query, "mark deployed", id)
}

func (r *deploymentsRepository) Reschedule(ctx context.Context, id uuid.UUID, nextAttemptAt time.Time, errMsg string) error {
	const query = `
		UPDATE executor_deployments
		SET retry_count = retry_count + 1, next_attempt_at = $2, error_message = $3
		WHERE id = $1`
	res, err := r.exec.ExecContext(ctx, query, id, nextAttemptAt, errMsg)
	if err != nil {
		return fmt.Errorf("reschedule deployment %s: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("deployment %s not found for reschedule", id)
	}
	return nil
}

func (r *deploymentsRepository) MarkFailed(ctx context.Context, id uuid.UUID, errMsg string) error {
	const query = `UPDATE executor_deployments SET status = 'failed', error_message = $2 WHERE id = $1`
	res, err := r.exec.ExecContext(ctx, query, id, errMsg)
	if err != nil {
		return fmt.Errorf("mark failed deployment %s: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("deployment %s not found for mark-failed", id)
	}
	return nil
}

func (r *deploymentsRepository) exec1(ctx context.Context, query, op string, id uuid.UUID) error {
	res, err := r.exec.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("%s deployment %s: %w", op, id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("deployment %s not found for %s", id, op)
	}
	return nil
}
