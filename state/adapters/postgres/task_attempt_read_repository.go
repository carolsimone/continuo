package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"

	"github.com/carolsimone/continuo/state/domain/projection"
	"github.com/carolsimone/continuo/state/domain/repository"
)

// TaskAttemptReadRepository reads a tenant's run's task attempts from
// task_attempts, reaching the run through each attempt's task row.
type TaskAttemptReadRepository struct {
	db *sqlx.DB
}

var _ repository.TaskAttemptReader = (*TaskAttemptReadRepository)(nil)

// NewTaskAttemptReadRepository returns a TaskAttemptReadRepository over db.
func NewTaskAttemptReadRepository(db *sqlx.DB) *TaskAttemptReadRepository {
	return &TaskAttemptReadRepository{db: db}
}

type taskAttemptReadRow struct {
	TaskID           uuid.UUID  `db:"task_id"`
	Attempt          int        `db:"attempt"`
	JobName          string     `db:"job_name"`
	Status           string     `db:"status"`
	FailureClass     *string    `db:"failure_class"`
	FailureReason    *string    `db:"failure_reason"`
	ExitCode         *int       `db:"exit_code"`
	DispatchedAt     time.Time  `db:"dispatched_at"`
	QueuedAt         *time.Time `db:"queued_at"`
	StartedAt        *time.Time `db:"started_at"`
	FinishedAt       *time.Time `db:"finished_at"`
	ExecutionSeconds *float64   `db:"execution_seconds"`
	ErrorMessage     *string    `db:"error_message"`
	LogS3Key         *string    `db:"log_s3_key"`
	RunResultsURI    *string    `db:"run_results_uri"`
}

// ListRunAttempts implements repository.TaskAttemptReader.
func (r *TaskAttemptReadRepository) ListRunAttempts(
	ctx context.Context, tenantID string, runID uuid.UUID, taskID *uuid.UUID, pageSize, pageOffset int,
) ([]projection.TaskAttempt, int, error) {
	const filter = `
		FROM task_attempts a
		JOIN tasks t ON t.tenant_id = a.tenant_id AND t.task_id = a.task_id
		WHERE t.tenant_id = $1 AND t.run_id = $2 AND ($3::uuid IS NULL OR a.task_id = $3)`

	var total int
	if err := r.db.GetContext(ctx, &total, `SELECT count(*)`+filter, tenantID, runID, taskID); err != nil {
		return nil, 0, fmt.Errorf("count attempts of run %s: %w", runID, err)
	}

	var rows []taskAttemptReadRow
	if err := r.db.SelectContext(ctx, &rows, `
		SELECT a.task_id, a.attempt, a.job_name, a.status, a.failure_class, a.failure_reason, a.exit_code,
		       a.dispatched_at, a.queued_at, a.started_at, a.finished_at, a.execution_seconds,
		       a.error_message, a.log_s3_key, a.run_results_uri`+filter+`
		ORDER BY a.task_id, a.attempt
		LIMIT $4 OFFSET $5`, tenantID, runID, taskID, pageSize, pageOffset); err != nil {
		return nil, 0, fmt.Errorf("list attempts of run %s: %w", runID, err)
	}

	out := make([]projection.TaskAttempt, 0, len(rows))
	for _, row := range rows {
		out = append(out, projection.TaskAttempt{
			TaskID: row.TaskID, Attempt: row.Attempt, JobName: row.JobName, Status: row.Status,
			FailureClass: row.FailureClass, FailureReason: row.FailureReason, ExitCode: row.ExitCode,
			DispatchedAt: row.DispatchedAt, QueuedAt: row.QueuedAt, StartedAt: row.StartedAt, FinishedAt: row.FinishedAt,
			ExecutionSeconds: row.ExecutionSeconds, ErrorMessage: row.ErrorMessage,
			LogS3Key: row.LogS3Key, RunResultsURI: row.RunResultsURI,
		})
	}
	return out, total, nil
}
