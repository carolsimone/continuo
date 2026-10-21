package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// loadAttempt reads one task_attempts row with FOR UPDATE. A missing row yields
// found == false and no error.
func loadAttempt(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID, attempt int32) (run.Attempt, bool, error) {
	var row taskAttemptRow
	err := tx.GetContext(ctx, &row, `
		SELECT task_id, attempt, job_name, status, failure_class, failure_reason,
		       dispatched_at, queued_at, started_at, finished_at,
		       execution_seconds, exit_code, error_message, log_s3_key,
		       run_results_uri, parse_cache, parse_cache_reason
		FROM task_attempts
		WHERE task_id = $1 AND attempt = $2
		FOR UPDATE`, taskID, attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return run.Attempt{}, false, nil
	}
	if err != nil {
		return run.Attempt{}, false, fmt.Errorf("load attempt %d of task_id %s: %w", attempt, taskID, err)
	}
	return attemptFromRow(row), true, nil
}

// attemptFromRow maps a task_attempts row onto the domain attempt.
func attemptFromRow(r taskAttemptRow) run.Attempt {
	dispatched := r.DispatchedAt
	return run.Attempt{
		TaskID:           r.TaskID,
		Number:           r.Attempt,
		JobName:          r.JobName,
		Status:           run.AttemptStatus(r.Status),
		FailureClass:     r.FailureClass,
		FailureReason:    r.FailureReason,
		DispatchedAt:     &dispatched,
		QueuedAt:         r.QueuedAt,
		StartedAt:        r.StartedAt,
		FinishedAt:       r.FinishedAt,
		ExecutionSeconds: r.ExecutionSeconds,
		ExitCode:         r.ExitCode,
		ErrorMessage:     r.ErrorMessage,
		LogS3Key:         r.LogS3Key,
		RunResultsURI:    r.RunResultsURI,
		ParseCache:       r.ParseCache,
		ParseCacheReason: r.ParseCacheReason,
	}
}

// saveAttempt inserts the attempt, or replaces every fact of the stored one with
// the same (task_id, attempt). dispatched_at is the supplied value or the
// database clock on insert and is left alone on replace. last_report_at is the
// database clock and never moves backwards; an attempt that ends has no
// next_check_at, and a live one keeps the one it has.
func saveAttempt(ctx context.Context, tx *sqlx.Tx, a run.Attempt) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO task_attempts (
			task_id, attempt, job_name, status, failure_class, failure_reason,
			dispatched_at, queued_at, started_at, finished_at,
			execution_seconds, exit_code, error_message, log_s3_key,
			run_results_uri, parse_cache, parse_cache_reason, last_report_at)
		VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()), $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now())
		ON CONFLICT (task_id, attempt) DO UPDATE SET
			job_name           = EXCLUDED.job_name,
			status             = EXCLUDED.status,
			failure_class      = EXCLUDED.failure_class,
			failure_reason     = EXCLUDED.failure_reason,
			queued_at          = EXCLUDED.queued_at,
			started_at         = EXCLUDED.started_at,
			finished_at        = EXCLUDED.finished_at,
			execution_seconds  = EXCLUDED.execution_seconds,
			exit_code          = EXCLUDED.exit_code,
			error_message      = EXCLUDED.error_message,
			log_s3_key         = EXCLUDED.log_s3_key,
			run_results_uri    = EXCLUDED.run_results_uri,
			parse_cache        = EXCLUDED.parse_cache,
			parse_cache_reason = EXCLUDED.parse_cache_reason,
			last_report_at     = GREATEST(task_attempts.last_report_at, EXCLUDED.last_report_at),
			next_check_at      = CASE WHEN EXCLUDED.status IN ('succeeded', 'failed', 'cancelled')
			                          THEN NULL ELSE task_attempts.next_check_at END`,
		a.TaskID, a.Number, a.JobName, string(a.Status), a.FailureClass, a.FailureReason,
		a.DispatchedAt, a.QueuedAt, a.StartedAt, a.FinishedAt,
		a.ExecutionSeconds, a.ExitCode, a.ErrorMessage, a.LogS3Key,
		a.RunResultsURI, a.ParseCache, a.ParseCacheReason)
	if err != nil {
		return fmt.Errorf("save attempt %d of task_id %s: %w", a.Number, a.TaskID, err)
	}
	return nil
}

// cancelOpenAttempts marks the dispatched, queued and running attempts of the
// run's tasks cancelled, clears their next check, and returns how many it
// changed.
func cancelOpenAttempts(ctx context.Context, tx *sqlx.Tx, runID uuid.UUID) (int, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE task_attempts ta
		SET status = 'cancelled', next_check_at = NULL
		FROM task_tracker tt
		WHERE ta.task_id = tt.task_id
		  AND tt.schedule_id = $1
		  AND ta.status IN ('dispatched', 'queued', 'running')`, runID)
	if err != nil {
		return 0, fmt.Errorf("cancel the open attempts of run %s: %w", runID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("cancel the open attempts of run %s: %w", runID, err)
	}
	return int(n), nil
}
