package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// loadAttempt reads one of the tenant's task_attempts rows with FOR UPDATE. A
// missing row yields found == false and no error.
func loadAttempt(ctx context.Context, tx *sqlx.Tx, tenantID string, taskID uuid.UUID, attempt int32) (run.Attempt, bool, error) {
	var row taskAttemptRow
	err := tx.GetContext(ctx, &row, `
		SELECT task_id, attempt, job_name, status, failure_class, failure_reason,
		       dispatched_at, queued_at, started_at, finished_at,
		       execution_seconds, exit_code, error_message, log_s3_key,
		       run_results_uri, parse_cache, parse_cache_reason
		FROM task_attempts
		WHERE tenant_id = $3 AND task_id = $1 AND attempt = $2
		FOR UPDATE`, taskID, attempt, tenantID)
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

// saveAttempt inserts the tenant's attempt, or replaces every fact of the stored
// one with the same (tenant_id, task_id, attempt). dispatched_at is the supplied value or the
// database clock on insert and is left alone on replace. last_report_at is the
// database clock and never moves backwards. next_check_at is NextCheckIn after
// the database clock when that is set, NULL once the attempt has ended (a final
// attempt's NextCheckIn is ignored), and otherwise left as it is.
func saveAttempt(ctx context.Context, tx *sqlx.Tx, tenantID string, a run.Attempt) error {
	checkIn := a.NextCheckIn
	if a.Status.IsFinal() {
		checkIn = 0
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO task_attempts (
			tenant_id, task_id, attempt, job_name, status, failure_class, failure_reason,
			dispatched_at, queued_at, started_at, finished_at,
			execution_seconds, exit_code, error_message, log_s3_key,
			run_results_uri, parse_cache, parse_cache_reason, last_report_at, next_check_at)
		VALUES ($19, $1, $2, $3, $4, $5, $6, COALESCE($7, now()), $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now(),
		        CASE WHEN $18::float8 > 0 THEN now() + make_interval(secs => $18::float8) END)
		ON CONFLICT (tenant_id, task_id, attempt) DO UPDATE SET
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
			next_check_at      = CASE WHEN EXCLUDED.status IN ('succeeded', 'failed', 'stopped') THEN NULL
			                          WHEN $18::float8 > 0 THEN EXCLUDED.next_check_at
			                          ELSE task_attempts.next_check_at END`,
		a.TaskID, a.Number, a.JobName, string(a.Status), a.FailureClass, a.FailureReason,
		a.DispatchedAt, a.QueuedAt, a.StartedAt, a.FinishedAt,
		a.ExecutionSeconds, a.ExitCode, a.ErrorMessage, a.LogS3Key,
		a.RunResultsURI, a.ParseCache, a.ParseCacheReason, checkIn.Seconds(), tenantID)
	if err != nil {
		return fmt.Errorf("save attempt %d of task_id %s: %w", a.Number, a.TaskID, err)
	}
	return nil
}

// addAttempts inserts the tenant's attempts run-controller has just dispatched with one
// INSERT over arrays: each with its dispatched_at (the database clock when nil) and
// next_check_at NextCheckIn after the database clock, which the database
// evaluates, so the attempt clocks compare against one clock.
func addAttempts(ctx context.Context, tx *sqlx.Tx, tenantID string, attempts []run.Attempt) error {
	if len(attempts) == 0 {
		return nil
	}
	taskIDs := make([]uuid.UUID, len(attempts))
	numbers := make([]int32, len(attempts))
	jobNames := make([]string, len(attempts))
	statuses := make([]string, len(attempts))
	dispatched := make([]*time.Time, len(attempts))
	checkIn := make([]float64, len(attempts))
	for i, a := range attempts {
		taskIDs[i], numbers[i], jobNames[i], statuses[i] = a.TaskID, a.Number, a.JobName, string(a.Status)
		dispatched[i], checkIn[i] = a.DispatchedAt, a.NextCheckIn.Seconds()
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO task_attempts (tenant_id, task_id, attempt, job_name, status, dispatched_at, next_check_at)
		SELECT $7, u.task_id, u.attempt, u.job_name, u.status, COALESCE(u.dispatched_at, now()),
		       CASE WHEN u.check_in > 0 THEN now() + make_interval(secs => u.check_in) END
		FROM unnest($1::uuid[], $2::int[], $3::text[], $4::text[], $5::timestamptz[], $6::float8[])
		     AS u(task_id, attempt, job_name, status, dispatched_at, check_in)`,
		pq.Array(taskIDs), pq.Array(numbers), pq.Array(jobNames), pq.Array(statuses), pq.Array(dispatched), pq.Array(checkIn),
		tenantID)
	if err != nil {
		return fmt.Errorf("add %d attempts: %w", len(attempts), err)
	}
	return nil
}

// requestStops asks the current attempt of each stopping task of the tenant's
// run to stop when that attempt is open: stop_requested_at is the database clock,
// checks_sent restarts at 0 so the re-sends of the cancel count from the stop,
// and next_check_at is run.NextStopCheckAfter(0) after the database clock. It
// returns those attempts, in task id order.
func requestStops(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID) ([]run.StopTarget, error) {
	var rows []struct {
		TaskID     uuid.UUID `db:"task_id"`
		Attempt    int32     `db:"attempt"`
		JobName    string    `db:"job_name"`
		ChecksSent int       `db:"checks_sent"`
	}
	if err := tx.SelectContext(ctx, &rows, `
		UPDATE task_attempts a
		SET stop_requested_at = now(),
		    checks_sent       = 0,
		    next_check_at     = now() + make_interval(secs => $2::float8)
		FROM tasks t
		WHERE a.tenant_id = t.tenant_id
		  AND a.task_id = t.task_id
		  AND a.attempt = t.attempt
		  AND t.tenant_id = $3
		  AND t.run_id = $1
		  AND t.status = 'stopping'
		  AND a.status IN ('dispatched', 'queued', 'running')
		RETURNING a.task_id, a.attempt, a.job_name, a.checks_sent`,
		runID, run.NextStopCheckAfter(0).Seconds(), tenantID); err != nil {
		return nil, fmt.Errorf("request the stops of run %s: %w", runID, err)
	}
	targets := make([]run.StopTarget, 0, len(rows))
	for _, r := range rows {
		targets = append(targets, run.StopTarget{TaskID: r.TaskID, Attempt: r.Attempt, JobName: r.JobName, ChecksSent: r.ChecksSent})
	}
	slices.SortFunc(targets, func(x, y run.StopTarget) int { return strings.Compare(x.TaskID.String(), y.TaskID.String()) })
	return targets, nil
}
