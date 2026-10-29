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
	repository "github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// AttemptRepository implements repository.AttemptRepository. Its scans read
// every tenant and name each attempt and run with its tenant; its lock and
// write act on the tenant their key or run names, which the attempt reconcile
// then loads and acts on under that tenant. CountCancelling counts the whole
// install. Its lists read through the autocommit db, its lock and write
// through the unit of work's transaction.
type AttemptRepository struct {
	db *sqlx.DB
	tx *sqlx.Tx
}

var _ repository.AttemptRepository = (*AttemptRepository)(nil)

// NewAttemptRepository binds the repository to db and tx (nil outside a
// transaction).
func NewAttemptRepository(db *sqlx.DB, tx *sqlx.Tx) *AttemptRepository {
	return &AttemptRepository{db: db, tx: tx}
}

// DueAttempts reads the open attempts of every tenant whose next check is
// due, through idx_task_attempts_due, keeping those that no stop was requested
// of, whose run is pending or running and whose task's node the run's release
// holds: exactly the attempts LockQuestion can lock under their tenant, so one
// it never could cannot hold the head of the due order.
func (r *AttemptRepository) DueAttempts(ctx context.Context, limit int) ([]repository.AttemptKey, error) {
	var rows []struct {
		TenantID string    `db:"tenant_id"`
		TaskID   uuid.UUID `db:"task_id"`
		Attempt  int32     `db:"attempt"`
	}
	if err := r.db.SelectContext(ctx, &rows, `
		SELECT a.tenant_id, a.task_id, a.attempt
		FROM task_attempts a
		JOIN tasks t ON t.tenant_id = a.tenant_id AND t.task_id = a.task_id
		JOIN runs s ON s.tenant_id = t.tenant_id AND s.run_id = t.run_id
		JOIN topology_nodes n ON n.tenant_id = s.tenant_id AND n.release_id = s.release_id AND n.node_key = t.node_key
		WHERE a.status IN ('dispatched', 'queued', 'running') AND a.next_check_at <= now()
		  AND a.stop_requested_at IS NULL
		  AND s.status IN ('pending', 'running')
		ORDER BY a.next_check_at, a.tenant_id, a.task_id, a.attempt
		LIMIT $1`, limit); err != nil {
		return nil, fmt.Errorf("read the due attempts: %w", err)
	}
	out := make([]repository.AttemptKey, 0, len(rows))
	for _, row := range rows {
		out = append(out, repository.AttemptKey{TenantID: row.TenantID, TaskID: row.TaskID, Attempt: row.Attempt})
	}
	return out, nil
}

// runRefRow is one run of a scan, with its tenant.
type runRefRow struct {
	TenantID string    `db:"tenant_id"`
	RunID    uuid.UUID `db:"run_id"`
}

// runRefs names each run of rows with its tenant, in order.
func runRefs(rows []runRefRow) []repository.RunRef {
	out := make([]repository.RunRef, 0, len(rows))
	for _, row := range rows {
		out = append(out, repository.RunRef{TenantID: row.TenantID, RunID: row.RunID})
	}
	return out
}

// StalledRuns reads the active runs (pending, running or cancelling) of every
// tenant with an old progress clock and no work in flight (runWorkInFlight: no
// task dispatched, queued, running or stopping, no task waiting for a node
// lease whose holder is, and none waiting behind an older waiter of a running
// run, each within the run's tenant). A run it lists may hold the oldest
// waiter of a free node; the attempt reconcile grants that node under the
// run's lock (run.Run.FailStalled) rather than failing the run. The run scan
// filters runs by status, which holds few active rows.
func (r *AttemptRepository) StalledRuns(ctx context.Context, idle time.Duration, limit int) ([]repository.RunRef, error) {
	var rows []runRefRow
	if err := r.db.SelectContext(ctx, &rows, `
		SELECT s.tenant_id, s.run_id
		FROM runs s
		WHERE s.status = ANY($4::text[])
		  AND s.last_progress_at < now() - make_interval(secs => $2::float8)
		  AND NOT `+runWorkInFlight+`
		ORDER BY s.last_progress_at, s.tenant_id, s.run_id
		LIMIT $3`, pq.Array(inFlightTaskStatuses()), idle.Seconds(), limit,
		pq.Array(activeRunStatuses())); err != nil {
		return nil, fmt.Errorf("read the stalled runs: %w", err)
	}
	return runRefs(rows), nil
}

type questionRow struct {
	RunID        uuid.UUID `db:"run_id"`
	ScheduleName string    `db:"run_schedule_name"`
	TaskID       uuid.UUID `db:"task_id"`
	Attempt      int32     `db:"attempt"`
	Status       string    `db:"status"`
	ChecksSent   int       `db:"checks_sent"`
	JobName      string    `db:"job_name"`
	Operation    string    `db:"operation"`
	topologyNodeRow
}

// LockQuestion locks the attempt row of the key's tenant, skipping one another
// transaction holds, and reads its task, run and the node of the run's
// release, re-checking that the attempt is still open, due and not asked to
// stop and its run pending or running.
func (r *AttemptRepository) LockQuestion(ctx context.Context, key repository.AttemptKey) (run.Question, bool, error) {
	if r.tx == nil {
		return run.Question{}, false, errors.New("LockQuestion requires an active transaction")
	}
	var row questionRow
	err := r.tx.GetContext(ctx, &row, `
		SELECT s.run_id, s.schedule_name AS run_schedule_name, a.task_id, a.attempt, a.status, a.checks_sent, a.job_name,
		       t.operation,
		       n.node_key, n.unique_id, n.service_name, n.schema_name, n.table_name, n.node_type, n.test_count,
		       n.content_hash, n.image_tag, n.secret_ref, n.schedule_name, n.timeout_seconds
		FROM task_attempts a
		JOIN tasks t ON t.tenant_id = a.tenant_id AND t.task_id = a.task_id
		JOIN runs s ON s.tenant_id = t.tenant_id AND s.run_id = t.run_id
		JOIN topology_nodes n ON n.tenant_id = s.tenant_id AND n.release_id = s.release_id AND n.node_key = t.node_key
		WHERE a.tenant_id = $3 AND a.task_id = $1 AND a.attempt = $2
		  AND a.status IN ('dispatched', 'queued', 'running') AND a.next_check_at <= now()
		  AND a.stop_requested_at IS NULL
		  AND s.status IN ('pending', 'running')
		FOR UPDATE OF a SKIP LOCKED`, key.TaskID, key.Attempt, key.TenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return run.Question{}, false, nil
	}
	if err != nil {
		return run.Question{}, false, fmt.Errorf("lock attempt %d of task %s: %w", key.Attempt, key.TaskID, err)
	}
	return run.Question{
		RunID:        row.RunID,
		TenantID:     key.TenantID,
		ScheduleName: row.ScheduleName,
		TaskID:       row.TaskID,
		Attempt:      row.Attempt,
		Status:       run.AttemptStatus(row.Status),
		ChecksSent:   row.ChecksSent,
		JobName:      row.JobName,
		Node:         row.toDomain(),
		Operation:    operationFromColumn(row.Operation),
	}, true, nil
}

// MarkQuestioned counts the question about the attempt of the key's tenant and
// moves its next check.
func (r *AttemptRepository) MarkQuestioned(ctx context.Context, key repository.AttemptKey, next time.Duration) error {
	if r.tx == nil {
		return errors.New("MarkQuestioned requires an active transaction")
	}
	if _, err := r.tx.ExecContext(ctx, `
		UPDATE task_attempts
		SET checks_sent = checks_sent + 1, next_check_at = now() + make_interval(secs => $3::float8)
		WHERE tenant_id = $4 AND task_id = $1 AND attempt = $2`, key.TaskID, key.Attempt, next.Seconds(), key.TenantID); err != nil {
		return fmt.Errorf("record a question about attempt %d of task %s: %w", key.Attempt, key.TaskID, err)
	}
	return nil
}

// DueStops reads the cancelling runs of every tenant that have an open
// attempt asked to stop whose next check is due, through
// idx_task_attempts_due, the longest due first.
func (r *AttemptRepository) DueStops(ctx context.Context, limit int) ([]repository.RunRef, error) {
	var rows []runRefRow
	if err := r.db.SelectContext(ctx, &rows, `
		SELECT s.tenant_id, s.run_id
		FROM task_attempts a
		JOIN tasks t ON t.tenant_id = a.tenant_id AND t.task_id = a.task_id
		JOIN runs s ON s.tenant_id = t.tenant_id AND s.run_id = t.run_id
		WHERE a.status IN ('dispatched', 'queued', 'running') AND a.next_check_at <= now()
		  AND a.stop_requested_at IS NOT NULL
		  AND s.status = 'cancelling'
		GROUP BY s.tenant_id, s.run_id
		ORDER BY min(a.next_check_at), s.tenant_id, s.run_id
		LIMIT $1`, limit); err != nil {
		return nil, fmt.Errorf("read the cancelling runs with a quiet stop: %w", err)
	}
	return runRefs(rows), nil
}

type stopRow struct {
	RunID        uuid.UUID  `db:"run_id"`
	ScheduleName string     `db:"schedule_name"`
	CancelledBy  *string    `db:"cancelled_by"`
	Reason       *string    `db:"cancellation_reason"`
	CancelledAt  *time.Time `db:"cancelled_at"`
	TaskID       uuid.UUID  `db:"task_id"`
	Attempt      int32      `db:"attempt"`
	JobName      string     `db:"job_name"`
	ChecksSent   int        `db:"checks_sent"`
	Due          bool       `db:"due"`
}

// LockStops locks every open attempt of the cancelling run ref names, under
// the run's tenant, that was asked to stop, skipping one another transaction
// holds, and reads the run's cancel with them. It finds nothing when none of
// the attempts it locked is due.
func (r *AttemptRepository) LockStops(ctx context.Context, ref repository.RunRef) (run.StopQuestion, bool, error) {
	if r.tx == nil {
		return run.StopQuestion{}, false, errors.New("LockStops requires an active transaction")
	}
	var rows []stopRow
	if err := r.tx.SelectContext(ctx, &rows, `
		SELECT s.run_id, s.schedule_name, s.cancelled_by, s.cancellation_reason, s.cancelled_at,
		       a.task_id, a.attempt, a.job_name, a.checks_sent, a.next_check_at <= now() AS due
		FROM task_attempts a
		JOIN tasks t ON t.tenant_id = a.tenant_id AND t.task_id = a.task_id
		JOIN runs s ON s.tenant_id = t.tenant_id AND s.run_id = t.run_id
		WHERE s.tenant_id = $2 AND s.run_id = $1 AND s.status = 'cancelling'
		  AND a.status IN ('dispatched', 'queued', 'running')
		  AND a.stop_requested_at IS NOT NULL
		FOR UPDATE OF a SKIP LOCKED`, ref.RunID, ref.TenantID); err != nil {
		return run.StopQuestion{}, false, fmt.Errorf("lock the stops of run %s: %w", ref.RunID, err)
	}
	anyDue := false
	for _, row := range rows {
		anyDue = anyDue || row.Due
	}
	if !anyDue {
		return run.StopQuestion{}, false, nil
	}
	head := rows[0]
	q := run.StopQuestion{RunID: head.RunID, TenantID: ref.TenantID, ScheduleName: head.ScheduleName}
	if head.CancelledBy != nil {
		q.CancelledBy = *head.CancelledBy
	}
	if head.Reason != nil {
		q.Reason = *head.Reason
	}
	if head.CancelledAt != nil {
		q.CancelledAt = *head.CancelledAt
	}
	q.Attempts = make([]run.StopTarget, 0, len(rows))
	for _, row := range rows {
		q.Attempts = append(q.Attempts, run.StopTarget{TaskID: row.TaskID, Attempt: row.Attempt, JobName: row.JobName, ChecksSent: row.ChecksSent})
	}
	slices.SortFunc(q.Attempts, func(x, y run.StopTarget) int { return strings.Compare(x.TaskID.String(), y.TaskID.String()) })
	return q, true, nil
}

// CountCancelling counts the cancelling runs of every tenant and those whose
// cancel is older than overdueAfter on the database clock.
func (r *AttemptRepository) CountCancelling(ctx context.Context, overdueAfter time.Duration) (int, int, error) {
	var counts struct {
		Total   int `db:"total"`
		Overdue int `db:"overdue"`
	}
	if err := r.db.GetContext(ctx, &counts, `
		SELECT count(*) AS total,
		       count(*) FILTER (WHERE cancelled_at < now() - make_interval(secs => $1::float8)) AS overdue
		FROM runs
		WHERE status = 'cancelling'`, overdueAfter.Seconds()); err != nil {
		return 0, 0, fmt.Errorf("count the cancelling runs: %w", err)
	}
	return counts.Total, counts.Overdue, nil
}
