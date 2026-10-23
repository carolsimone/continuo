package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// AttemptRepository implements repository.AttemptRepository: the attempt
// questions for one tenant, the stalled runs for the whole install. Its lists
// read through the autocommit db, its lock and write through the unit of
// work's transaction.
type AttemptRepository struct {
	db       *sqlx.DB
	tx       *sqlx.Tx
	tenantID string
}

var _ repository.AttemptRepository = (*AttemptRepository)(nil)

// NewAttemptRepository binds the repository to db, tx (nil outside a
// transaction) and tenantID, whose topology the questions read.
func NewAttemptRepository(db *sqlx.DB, tx *sqlx.Tx, tenantID string) *AttemptRepository {
	return &AttemptRepository{db: db, tx: tx, tenantID: tenantID}
}

// DueAttempts reads the open attempts whose next check is due, through
// idx_task_attempts_due, keeping those that no stop was requested of, whose run
// is pending or running and whose task's node the run's release holds for the
// tenant: exactly the attempts LockQuestion can lock, so one it never could cannot hold the head of the
// due order.
func (r *AttemptRepository) DueAttempts(ctx context.Context, limit int) ([]repository.AttemptKey, error) {
	var rows []struct {
		TaskID  uuid.UUID `db:"task_id"`
		Attempt int32     `db:"attempt"`
	}
	if err := r.db.SelectContext(ctx, &rows, `
		SELECT a.task_id, a.attempt
		FROM task_attempts a
		JOIN task_tracker t ON t.task_id = a.task_id
		JOIN scheduler_tracker s ON s.schedule_id = t.schedule_id
		JOIN topology_nodes n ON n.tenant_id = $2 AND n.release_id = s.release_id AND n.node_key = t.node_key
		WHERE a.status IN ('dispatched', 'queued', 'running') AND a.next_check_at <= now()
		  AND a.stop_requested_at IS NULL
		  AND s.status IN ('pending', 'running')
		ORDER BY a.next_check_at, a.task_id, a.attempt
		LIMIT $1`, limit, r.tenantID); err != nil {
		return nil, fmt.Errorf("read the due attempts: %w", err)
	}
	out := make([]repository.AttemptKey, 0, len(rows))
	for _, row := range rows {
		out = append(out, repository.AttemptKey{TaskID: row.TaskID, Attempt: row.Attempt})
	}
	return out, nil
}

// StalledRuns reads the active runs (pending, running or cancelling) with an
// old progress clock and no task in flight (a stopping task is in flight), of every tenant: scheduler_tracker has no tenant column. The run
// scan filters scheduler_tracker by status, which holds few active rows, and
// the in-flight check probes idx_task_tracker_schedule_id.
func (r *AttemptRepository) StalledRuns(ctx context.Context, idle time.Duration, limit int) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	if err := r.db.SelectContext(ctx, &ids, `
		SELECT s.schedule_id
		FROM scheduler_tracker s
		WHERE s.status IN ('pending', 'running', 'cancelling')
		  AND s.last_heartbeat_at < now() - make_interval(secs => $1::float8)
		  AND NOT EXISTS (
		      SELECT 1 FROM task_tracker t
		      WHERE t.schedule_id = s.schedule_id AND t.status IN ('dispatched', 'queued', 'running', 'stopping'))
		ORDER BY s.last_heartbeat_at, s.schedule_id
		LIMIT $2`, idle.Seconds(), limit); err != nil {
		return nil, fmt.Errorf("read the stalled runs: %w", err)
	}
	return ids, nil
}

type questionRow struct {
	RunID        uuid.UUID `db:"schedule_id"`
	ScheduleName string    `db:"run_schedule_name"`
	TaskID       uuid.UUID `db:"task_id"`
	Attempt      int32     `db:"attempt"`
	Status       string    `db:"status"`
	ChecksSent   int       `db:"checks_sent"`
	JobName      string    `db:"job_name"`
	Operation    string    `db:"operation"`
	topologyNodeRow
}

// LockQuestion locks the attempt row, skipping one another transaction holds,
// and reads its task, run and the node of the run's release, re-checking that
// the attempt is still open, due and not asked to stop and its run pending or
// running.
func (r *AttemptRepository) LockQuestion(ctx context.Context, key repository.AttemptKey) (run.Question, bool, error) {
	if r.tx == nil {
		return run.Question{}, false, errors.New("LockQuestion requires an active transaction")
	}
	var row questionRow
	err := r.tx.GetContext(ctx, &row, `
		SELECT s.schedule_id, s.schedule_name AS run_schedule_name, a.task_id, a.attempt, a.status, a.checks_sent, a.job_name,
		       t.operation,
		       n.node_key, n.unique_id, n.service_name, n.schema_name, n.table_name, n.node_type, n.test_count,
		       n.content_hash, n.image_tag, n.secret_ref, n.schedule_name, n.timeout_seconds
		FROM task_attempts a
		JOIN task_tracker t ON t.task_id = a.task_id
		JOIN scheduler_tracker s ON s.schedule_id = t.schedule_id
		JOIN topology_nodes n ON n.tenant_id = $3 AND n.release_id = s.release_id AND n.node_key = t.node_key
		WHERE a.task_id = $1 AND a.attempt = $2
		  AND a.status IN ('dispatched', 'queued', 'running') AND a.next_check_at <= now()
		  AND a.stop_requested_at IS NULL
		  AND s.status IN ('pending', 'running')
		FOR UPDATE OF a SKIP LOCKED`, key.TaskID, key.Attempt, r.tenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return run.Question{}, false, nil
	}
	if err != nil {
		return run.Question{}, false, fmt.Errorf("lock attempt %d of task %s: %w", key.Attempt, key.TaskID, err)
	}
	return run.Question{
		RunID:        row.RunID,
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

// MarkQuestioned counts the question and moves the attempt's next check.
func (r *AttemptRepository) MarkQuestioned(ctx context.Context, key repository.AttemptKey, next time.Duration) error {
	if r.tx == nil {
		return errors.New("MarkQuestioned requires an active transaction")
	}
	if _, err := r.tx.ExecContext(ctx, `
		UPDATE task_attempts
		SET checks_sent = checks_sent + 1, next_check_at = now() + make_interval(secs => $3::float8)
		WHERE task_id = $1 AND attempt = $2`, key.TaskID, key.Attempt, next.Seconds()); err != nil {
		return fmt.Errorf("record a question about attempt %d of task %s: %w", key.Attempt, key.TaskID, err)
	}
	return nil
}
