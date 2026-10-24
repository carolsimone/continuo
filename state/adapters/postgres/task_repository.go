package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// TaskTrackerRepository defines all operations for task_tracker table
type TaskTrackerRepository interface {
	Create(ctx context.Context, task *TaskTracker) error
	GetByID(ctx context.Context, taskID uuid.UUID) (*TaskTracker, error)
	GetByScheduleAndNode(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*TaskTracker, error)
	ListByScheduleID(ctx context.Context, scheduleID uuid.UUID, status *run.TaskStatus, limit, offset int) ([]*TaskTracker, int, error)
	// ListByRunTx returns every task of a run in node-key order, reading
	// through tx so a caller that holds the connection does not wait for a
	// second one.
	ListByRunTx(ctx context.Context, tx *sqlx.Tx, runID uuid.UUID) ([]*TaskTracker, error)
	// RunReleasesTx maps each task of taskIDs whose run names a release to
	// that release, reading through tx.
	RunReleasesTx(ctx context.Context, tx *sqlx.Tx, taskIDs []uuid.UUID) (map[uuid.UUID]string, error)
	// BulkCreateTx inserts every task of a run within a transaction, with one
	// COPY. A task whose id, or whose node in its run, is already stored fails
	// the whole copy.
	BulkCreateTx(ctx context.Context, tx *sqlx.Tx, tasks []*TaskTracker) error
	// SetStatusAndAttemptTx writes status and attempt for the given task,
	// leaving a cancelled row untouched. Returns rows affected.
	SetStatusAndAttemptTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID, status string, attempt int32) (int32, error)
	// LoadAttemptStateTx returns the task's status, stored attempt,
	// max_retries, node_key, job_name, service_name, schema_name, table_name
	// and dispatched_at, locking the row FOR UPDATE so concurrent writers of
	// the task (attempt reports, the cancel) serialize. found is false when
	// the row does not exist. attempt is what the Run aggregate compares a
	// report's attempt with.
	LoadAttemptStateTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID) (row TaskAttemptRow, found bool, err error)
	// MarkDispatchedTx makes the pending, undispatched tasks among taskIDs
	// dispatched, stamping dispatched_at and ending their wait for a node
	// lease, and returns the ids it changed.
	MarkDispatchedTx(ctx context.Context, tx *sqlx.Tx, taskIDs []uuid.UUID, at time.Time) ([]uuid.UUID, error)
	// MarkSkippedTx sets status 'skipped' on the pending, undispatched tasks
	// among taskIDs, ending their wait for a node lease, and returns how many
	// it changed.
	MarkSkippedTx(ctx context.Context, tx *sqlx.Tx, taskIDs []uuid.UUID) (int64, error)
	// SkipPendingTx sets status 'skipped' on the run's pending, undispatched
	// tasks, ending their wait for a node lease, and returns how many it
	// changed.
	SkipPendingTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (int64, error)
	// HasInFlightTaskTx reports whether any task of the schedule is
	// dispatched, queued, running or stopping, or is pending and waits for a
	// node lease whose holder is one of those.
	HasInFlightTaskTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (bool, error)
	// HasFailedTaskTx reports whether any task for the given schedule has status
	// 'failed' or 'cancelled'.
	HasFailedTaskTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (bool, error)
	// HasNonSucceededTask returns true iff at least one task for the given schedule_id
	// has a status other than 'succeeded'. Used by rebase eligibility: a source run is
	// visible as a rebase candidate iff it is terminal AND has ≥1 non-succeeded task.
	HasNonSucceededTask(ctx context.Context, scheduleID uuid.UUID) (bool, error)
	// BeginStopTx cancels the schedule's pending tasks and makes its
	// dispatched, queued and running tasks stopping, stamping each with
	// cancelled_by and cancelled_at (the run's cancellation instant); it ends
	// the wait of each pending task and releases the node lease of each task
	// it cancels, and returns how many it moved to each status.
	BeginStopTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy string, cancelledAt time.Time) (run.StopCounts, error)
}

type taskTrackerRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

// NewTaskTrackerRepository creates a new TaskTrackerRepository
func NewTaskTrackerRepository(db *sqlx.DB, logger *slog.Logger) TaskTrackerRepository {
	return &taskTrackerRepository{
		db:     db,
		logger: logger,
	}
}

// Create inserts a new task_tracker record into the database. Operation is
// normalized here (empty → 'run') so callers that build a TaskTracker without
// setting it still satisfy the task_tracker_operation_check constraint.
func (r *taskTrackerRepository) Create(ctx context.Context, task *TaskTracker) error {
	task.Operation = operationWithDefault(task.Operation)
	query := `
		INSERT INTO task_tracker (
			task_id, schedule_id, created_at, service_name, schema_name,
			table_name, job_name, status, attempt, max_retries, cancelled_at, cancelled_by,
			image_tag, inherited_from_task_id, operation
		) VALUES (
			:task_id, :schedule_id, :created_at, :service_name, :schema_name,
			:table_name, :job_name, :status, :attempt, :max_retries, :cancelled_at, :cancelled_by,
			:image_tag, :inherited_from_task_id, :operation
		)
	`

	_, err := r.db.NamedExecContext(ctx, query, task)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505") {
			r.logger.Warn("Duplicate key violation when creating task_tracker",
				"task_id", task.TaskID,
			)
			return ErrDuplicateKey
		}
		r.logger.Error("Failed to create task_tracker",
			"task_id", task.TaskID,
			"schedule_id", task.ScheduleID,
			"error", err,
		)
		return fmt.Errorf("failed to create task_tracker: %w", err)
	}

	r.logger.Info("Created task_tracker",
		"task_id", task.TaskID,
		"schedule_id", task.ScheduleID,
		"service", task.ServiceName,
		"table", task.TableName,
	)

	return nil
}

// taskReadQuery selects the task_tracker rows (aliased t) a task read returns,
// each with waiting_on_run_id: the run whose task holds the node lease the task
// waits for. $1 is the tenant whose leases are read; the caller appends its
// WHERE clause, whose parameters start at $2. A node has at most one lease per
// tenant, so the join adds at most one row to a task, and a task never reads
// its own lease as the one it waits for. A task waiting for a node no task
// holds (its grant is on the way) reads a NULL waiting_on_run_id.
const taskReadQuery = `
	SELECT t.task_id, t.schedule_id, t.created_at, t.service_name, t.schema_name,
	       t.table_name, t.job_name, t.status, t.attempt, t.max_retries, t.cancelled_at, t.cancelled_by,
	       t.image_tag, t.inherited_from_task_id, t.operation, t.node_key, t.dispatched_at,
	       t.wait_reason, t.lease_wait_since, l.run_id AS waiting_on_run_id
	FROM task_tracker t
	LEFT JOIN node_leases l
	  ON t.wait_reason = 'node_lease' AND l.tenant_id = $1
	 AND l.node_key = t.node_key AND l.task_id <> t.task_id`

// GetByID retrieves a task_tracker by task_id, with the run holding the node
// lease it waits for read from the install's tenant.
func (r *taskTrackerRepository) GetByID(ctx context.Context, taskID uuid.UUID) (*TaskTracker, error) {
	var task TaskTracker
	err := r.db.GetContext(ctx, &task, taskReadQuery+` WHERE t.task_id = $2`, pkgevents.DefaultTenantID, taskID)
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Debug("Task tracker not found",
				"task_id", taskID,
			)
			return nil, ErrNotFound
		}
		r.logger.Error("Failed to get task_tracker",
			"task_id", taskID,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get task_tracker: %w", err)
	}

	r.logger.Debug("Retrieved task_tracker",
		"task_id", taskID,
		"status", task.Status,
	)

	return &task, nil
}

// ListByRunTx returns every task of runID, ordered by node key (byte order),
// then by task id for rows that have none.
func (r *taskTrackerRepository) ListByRunTx(ctx context.Context, tx *sqlx.Tx, runID uuid.UUID) ([]*TaskTracker, error) {
	var tasks []*TaskTracker
	err := tx.SelectContext(ctx, &tasks, `
		SELECT task_id, schedule_id, created_at, service_name, schema_name,
		       table_name, job_name, status, attempt, max_retries, cancelled_at, cancelled_by,
		       image_tag, inherited_from_task_id, operation, node_key, dispatched_at
		FROM task_tracker
		WHERE schedule_id = $1
		ORDER BY node_key COLLATE "C", task_id`, runID)
	if err != nil {
		return nil, fmt.Errorf("list the tasks of run %s: %w", runID, err)
	}
	return tasks, nil
}

// RunReleasesTx maps each task of taskIDs whose run names a release to that
// release. A task id no row has, or whose run's release_id is NULL or empty,
// is absent.
func (r *taskTrackerRepository) RunReleasesTx(ctx context.Context, tx *sqlx.Tx, taskIDs []uuid.UUID) (map[uuid.UUID]string, error) {
	out := make(map[uuid.UUID]string, len(taskIDs))
	if len(taskIDs) == 0 {
		return out, nil
	}
	var rows []struct {
		TaskID    uuid.UUID `db:"task_id"`
		ReleaseID string    `db:"release_id"`
	}
	err := tx.SelectContext(ctx, &rows, `
		SELECT t.task_id, s.release_id
		FROM task_tracker t
		JOIN scheduler_tracker s ON s.schedule_id = t.schedule_id
		WHERE t.task_id = ANY($1) AND s.release_id <> ''`,
		pq.Array(taskIDs))
	if err != nil {
		return nil, fmt.Errorf("read the run releases of %d tasks: %w", len(taskIDs), err)
	}
	for _, row := range rows {
		out[row.TaskID] = row.ReleaseID
	}
	return out, nil
}

// GetByScheduleAndNode retrieves a task_tracker by schedule_id and node
// information, with the run holding the node lease it waits for read from the
// install's tenant.
func (r *taskTrackerRepository) GetByScheduleAndNode(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*TaskTracker, error) {
	var task TaskTracker
	err := r.db.GetContext(ctx, &task, taskReadQuery+`
		WHERE t.schedule_id = $2
		  AND t.service_name = $3
		  AND t.schema_name = $4
		  AND t.table_name = $5`,
		pkgevents.DefaultTenantID, scheduleID, serviceName, schemaName, tableName)
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Debug("Task tracker not found by schedule and node",
				"schedule_id", scheduleID,
				"service_name", serviceName,
				"schema_name", schemaName,
				"table_name", tableName,
			)
			return nil, ErrNotFound
		}
		r.logger.Error("Failed to get task_tracker by schedule and node",
			"schedule_id", scheduleID,
			"service_name", serviceName,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get task_tracker: %w", err)
	}

	r.logger.Debug("Retrieved task_tracker by schedule and node",
		"task_id", task.TaskID,
		"schedule_id", scheduleID,
		"status", task.Status,
	)

	return &task, nil
}

// ListByScheduleID retrieves one page of a schedule's tasks, newest first,
// with an optional status filter, and the number of tasks the filter matches.
// Each task carries the run holding the node lease it waits for, read from the
// install's tenant.
func (r *taskTrackerRepository) ListByScheduleID(ctx context.Context, scheduleID uuid.UUID, status *run.TaskStatus, limit, offset int) ([]*TaskTracker, int, error) {
	var statusFilter sql.NullString
	if status != nil {
		statusFilter = sql.NullString{String: string(*status), Valid: true}
	}

	var total int
	if err := r.db.GetContext(ctx, &total, `
		SELECT COUNT(*) FROM task_tracker
		WHERE schedule_id = $1 AND ($2::text IS NULL OR status = $2)`,
		scheduleID, statusFilter); err != nil {
		return nil, 0, fmt.Errorf("failed to count tasks: %w", err)
	}

	var tasks []*TaskTracker
	if err := r.db.SelectContext(ctx, &tasks, taskReadQuery+`
		WHERE t.schedule_id = $2 AND ($3::text IS NULL OR t.status = $3)
		ORDER BY t.created_at DESC
		LIMIT $4 OFFSET $5`,
		pkgevents.DefaultTenantID, scheduleID, statusFilter, limit, offset); err != nil {
		r.logger.Error("Failed to list tasks by schedule",
			"schedule_id", scheduleID,
			"error", err,
		)
		return nil, 0, fmt.Errorf("failed to list tasks: %w", err)
	}

	r.logger.Debug("Listed tasks by schedule",
		"schedule_id", scheduleID,
		"count", len(tasks),
		"total", total,
	)

	return tasks, total, nil
}

// copyTaskTracker streams a run's tasks with COPY … FROM STDIN.
const copyTaskTracker = `COPY task_tracker (task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, attempt, max_retries, image_tag, inherited_from_task_id, operation, node_key, dispatched_at) FROM STDIN`

// BulkCreateTx inserts every task of a run inside tx with one COPY, so a run
// of any size is written in one round trip, binds no statement parameters, and
// commits or rolls back with tx. An empty operation is stored as 'run', which
// the task_tracker_operation_check constraint requires.
func (r *taskTrackerRepository) BulkCreateTx(ctx context.Context, tx *sqlx.Tx, tasks []*TaskTracker) error {
	rows := make([][]any, 0, len(tasks))
	for _, t := range tasks {
		rows = append(rows, []any{
			t.TaskID, t.ScheduleID, t.CreatedAt, t.ServiceName, t.SchemaName, t.TableName, t.JobName,
			string(t.Status), t.Attempt, t.MaxRetries, t.ImageTag, t.InheritedFromTaskID,
			operationWithDefault(t.Operation), t.NodeKey, t.DispatchedAt,
		})
	}
	if err := copyRows(ctx, tx, copyTaskTracker, rows); err != nil {
		return fmt.Errorf("copy %d task_tracker rows: %w", len(tasks), err)
	}
	return nil
}

// SetStatusAndAttemptTx writes status and attempt for the given task,
// leaving a cancelled row untouched. Returns rows affected (0 if the task is
// cancelled or the row is missing). It applies no attempt logic — the Run
// aggregate decides when a write is warranted under the row lock taken by
// LoadAttemptStateTx.
func (r *taskTrackerRepository) SetStatusAndAttemptTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID, status string, attempt int32) (int32, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE task_tracker
		SET status = $2, attempt = $3
		WHERE task_id = $1
		  AND status != 'cancelled'
	`, taskID, status, attempt)
	if err != nil {
		return 0, fmt.Errorf("set task status and attempt: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return num.ClampInt32(n), nil
}

// TaskAttemptRow is the part of a task_tracker row an attempt report or a
// lease grant reads. RunID is the row's schedule_id; NodeKey and WaitReason are
// empty when the row has none.
type TaskAttemptRow struct {
	RunID        uuid.UUID  `db:"schedule_id"`
	Status       string     `db:"status"`
	Attempt      int32      `db:"attempt"`
	MaxRetries   int32      `db:"max_retries"`
	NodeKey      string     `db:"node_key"`
	JobName      string     `db:"job_name"`
	ServiceName  string     `db:"service_name"`
	SchemaName   string     `db:"schema_name"`
	TableName    string     `db:"table_name"`
	DispatchedAt *time.Time `db:"dispatched_at"`
	WaitReason   string     `db:"wait_reason"`
}

// LoadAttemptStateTx reads the schedule_id, status, attempt, max_retries,
// node_key (empty when NULL), job_name, service_name, schema_name, table_name,
// dispatched_at and wait_reason (empty when NULL) of the task_tracker row of
// taskID with FOR UPDATE, so concurrent writers of the same task serialize
// within their transactions. A missing row yields found == false and no error.
func (r *taskTrackerRepository) LoadAttemptStateTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID) (TaskAttemptRow, bool, error) {
	var row TaskAttemptRow
	err := tx.GetContext(ctx, &row,
		`SELECT schedule_id, status, attempt, max_retries, COALESCE(node_key, '') AS node_key,
		        job_name, service_name, schema_name, table_name, dispatched_at,
		        COALESCE(wait_reason, '') AS wait_reason
		 FROM task_tracker WHERE task_id = $1 FOR UPDATE`,
		taskID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return TaskAttemptRow{}, false, nil
	}
	if err != nil {
		return TaskAttemptRow{}, false, fmt.Errorf("load the attempt of task_id %s: %w", taskID, err)
	}
	return row, true, nil
}

// MarkDispatchedTx makes the tasks of taskIDs that are still pending and
// undispatched dispatched, with dispatched_at = at, clears what each waited
// for (wait_reason, lease_wait_since), and returns the ids it changed.
func (r *taskTrackerRepository) MarkDispatchedTx(ctx context.Context, tx *sqlx.Tx, taskIDs []uuid.UUID, at time.Time) ([]uuid.UUID, error) {
	if len(taskIDs) == 0 {
		return nil, nil
	}
	var stamped []uuid.UUID
	err := tx.SelectContext(ctx, &stamped, `
		UPDATE task_tracker
		SET dispatched_at = $2, status = 'dispatched', wait_reason = NULL, lease_wait_since = NULL
		WHERE task_id = ANY($1) AND status = 'pending' AND dispatched_at IS NULL
		RETURNING task_id`,
		pq.Array(taskIDs), at)
	if err != nil {
		return nil, fmt.Errorf("mark %d tasks dispatched: %w", len(taskIDs), err)
	}
	return stamped, nil
}

// MarkSkippedTx sets status = 'skipped' on the tasks of taskIDs that are still
// pending and undispatched, clears what each waited for (wait_reason,
// lease_wait_since), and returns how many it changed.
func (r *taskTrackerRepository) MarkSkippedTx(ctx context.Context, tx *sqlx.Tx, taskIDs []uuid.UUID) (int64, error) {
	if len(taskIDs) == 0 {
		return 0, nil
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE task_tracker
		SET status = 'skipped', wait_reason = NULL, lease_wait_since = NULL
		WHERE task_id = ANY($1) AND status = 'pending' AND dispatched_at IS NULL`,
		pq.Array(taskIDs))
	if err != nil {
		return 0, fmt.Errorf("mark %d tasks skipped: %w", len(taskIDs), err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("mark %d tasks skipped: %w", len(taskIDs), err)
	}
	return n, nil
}

// SkipPendingTx sets status = 'skipped' on the run's tasks that are still
// pending and undispatched, clears what each waited for (wait_reason,
// lease_wait_since), and returns how many it changed.
func (r *taskTrackerRepository) SkipPendingTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE task_tracker
		SET status = 'skipped', wait_reason = NULL, lease_wait_since = NULL
		WHERE schedule_id = $1 AND status = 'pending' AND dispatched_at IS NULL`, scheduleID)
	if err != nil {
		return 0, fmt.Errorf("skip the pending tasks of schedule_id %s: %w", scheduleID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("skip the pending tasks of schedule_id %s: %w", scheduleID, err)
	}
	return n, nil
}

// runWorkInFlight holds for the scheduler_tracker row s when the run has work
// in flight: a task of it is in flight, or a pending task of it waits for a
// node lease whose holder is in flight, so it waits on that holder's attempt.
// $1 names the in-flight task statuses (dispatched, queued, running and
// stopping). task_tracker has no tenant column, so a lease on the waiting
// task's node in any tenant counts. The task checks probe
// idx_task_tracker_schedule_id; node_leases holds one row per node being
// written. StalledRuns lists a run only when this is false, and
// HasInFlightTaskTx re-checks it under the run's lock.
const runWorkInFlight = `(
	EXISTS (
		SELECT 1 FROM task_tracker t
		WHERE t.schedule_id = s.schedule_id AND t.status = ANY($1::text[]))
	OR EXISTS (
		SELECT 1 FROM task_tracker w
		JOIN node_leases l ON l.node_key = w.node_key
		JOIN task_tracker h ON h.task_id = l.task_id
		WHERE w.schedule_id = s.schedule_id AND w.status = 'pending' AND w.wait_reason = 'node_lease'
		  AND h.status = ANY($1::text[])))`

// HasInFlightTaskTx reports whether the schedule has work in flight: a task of
// it is dispatched, queued, running or stopping, or a pending task of it waits
// for a node lease whose holder is (runWorkInFlight).
func (r *taskTrackerRepository) HasInFlightTaskTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM scheduler_tracker s WHERE s.schedule_id = $2 AND `+runWorkInFlight+`)`,
		pq.Array(inFlightTaskStatuses()), scheduleID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("has in-flight task check for schedule_id %s: %w", scheduleID, err)
	}
	return exists, nil
}

// HasFailedTaskTx reports whether any task_tracker row for the given schedule
// has status 'failed' or 'cancelled': a task that ended unsuccessfully (pending
// and running tasks do not match), so its run finalizes failed.
func (r *taskTrackerRepository) HasFailedTaskTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM task_tracker WHERE schedule_id = $1 AND status IN ('failed', 'cancelled') LIMIT 1)`,
		scheduleID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("has failed task check for schedule_id %s: %w", scheduleID, err)
	}
	return exists, nil
}

// HasNonSucceededTask returns true iff at least one task_tracker row for the given
// schedule_id has a status other than 'succeeded'. Used by rebase eligibility: a
// source run is visible as a rebase candidate iff it is terminal AND has ≥1
// non-succeeded task.
func (r *taskTrackerRepository) HasNonSucceededTask(ctx context.Context, scheduleID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.GetContext(ctx, &exists, `
		SELECT EXISTS(SELECT 1 FROM task_tracker
		              WHERE schedule_id = $1 AND status != 'succeeded')`,
		scheduleID,
	)
	if err != nil {
		return false, fmt.Errorf("HasNonSucceededTask: %w", err)
	}
	return exists, nil
}

// BeginStopTx stops the schedule's tasks that have not ended, within the given
// transaction: a dispatched, queued or running task whose current attempt is
// open (dispatched, queued or running) becomes stopping, and every other one
// of them, a pending task or a task with no open current attempt, becomes
// cancelled, since no attempt of it can answer a stop. Each is stamped with
// cancelled_at and cancelled_by, and stops waiting for a node lease
// (wait_reason and lease_wait_since are cleared). A task that has ended
// (succeeded, failed, skipped or cancelled) or is already stopping keeps its
// status. The update locks each row it changes until tx ends; a report of an
// attempt of the run locks the run first, so it reads these rows only after
// the cancel commits. The node lease of each task it cancels is released in
// the same transaction, since the cancel ends that task; a stopping task keeps
// its lease until the report that ends it. It returns how many tasks it
// cancelled and how many it made stopping.
func (r *taskTrackerRepository) BeginStopTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy string, cancelledAt time.Time) (run.StopCounts, error) {
	var stopped []struct {
		TaskID uuid.UUID      `db:"task_id"`
		Status run.TaskStatus `db:"status"`
	}
	err := tx.SelectContext(ctx, &stopped, `
		UPDATE task_tracker t
		SET status           = CASE
		                         WHEN t.status = 'pending' OR NOT EXISTS (
		                           SELECT 1 FROM task_attempts a
		                           WHERE a.task_id = t.task_id
		                             AND a.attempt = t.attempt
		                             AND a.status IN ('dispatched', 'queued', 'running'))
		                         THEN 'cancelled'
		                         ELSE 'stopping'
		                       END,
		    cancelled_at     = $2,
		    cancelled_by     = $3,
		    wait_reason      = NULL,
		    lease_wait_since = NULL
		WHERE t.schedule_id = $1
		  AND t.status IN ('pending', 'dispatched', 'queued', 'running')
		RETURNING t.task_id, t.status
	`, scheduleID, cancelledAt, cancelledBy)
	if err != nil {
		return run.StopCounts{}, fmt.Errorf("stop the tasks of schedule_id %s: %w", scheduleID, err)
	}
	var counts run.StopCounts
	var cancelled []uuid.UUID
	for _, s := range stopped {
		switch s.Status {
		case run.TaskStatusCancelled:
			counts.Cancelled++
			cancelled = append(cancelled, s.TaskID)
		case run.TaskStatusStopping:
			counts.Stopping++
		}
	}
	if err := releaseLeases(ctx, tx, cancelled); err != nil {
		return run.StopCounts{}, fmt.Errorf("stop the tasks of schedule_id %s: %w", scheduleID, err)
	}
	return counts, nil
}
