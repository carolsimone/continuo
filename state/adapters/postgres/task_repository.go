package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// TaskRepository defines all operations for tasks table. Every method names
// the tenant whose tasks it reads or writes and touches only that tenant's
// rows, and every lease a read joins is the tenant's; a row to insert carries
// its own tenant.
type TaskRepository interface {
	Create(ctx context.Context, task *TaskRow) error
	GetByID(ctx context.Context, tenantID string, taskID uuid.UUID) (*TaskRow, error)
	GetByRunAndNode(ctx context.Context, tenantID string, runID uuid.UUID, serviceName, schemaName, tableName string) (*TaskRow, error)
	ListByRunID(ctx context.Context, tenantID string, runID uuid.UUID, status *run.TaskStatus, limit, offset int) ([]*TaskRow, int, error)
	// ListByRunTx returns every task of a run in node-key order, reading
	// through tx so a caller that holds the connection does not wait for a
	// second one.
	ListByRunTx(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID) ([]*TaskRow, error)
	// RunReleasesTx maps each task of taskIDs whose run names a release to
	// that release, reading through tx.
	RunReleasesTx(ctx context.Context, tx *sqlx.Tx, tenantID string, taskIDs []uuid.UUID) (map[uuid.UUID]string, error)
	// BulkCreateTx inserts every task of a run within a transaction, with one
	// COPY. A task whose id, or whose node in its run, is already stored fails
	// the whole copy.
	BulkCreateTx(ctx context.Context, tx *sqlx.Tx, tasks []*TaskRow) error
	// SetStatusAndAttemptTx writes status and attempt for the given task,
	// leaving a cancelled row untouched. Returns rows affected.
	SetStatusAndAttemptTx(ctx context.Context, tx *sqlx.Tx, tenantID string, taskID uuid.UUID, status string, attempt int32) (int32, error)
	// LoadAttemptStateTx returns the task's status, stored attempt,
	// max_retries, node_key, job_name, service_name, schema_name, table_name
	// and dispatched_at, locking the row FOR UPDATE so concurrent writers of
	// the task (attempt reports, the cancel) serialize. found is false when
	// the row does not exist. attempt is what the Run aggregate compares a
	// report's attempt with.
	LoadAttemptStateTx(ctx context.Context, tx *sqlx.Tx, tenantID string, taskID uuid.UUID) (row TaskAttemptRow, found bool, err error)
	// MarkDispatchedTx makes the pending, undispatched tasks among taskIDs
	// dispatched, stamping dispatched_at and ending their wait for a node
	// lease, and returns the ids it changed.
	MarkDispatchedTx(ctx context.Context, tx *sqlx.Tx, tenantID string, taskIDs []uuid.UUID, at time.Time) ([]uuid.UUID, error)
	// MarkSkippedTx sets status 'skipped' on the pending, undispatched tasks
	// among taskIDs, ending their wait for a node lease, and returns how many
	// it changed.
	MarkSkippedTx(ctx context.Context, tx *sqlx.Tx, tenantID string, taskIDs []uuid.UUID) (int64, error)
	// SkipPendingTx sets status 'skipped' on the run's pending, undispatched
	// tasks, ending their wait for a node lease, and returns how many it
	// changed.
	SkipPendingTx(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID) (int64, error)
	// HasInFlightTaskTx reports whether any task of the run is dispatched,
	// queued, running or stopping, or is pending and waits for a node lease
	// whose holder is one of those.
	HasInFlightTaskTx(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID) (bool, error)
	// HasFailedTaskTx reports whether any task of the given run has status
	// 'failed' or 'cancelled'.
	HasFailedTaskTx(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID) (bool, error)
	// HasNonSucceededTask returns true iff at least one task for the given run_id
	// has a status other than 'succeeded'. Used by rebase eligibility: a source run is
	// visible as a rebase candidate iff it is terminal AND has ≥1 non-succeeded task.
	HasNonSucceededTask(ctx context.Context, tenantID string, runID uuid.UUID) (bool, error)
	// BeginStopTx cancels the schedule's pending tasks and makes its
	// dispatched, queued and running tasks stopping, stamping each with
	// cancelled_by and cancelled_at (the run's cancellation instant); it ends
	// the wait of each pending task and releases the node lease of each task
	// it cancels, and returns how many it moved to each status.
	BeginStopTx(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID, cancelledBy string, cancelledAt time.Time) (run.StopCounts, error)
}

type taskRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

// NewTaskRepository creates a TaskRepository over db.
func NewTaskRepository(db *sqlx.DB, logger *slog.Logger) TaskRepository {
	return &taskRepository{
		db:     db,
		logger: logger,
	}
}

// Create inserts a new task record into the database, under the row's tenant.
// Operation is normalized here (empty → 'run') so callers that build a TaskRow
// without setting it still satisfy the tasks_operation_check constraint.
func (r *taskRepository) Create(ctx context.Context, task *TaskRow) error {
	if task.TenantID == "" {
		return fmt.Errorf("create task %s: %w", task.TaskID, ErrNoTenant)
	}
	task.Operation = operationWithDefault(task.Operation)
	query := `
		INSERT INTO tasks (
			tenant_id, task_id, run_id, created_at, service_name, schema_name,
			table_name, job_name, status, attempt, max_retries, cancelled_at, cancelled_by,
			image_tag, inherited_from_task_id, operation
		) VALUES (
			:tenant_id, :task_id, :run_id, :created_at, :service_name, :schema_name,
			:table_name, :job_name, :status, :attempt, :max_retries, :cancelled_at, :cancelled_by,
			:image_tag, :inherited_from_task_id, :operation
		)
	`

	_, err := r.db.NamedExecContext(ctx, query, task)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505") {
			r.logger.Warn("Duplicate key violation when creating task",
				"task_id", task.TaskID,
			)
			return ErrDuplicateKey
		}
		r.logger.Error("Failed to create task",
			"task_id", task.TaskID,
			"run_id", task.ScheduleID,
			"error", err,
		)
		return fmt.Errorf("failed to create task: %w", err)
	}

	r.logger.Info("Created task",
		"task_id", task.TaskID,
		"run_id", task.ScheduleID,
		"service", task.ServiceName,
		"table", task.TableName,
	)

	return nil
}

// taskReadQuery selects the task rows (aliased t) of the tenant $1 a task read
// returns, each with waiting_on_run_id: the run whose task holds the node lease
// the task waits for, read from the tenant's leases. The caller appends the
// rest of its WHERE clause, starting with AND, whose parameters start at $2. A
// node has at most one lease per tenant, so the join adds at most one row to a
// task, and a task never reads its own lease as the one it waits for. A task
// waiting for a node no task holds (its grant is on the way) reads a NULL
// waiting_on_run_id.
const taskReadQuery = `
	SELECT t.tenant_id, t.task_id, t.run_id, t.created_at, t.service_name, t.schema_name,
	       t.table_name, t.job_name, t.status, t.attempt, t.max_retries, t.cancelled_at, t.cancelled_by,
	       t.image_tag, t.inherited_from_task_id, t.operation, t.node_key, t.dispatched_at,
	       t.wait_reason, t.lease_wait_since, l.run_id AS waiting_on_run_id
	FROM tasks t
	LEFT JOIN node_leases l
	  ON t.wait_reason = 'node_lease' AND l.tenant_id = t.tenant_id
	 AND l.node_key = t.node_key AND l.task_id <> t.task_id
	WHERE t.tenant_id = $1`

// GetByID retrieves the tenant's task by task_id, with the run holding the
// node lease it waits for.
func (r *taskRepository) GetByID(ctx context.Context, tenantID string, taskID uuid.UUID) (*TaskRow, error) {
	var task TaskRow
	err := r.db.GetContext(ctx, &task, taskReadQuery+` AND t.task_id = $2`, tenantID, taskID)
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Debug("Task not found",
				"task_id", taskID,
			)
			return nil, ErrNotFound
		}
		r.logger.Error("Failed to get task",
			"task_id", taskID,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	r.logger.Debug("Retrieved task",
		"task_id", taskID,
		"status", task.Status,
	)

	return &task, nil
}

// ListByRunTx returns every task of the tenant's run runID, ordered by node
// key (byte order), then by task id for rows that have none.
func (r *taskRepository) ListByRunTx(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID) ([]*TaskRow, error) {
	var tasks []*TaskRow
	err := tx.SelectContext(ctx, &tasks, `
		SELECT tenant_id, task_id, run_id, created_at, service_name, schema_name,
		       table_name, job_name, status, attempt, max_retries, cancelled_at, cancelled_by,
		       image_tag, inherited_from_task_id, operation, node_key, dispatched_at
		FROM tasks
		WHERE tenant_id = $1 AND run_id = $2
		ORDER BY node_key COLLATE "C", task_id`, tenantID, runID)
	if err != nil {
		return nil, fmt.Errorf("list the tasks of run %s: %w", runID, err)
	}
	return tasks, nil
}

// RunReleasesTx maps each of the tenant's tasks of taskIDs whose run names a
// release to that release. A task id the tenant has no row of, or whose run's
// release_id is NULL or empty, is absent.
func (r *taskRepository) RunReleasesTx(ctx context.Context, tx *sqlx.Tx, tenantID string, taskIDs []uuid.UUID) (map[uuid.UUID]string, error) {
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
		FROM tasks t
		JOIN runs s ON s.tenant_id = t.tenant_id AND s.run_id = t.run_id
		WHERE t.tenant_id = $1 AND t.task_id = ANY($2) AND s.release_id <> ''`,
		tenantID, pq.Array(taskIDs))
	if err != nil {
		return nil, fmt.Errorf("read the run releases of %d tasks: %w", len(taskIDs), err)
	}
	for _, row := range rows {
		out[row.TaskID] = row.ReleaseID
	}
	return out, nil
}

// GetByRunAndNode retrieves the tenant's task by run_id and node information,
// with the run holding the node lease it waits for.
func (r *taskRepository) GetByRunAndNode(ctx context.Context, tenantID string, runID uuid.UUID, serviceName, schemaName, tableName string) (*TaskRow, error) {
	var task TaskRow
	err := r.db.GetContext(ctx, &task, taskReadQuery+`
		  AND t.run_id = $2
		  AND t.service_name = $3
		  AND t.schema_name = $4
		  AND t.table_name = $5`,
		tenantID, runID, serviceName, schemaName, tableName)
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Debug("Task not found by run and node",
				"run_id", runID,
				"service_name", serviceName,
				"schema_name", schemaName,
				"table_name", tableName,
			)
			return nil, ErrNotFound
		}
		r.logger.Error("Failed to get task by run and node",
			"run_id", runID,
			"service_name", serviceName,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get task: %w", err)
	}

	r.logger.Debug("Retrieved task by run and node",
		"task_id", task.TaskID,
		"run_id", runID,
		"status", task.Status,
	)

	return &task, nil
}

// ListByRunID retrieves one page of the tasks of the tenant's run, newest
// first, with an optional status filter, and the number of tasks the filter
// matches. Each task carries the run holding the node lease it waits for.
func (r *taskRepository) ListByRunID(ctx context.Context, tenantID string, runID uuid.UUID, status *run.TaskStatus, limit, offset int) ([]*TaskRow, int, error) {
	var statusFilter sql.NullString
	if status != nil {
		statusFilter = sql.NullString{String: string(*status), Valid: true}
	}

	var total int
	if err := r.db.GetContext(ctx, &total, `
		SELECT COUNT(*) FROM tasks
		WHERE tenant_id = $3 AND run_id = $1 AND ($2::text IS NULL OR status = $2)`,
		runID, statusFilter, tenantID); err != nil {
		return nil, 0, fmt.Errorf("failed to count tasks: %w", err)
	}

	var tasks []*TaskRow
	if err := r.db.SelectContext(ctx, &tasks, taskReadQuery+`
		  AND t.run_id = $2 AND ($3::text IS NULL OR t.status = $3)
		ORDER BY t.created_at DESC
		LIMIT $4 OFFSET $5`,
		tenantID, runID, statusFilter, limit, offset); err != nil {
		r.logger.Error("Failed to list tasks by schedule",
			"run_id", runID,
			"error", err,
		)
		return nil, 0, fmt.Errorf("failed to list tasks: %w", err)
	}

	r.logger.Debug("Listed tasks by schedule",
		"run_id", runID,
		"count", len(tasks),
		"total", total,
	)

	return tasks, total, nil
}

// copyTasks streams a run's tasks with COPY … FROM STDIN.
const copyTasks = `COPY tasks (tenant_id, task_id, run_id, created_at, service_name, schema_name, table_name, job_name, status, attempt, max_retries, image_tag, inherited_from_task_id, operation, node_key, dispatched_at) FROM STDIN`

// BulkCreateTx inserts every task of a run inside tx with one COPY, each under
// its row's tenant, so a run of any size is written in one round trip, binds
// no statement parameters, and commits or rolls back with tx. An empty
// operation is stored as 'run', which the tasks_operation_check constraint
// requires; a row with no tenant fails the whole write.
func (r *taskRepository) BulkCreateTx(ctx context.Context, tx *sqlx.Tx, tasks []*TaskRow) error {
	rows := make([][]any, 0, len(tasks))
	for _, t := range tasks {
		if t.TenantID == "" {
			return fmt.Errorf("copy task %s: %w", t.TaskID, ErrNoTenant)
		}
		rows = append(rows, []any{
			t.TenantID, t.TaskID, t.ScheduleID, t.CreatedAt, t.ServiceName, t.SchemaName, t.TableName, t.JobName,
			string(t.Status), t.Attempt, t.MaxRetries, t.ImageTag, t.InheritedFromTaskID,
			operationWithDefault(t.Operation), t.NodeKey, t.DispatchedAt,
		})
	}
	if err := copyRows(ctx, tx, copyTasks, rows); err != nil {
		return fmt.Errorf("copy %d task rows: %w", len(tasks), err)
	}
	return nil
}

// SetStatusAndAttemptTx writes status and attempt for the given task,
// leaving a cancelled row untouched. Returns rows affected (0 if the task is
// cancelled or the row is missing). It applies no attempt logic — the Run
// aggregate decides when a write is warranted under the row lock taken by
// LoadAttemptStateTx.
func (r *taskRepository) SetStatusAndAttemptTx(ctx context.Context, tx *sqlx.Tx, tenantID string, taskID uuid.UUID, status string, attempt int32) (int32, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE tasks
		SET status = $2, attempt = $3
		WHERE tenant_id = $4 AND task_id = $1
		  AND status != 'cancelled'
	`, taskID, status, attempt, tenantID)
	if err != nil {
		return 0, fmt.Errorf("set task status and attempt: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return num.ClampInt32(n), nil
}

// TaskAttemptRow is the part of a task row an attempt report or a
// lease grant reads. RunID is the row's run_id; NodeKey and WaitReason are
// empty when the row has none.
type TaskAttemptRow struct {
	RunID        uuid.UUID  `db:"run_id"`
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

// LoadAttemptStateTx reads the run_id, status, attempt, max_retries,
// node_key (empty when NULL), job_name, service_name, schema_name, table_name,
// dispatched_at and wait_reason (empty when NULL) of the tenant's task row of
// taskID with FOR UPDATE, so concurrent writers of the same task serialize
// within their transactions. A row the tenant does not have yields found ==
// false and no error.
func (r *taskRepository) LoadAttemptStateTx(ctx context.Context, tx *sqlx.Tx, tenantID string, taskID uuid.UUID) (TaskAttemptRow, bool, error) {
	var row TaskAttemptRow
	err := tx.GetContext(ctx, &row,
		`SELECT run_id, status, attempt, max_retries, COALESCE(node_key, '') AS node_key,
		        job_name, service_name, schema_name, table_name, dispatched_at,
		        COALESCE(wait_reason, '') AS wait_reason
		 FROM tasks WHERE tenant_id = $1 AND task_id = $2 FOR UPDATE`,
		tenantID, taskID,
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
func (r *taskRepository) MarkDispatchedTx(ctx context.Context, tx *sqlx.Tx, tenantID string, taskIDs []uuid.UUID, at time.Time) ([]uuid.UUID, error) {
	if len(taskIDs) == 0 {
		return nil, nil
	}
	var stamped []uuid.UUID
	err := tx.SelectContext(ctx, &stamped, `
		UPDATE tasks
		SET dispatched_at = $2, status = 'dispatched', wait_reason = NULL, lease_wait_since = NULL
		WHERE tenant_id = $3 AND task_id = ANY($1) AND status = 'pending' AND dispatched_at IS NULL
		RETURNING task_id`,
		pq.Array(taskIDs), at, tenantID)
	if err != nil {
		return nil, fmt.Errorf("mark %d tasks dispatched: %w", len(taskIDs), err)
	}
	return stamped, nil
}

// MarkSkippedTx sets status = 'skipped' on the tasks of taskIDs that are still
// pending and undispatched, clears what each waited for (wait_reason,
// lease_wait_since), and returns how many it changed.
func (r *taskRepository) MarkSkippedTx(ctx context.Context, tx *sqlx.Tx, tenantID string, taskIDs []uuid.UUID) (int64, error) {
	if len(taskIDs) == 0 {
		return 0, nil
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'skipped', wait_reason = NULL, lease_wait_since = NULL
		WHERE tenant_id = $2 AND task_id = ANY($1) AND status = 'pending' AND dispatched_at IS NULL`,
		pq.Array(taskIDs), tenantID)
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
func (r *taskRepository) SkipPendingTx(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE tasks
		SET status = 'skipped', wait_reason = NULL, lease_wait_since = NULL
		WHERE tenant_id = $2 AND run_id = $1 AND status = 'pending' AND dispatched_at IS NULL`, runID, tenantID)
	if err != nil {
		return 0, fmt.Errorf("skip the pending tasks of run_id %s: %w", runID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("skip the pending tasks of run_id %s: %w", runID, err)
	}
	return n, nil
}

// runWorkInFlight holds for the run row s when the run has work
// in flight: a task of it is in flight; a pending task of it waits for a node
// lease whose holder is in flight, so it waits on that holder's attempt; or a
// pending task of it waits for a node lease behind an older waiter whose run
// is running, so the node's next grant serves that waiter first. A waiter is
// older when its wait began earlier or, for waits that began together, its
// task id is lower: the order nextWaiters grants in, which also skips a waiter
// whose run is not running, so such a waiter queues no one. $1 names the
// in-flight task statuses (dispatched, queued, running and stopping). The
// leases, holders and older waiters it reads are those of the run's tenant,
// so another tenant's task on the same node key never counts. The task checks
// probe idx_tasks_run_id and the older waiters idx_tasks_lease_waiters;
// node_leases holds one row per tenant and node being written. StalledRuns
// lists a run only when this is false, and HasInFlightTaskTx re-checks it
// under the run's lock.
const runWorkInFlight = `(
	EXISTS (
		SELECT 1 FROM tasks t
		WHERE t.tenant_id = s.tenant_id AND t.run_id = s.run_id AND t.status = ANY($1::text[]))
	OR EXISTS (
		SELECT 1 FROM tasks w
		JOIN node_leases l ON l.tenant_id = w.tenant_id AND l.node_key = w.node_key
		JOIN tasks h ON h.tenant_id = l.tenant_id AND h.task_id = l.task_id
		WHERE w.tenant_id = s.tenant_id AND w.run_id = s.run_id AND w.status = 'pending' AND w.wait_reason = 'node_lease'
		  AND h.status = ANY($1::text[]))
	OR EXISTS (
		SELECT 1 FROM tasks w
		JOIN tasks o ON o.tenant_id = w.tenant_id AND o.node_key = w.node_key
		  AND o.wait_reason = 'node_lease' AND o.status = 'pending'
		  AND o.lease_wait_since <= w.lease_wait_since
		  AND (o.lease_wait_since, o.task_id) < (w.lease_wait_since, w.task_id)
		JOIN runs q ON q.tenant_id = o.tenant_id AND q.run_id = o.run_id AND q.status = 'running'
		WHERE w.tenant_id = s.tenant_id AND w.run_id = s.run_id AND w.status = 'pending' AND w.wait_reason = 'node_lease'))`

// HasInFlightTaskTx reports whether the tenant's run has work in flight: a
// task of it is dispatched, queued, running or stopping, or a pending task of
// it waits for a node lease whose holder is, or behind an older waiter of a
// running run (runWorkInFlight).
func (r *taskRepository) HasInFlightTaskTx(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM runs s WHERE s.tenant_id = $3 AND s.run_id = $2 AND `+runWorkInFlight+`)`,
		pq.Array(inFlightTaskStatuses()), runID, tenantID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("has in-flight task check for run_id %s: %w", runID, err)
	}
	return exists, nil
}

// HasFailedTaskTx reports whether any task row of the given run
// has status 'failed' or 'cancelled': a task that ended unsuccessfully (pending
// and running tasks do not match), so its run finalizes failed.
func (r *taskRepository) HasFailedTaskTx(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM tasks WHERE tenant_id = $2 AND run_id = $1 AND status IN ('failed', 'cancelled'))`,
		runID, tenantID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("has failed task check for run_id %s: %w", runID, err)
	}
	return exists, nil
}

// HasNonSucceededTask returns true iff at least one task row for the given
// run_id has a status other than 'succeeded'. Used by rebase eligibility: a
// source run is visible as a rebase candidate iff it is terminal AND has ≥1
// non-succeeded task.
func (r *taskRepository) HasNonSucceededTask(ctx context.Context, tenantID string, runID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.GetContext(ctx, &exists, `
		SELECT EXISTS(SELECT 1 FROM tasks
		              WHERE tenant_id = $2 AND run_id = $1 AND status != 'succeeded')`,
		runID, tenantID,
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
func (r *taskRepository) BeginStopTx(ctx context.Context, tx *sqlx.Tx, tenantID string, runID uuid.UUID, cancelledBy string, cancelledAt time.Time) (run.StopCounts, error) {
	var stopped []struct {
		TaskID uuid.UUID      `db:"task_id"`
		Status run.TaskStatus `db:"status"`
	}
	err := tx.SelectContext(ctx, &stopped, `
		UPDATE tasks t
		SET status           = CASE
		                         WHEN t.status = 'pending' OR NOT EXISTS (
		                           SELECT 1 FROM task_attempts a
		                           WHERE a.tenant_id = t.tenant_id
		                             AND a.task_id = t.task_id
		                             AND a.attempt = t.attempt
		                             AND a.status IN ('dispatched', 'queued', 'running'))
		                         THEN 'cancelled'
		                         ELSE 'stopping'
		                       END,
		    cancelled_at     = $2,
		    cancelled_by     = $3,
		    wait_reason      = NULL,
		    lease_wait_since = NULL
		WHERE t.tenant_id = $4 AND t.run_id = $1
		  AND t.status IN ('pending', 'dispatched', 'queued', 'running')
		RETURNING t.task_id, t.status
	`, runID, cancelledAt, cancelledBy, tenantID)
	if err != nil {
		return run.StopCounts{}, fmt.Errorf("stop the tasks of run_id %s: %w", runID, err)
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
	if err := releaseLeases(ctx, tx, tenantID, cancelled); err != nil {
		return run.StopCounts{}, fmt.Errorf("stop the tasks of run_id %s: %w", runID, err)
	}
	return counts, nil
}
