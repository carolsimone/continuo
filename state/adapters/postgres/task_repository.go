package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// TaskTrackerRepository defines all operations for task_tracker table
type TaskTrackerRepository interface {
	Create(ctx context.Context, task *TaskTracker) error
	GetByID(ctx context.Context, taskID uuid.UUID) (*TaskTracker, error)
	GetByScheduleAndNode(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*TaskTracker, error)
	ListByScheduleID(ctx context.Context, scheduleID uuid.UUID, status *run.TaskStatus, limit, offset int) ([]*TaskTracker, int, error)
	// BulkCreateTx inserts multiple task_tracker rows within a transaction (ON CONFLICT DO NOTHING).
	BulkCreateTx(ctx context.Context, tx *sqlx.Tx, tasks []*TaskTracker) error
	// SetStatusAndAttemptTx writes status and retry_count for the given task,
	// leaving a cancelled row untouched. Returns rows affected.
	SetStatusAndAttemptTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID, status string, retryCount int32) (int32, error)
	// ExistsTx reports whether a task_tracker row with the given task_id exists.
	ExistsTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID) (bool, error)
	// LoadStatusAndAttemptTx returns the current status (empty string if the row
	// does not exist) and stored retry_count, locking the row FOR UPDATE so
	// concurrent task.status.updated deliveries for the same task serialize.
	// retry_count is the attempt discriminator the Run aggregate orders updates by.
	LoadStatusAndAttemptTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID) (status string, retryCount int32, err error)
	// HasFailedTaskTx reports whether any task for the given schedule has status = 'failed'.
	HasFailedTaskTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (bool, error)
	// HasRetryableFailedTaskTx reports whether any task for the given schedule has
	// status = 'failed' AND retry_count < max_retries (i.e. k8s will retry it).
	HasRetryableFailedTaskTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (bool, error)
	// HasNonSucceededTask returns true iff at least one task for the given schedule_id
	// has a status other than 'succeeded'. Used by rebase eligibility: a source run is
	// visible as a rebase candidate iff it is terminal AND has ≥1 non-succeeded task.
	HasNonSucceededTask(ctx context.Context, scheduleID uuid.UUID) (bool, error)
	// BulkCancelByScheduleIDTx sets status='cancelled' for all pending/running tasks
	// in a schedule. Returns the number of rows updated.
	BulkCancelByScheduleIDTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy string) (int64, error)
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

// Create inserts a new task_tracker record into the database
func (r *taskTrackerRepository) Create(ctx context.Context, task *TaskTracker) error {
	query := `
		INSERT INTO task_tracker (
			task_id, schedule_id, created_at, service_name, schema_name,
			table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by,
			manifest_version, image_tag, inherited_from_task_id
		) VALUES (
			:task_id, :schedule_id, :created_at, :service_name, :schema_name,
			:table_name, :job_name, :status, :retry_count, :max_retries, :cancelled_at, :cancelled_by,
			:manifest_version, :image_tag, :inherited_from_task_id
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

// GetByID retrieves a task_tracker by task_id
func (r *taskTrackerRepository) GetByID(ctx context.Context, taskID uuid.UUID) (*TaskTracker, error) {
	query := `
		SELECT task_id, schedule_id, created_at, service_name, schema_name,
		       table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by,
		       manifest_version, image_tag, inherited_from_task_id
		FROM task_tracker
		WHERE task_id = $1
	`

	var task TaskTracker
	err := r.db.GetContext(ctx, &task, query, taskID)
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

// GetByScheduleAndNode retrieves a task_tracker by schedule_id and node information
func (r *taskTrackerRepository) GetByScheduleAndNode(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*TaskTracker, error) {
	query := `
		SELECT task_id, schedule_id, created_at, service_name, schema_name,
		       table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by,
		       manifest_version, image_tag, inherited_from_task_id
		FROM task_tracker
		WHERE schedule_id = $1
		  AND service_name = $2
		  AND schema_name = $3
		  AND table_name = $4
	`

	var task TaskTracker
	err := r.db.GetContext(ctx, &task, query, scheduleID, serviceName, schemaName, tableName)
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

// ListByScheduleID retrieves all tasks for a specific schedule with optional status filter
func (r *taskTrackerRepository) ListByScheduleID(ctx context.Context, scheduleID uuid.UUID, status *run.TaskStatus, limit, offset int) ([]*TaskTracker, int, error) {
	whereClauses := []string{"schedule_id = :schedule_id"}
	args := map[string]interface{}{
		"schedule_id": scheduleID,
	}

	if status != nil {
		whereClauses = append(whereClauses, "status = :status")
		args["status"] = *status
	}

	whereClause := strings.Join(whereClauses, " AND ")

	// Count total matching records
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM task_tracker WHERE %s", whereClause)
	var total int

	countStmt, err := r.db.PrepareNamedContext(ctx, countQuery)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to prepare count query: %w", err)
	}
	defer countStmt.Close()

	if err := countStmt.GetContext(ctx, &total, args); err != nil {
		return nil, 0, fmt.Errorf("failed to count tasks: %w", err)
	}

	// Fetch paginated results
	args["limit"] = limit
	args["offset"] = offset

	query := fmt.Sprintf(`
		SELECT task_id, schedule_id, created_at, service_name, schema_name,
		       table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by,
		       manifest_version, image_tag, inherited_from_task_id
		FROM task_tracker
		WHERE %s
		ORDER BY created_at DESC
		LIMIT :limit OFFSET :offset
	`, whereClause)

	stmt, err := r.db.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to prepare query: %w", err)
	}
	defer stmt.Close()

	var tasks []*TaskTracker
	if err := stmt.SelectContext(ctx, &tasks, args); err != nil {
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

// BulkCreateTx inserts multiple task_tracker rows within a transaction.
// Uses ON CONFLICT (task_id) DO NOTHING for idempotent bulk inserts.
func (r *taskTrackerRepository) BulkCreateTx(ctx context.Context, tx *sqlx.Tx, tasks []*TaskTracker) error {
	if len(tasks) == 0 {
		return nil
	}
	query := `
		INSERT INTO task_tracker (
			task_id, schedule_id, created_at, service_name, schema_name,
			table_name, job_name, status, retry_count, max_retries, manifest_version, image_tag, inherited_from_task_id
		) VALUES (
			:task_id, :schedule_id, :created_at, :service_name, :schema_name,
			:table_name, :job_name, :status, :retry_count, :max_retries, :manifest_version, :image_tag, :inherited_from_task_id
		)
		ON CONFLICT (task_id) DO NOTHING
	`
	_, err := tx.NamedExecContext(ctx, query, tasks)
	if err != nil {
		return fmt.Errorf("bulk create task_tracker: %w", err)
	}
	return nil
}

// SetStatusAndAttemptTx writes status and retry_count for the given task,
// leaving a cancelled row untouched. Returns rows affected (0 if the task is
// cancelled or the row is missing; use ExistsTx to distinguish). It applies no
// attempt logic — the Run aggregate decides when a write is warranted under the
// row lock taken by LoadStatusAndAttemptTx.
func (r *taskTrackerRepository) SetStatusAndAttemptTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID, status string, retryCount int32) (int32, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE task_tracker
		SET status = $2, retry_count = $3
		WHERE task_id = $1
		  AND status != 'cancelled'
	`, taskID, status, retryCount)
	if err != nil {
		return 0, fmt.Errorf("set task status and attempt: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int32(n), nil
}

// LoadStatusAndAttemptTx returns the current status and retry_count of the
// task_tracker row for the given task_id, taking a FOR UPDATE row lock so
// concurrent task.status.updated deliveries for the same task serialize within
// their transactions. A missing row yields ("", 0, nil) — a lenient
// missing-row handling so the aggregate can disambiguate replay vs.
// not-yet-projected via its Exists fallback.
func (r *taskTrackerRepository) LoadStatusAndAttemptTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID) (string, int32, error) {
	var (
		status     string
		retryCount int32
	)
	err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(status, ''), COALESCE(retry_count, 0) FROM task_tracker WHERE task_id = $1 FOR UPDATE`,
		taskID,
	).Scan(&status, &retryCount)
	if err == sql.ErrNoRows {
		return "", 0, nil
	}
	if err != nil {
		return "", 0, fmt.Errorf("load task status and attempt for task_id %s: %w", taskID, err)
	}
	return status, retryCount, nil
}

// ExistsTx reports whether a task_tracker row exists for the given task_id.
func (r *taskTrackerRepository) ExistsTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM task_tracker WHERE task_id = $1)`,
		taskID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("exists check for task_id %s: %w", taskID, err)
	}
	return exists, nil
}

// HasFailedTaskTx reports whether any task_tracker row for the given schedule has status = 'failed'.
func (r *taskTrackerRepository) HasFailedTaskTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM task_tracker WHERE schedule_id = $1 AND status = 'failed' LIMIT 1)`,
		scheduleID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("has failed task check for schedule_id %s: %w", scheduleID, err)
	}
	return exists, nil
}

// HasRetryableFailedTaskTx reports whether any task_tracker row for the given schedule
// has status = 'failed' AND retry_count < max_retries (a retry is still pending).
func (r *taskTrackerRepository) HasRetryableFailedTaskTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (bool, error) {
	var exists bool
	err := tx.QueryRowContext(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM task_tracker
			WHERE schedule_id = $1
			  AND status = 'failed'
			  AND retry_count < max_retries
			LIMIT 1
		)`,
		scheduleID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("has retryable failed task check for schedule_id %s: %w", scheduleID, err)
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

// BulkCancelByScheduleIDTx sets status='cancelled' for all pending/running tasks
// in a schedule within the given transaction. Returns the number of rows updated.
func (r *taskTrackerRepository) BulkCancelByScheduleIDTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy string) (int64, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE task_tracker
		SET status       = $1,
		    cancelled_at = $2,
		    cancelled_by = $3
		WHERE schedule_id = $4
		  AND status IN ('pending', 'running')
	`, run.TaskStatusCancelled, time.Now(), cancelledBy, scheduleID)
	if err != nil {
		return 0, fmt.Errorf("failed to bulk cancel tasks: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected after bulk cancel: %w", err)
	}
	return n, nil
}
