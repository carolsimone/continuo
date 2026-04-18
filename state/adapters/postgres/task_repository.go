package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/carolsimone/continuo/state/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// TaskTrackerRepository defines all operations for task_tracker table
type TaskTrackerRepository interface {
	Create(ctx context.Context, task *model.TaskTracker) error
	GetByID(ctx context.Context, taskID uuid.UUID) (*model.TaskTracker, error)
	GetByScheduleAndNode(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*model.TaskTracker, error)
	Update(ctx context.Context, task *model.TaskTracker) error
	Delete(ctx context.Context, taskID uuid.UUID) error
	ListByScheduleID(ctx context.Context, scheduleID uuid.UUID, status *model.TaskStatus, limit, offset int) ([]*model.TaskTracker, int, error)
	List(ctx context.Context, filters TaskFilters) ([]*model.TaskTracker, int, error)
	// New: tx-accepting variant for atomic HTTP handler
	UpdateTx(ctx context.Context, tx *sqlx.Tx, task *model.TaskTracker) error
	// BulkCreateTx inserts multiple task_tracker rows within a transaction (ON CONFLICT DO NOTHING).
	BulkCreateTx(ctx context.Context, tx *sqlx.Tx, tasks []*model.TaskTracker) error
	// ListAllByScheduleID returns all task rows for a schedule (no pagination, no status filter).
	ListAllByScheduleID(ctx context.Context, scheduleID uuid.UUID) ([]*model.TaskTracker, error)
	// ResetTasksTx sets the given tasks to PENDING and returns the number of rows actually
	// modified (already-PENDING rows are excluded from the count).
	ResetTasksTx(ctx context.Context, tx *sqlx.Tx, ids []uuid.UUID) (int32, error)
	// UpdateStatusIfChangedTx updates status and retry_count for the given task only when
	// either field differs from the stored value. Returns the number of rows actually modified.
	UpdateStatusIfChangedTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID, status string, retryCount int32) (int32, error)
	// ExistsTx reports whether a task_tracker row with the given task_id exists.
	ExistsTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID) (bool, error)
	// HasFailedTaskTx reports whether any task for the given schedule has status = 'failed'.
	HasFailedTaskTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID) (bool, error)
}

// TaskFilters defines query filters for List operation
type TaskFilters struct {
	ScheduleID *uuid.UUID
	Status     *model.TaskStatus
	Limit      int
	Offset     int
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
func (r *taskTrackerRepository) Create(ctx context.Context, task *model.TaskTracker) error {
	query := `
		INSERT INTO task_tracker (
			task_id, schedule_id, created_at, service_name, schema_name,
			table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by
		) VALUES (
			:task_id, :schedule_id, :created_at, :service_name, :schema_name,
			:table_name, :job_name, :status, :retry_count, :max_retries, :cancelled_at, :cancelled_by
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
func (r *taskTrackerRepository) GetByID(ctx context.Context, taskID uuid.UUID) (*model.TaskTracker, error) {
	query := `
		SELECT task_id, schedule_id, created_at, service_name, schema_name,
		       table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by
		FROM task_tracker
		WHERE task_id = $1
	`

	var task model.TaskTracker
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
func (r *taskTrackerRepository) GetByScheduleAndNode(ctx context.Context, scheduleID uuid.UUID, serviceName, schemaName, tableName string) (*model.TaskTracker, error) {
	query := `
		SELECT task_id, schedule_id, created_at, service_name, schema_name,
		       table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by
		FROM task_tracker
		WHERE schedule_id = $1
		  AND service_name = $2
		  AND schema_name = $3
		  AND table_name = $4
	`

	var task model.TaskTracker
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

// Update updates task status, retry count, and cancellation fields
func (r *taskTrackerRepository) Update(ctx context.Context, task *model.TaskTracker) error {
	query := `
		UPDATE task_tracker
		SET status = :status,
			retry_count = :retry_count,
			cancelled_at = :cancelled_at,
			cancelled_by = :cancelled_by
		WHERE task_id = :task_id
	`

	result, err := r.db.NamedExecContext(ctx, query, task)
	if err != nil {
		r.logger.Error("Failed to update task_tracker",
			"task_id", task.TaskID,
			"error", err,
		)
		return fmt.Errorf("failed to update task_tracker: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Debug("Task tracker not found for update",
			"task_id", task.TaskID,
		)
		return ErrNotFound
	}

	r.logger.Info("Updated task_tracker",
		"task_id", task.TaskID,
		"status", task.Status,
		"retry_count", task.RetryCount,
	)

	return nil
}

// Delete removes a task_tracker record from the database
func (r *taskTrackerRepository) Delete(ctx context.Context, taskID uuid.UUID) error {
	query := `DELETE FROM task_tracker WHERE task_id = $1`

	result, err := r.db.ExecContext(ctx, query, taskID)
	if err != nil {
		r.logger.Error("Failed to delete task_tracker",
			"task_id", taskID,
			"error", err,
		)
		return fmt.Errorf("failed to delete task_tracker: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Debug("Task tracker not found for delete",
			"task_id", taskID,
		)
		return ErrNotFound
	}

	r.logger.Info("Deleted task_tracker",
		"task_id", taskID,
	)

	return nil
}

// UpdateTx updates task status and retry count within an existing transaction.
func (r *taskTrackerRepository) UpdateTx(ctx context.Context, tx *sqlx.Tx, task *model.TaskTracker) error {
	query := `
		UPDATE task_tracker
		SET status = :status,
			retry_count = :retry_count,
			cancelled_at = :cancelled_at,
			cancelled_by = :cancelled_by
		WHERE task_id = :task_id
	`
	result, err := tx.NamedExecContext(ctx, query, task)
	if err != nil {
		return fmt.Errorf("failed to update task_tracker: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

// ListByScheduleID retrieves all tasks for a specific schedule with optional status filter
func (r *taskTrackerRepository) ListByScheduleID(ctx context.Context, scheduleID uuid.UUID, status *model.TaskStatus, limit, offset int) ([]*model.TaskTracker, int, error) {
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
		       table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by
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

	var tasks []*model.TaskTracker
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
func (r *taskTrackerRepository) BulkCreateTx(ctx context.Context, tx *sqlx.Tx, tasks []*model.TaskTracker) error {
	if len(tasks) == 0 {
		return nil
	}
	query := `
		INSERT INTO task_tracker (
			task_id, schedule_id, created_at, service_name, schema_name,
			table_name, job_name, status, retry_count, max_retries
		) VALUES (
			:task_id, :schedule_id, :created_at, :service_name, :schema_name,
			:table_name, :job_name, :status, :retry_count, :max_retries
		)
		ON CONFLICT (task_id) DO NOTHING
	`
	_, err := tx.NamedExecContext(ctx, query, tasks)
	if err != nil {
		return fmt.Errorf("bulk create task_tracker: %w", err)
	}
	return nil
}

// ListAllByScheduleID returns all task_tracker rows for a schedule without pagination.
func (r *taskTrackerRepository) ListAllByScheduleID(ctx context.Context, scheduleID uuid.UUID) ([]*model.TaskTracker, error) {
	var tasks []*model.TaskTracker
	err := r.db.SelectContext(ctx, &tasks, `
		SELECT task_id, schedule_id, created_at, service_name, schema_name,
		       table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by
		FROM task_tracker
		WHERE schedule_id = $1
		ORDER BY created_at ASC
	`, scheduleID)
	if err != nil {
		return nil, fmt.Errorf("list all tasks by schedule_id: %w", err)
	}
	return tasks, nil
}

// UpdateStatusIfChangedTx updates status and retry_count for the given task only when
// either value differs from what is currently stored. Returns the number of rows modified
// (0 means the row exists but nothing changed; use ExistsTx to distinguish from missing).
func (r *taskTrackerRepository) UpdateStatusIfChangedTx(ctx context.Context, tx *sqlx.Tx, taskID uuid.UUID, status string, retryCount int32) (int32, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE task_tracker
		SET status = $2, retry_count = $3
		WHERE task_id = $1
		  AND (status != $2 OR retry_count != $3)
	`, taskID, status, retryCount)
	if err != nil {
		return 0, fmt.Errorf("update task status if changed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int32(n), nil
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

// ResetTasksTx moves the given tasks to PENDING and returns the number of rows
// actually modified (already-PENDING rows are excluded).
func (r *taskTrackerRepository) ResetTasksTx(ctx context.Context, tx *sqlx.Tx, ids []uuid.UUID) (int32, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids)+1)
	args[0] = string(model.TaskStatusPending)
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args[i+1] = id // bind as uuid.UUID so the PK index is used
	}
	query := fmt.Sprintf(`
		UPDATE task_tracker
		SET status = $1
		WHERE task_id IN (%s) AND status != $1
	`, strings.Join(placeholders, ", "))

	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("reset tasks: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int32(n), nil
}

// List queries tasks with flexible filters and pagination
func (r *taskTrackerRepository) List(ctx context.Context, filters TaskFilters) ([]*model.TaskTracker, int, error) {
	// Build dynamic WHERE clause
	whereClauses := []string{}
	args := map[string]interface{}{}

	if filters.ScheduleID != nil {
		whereClauses = append(whereClauses, "schedule_id = :schedule_id")
		args["schedule_id"] = *filters.ScheduleID
	}

	if filters.Status != nil {
		whereClauses = append(whereClauses, "status = :status")
		args["status"] = *filters.Status
	}

	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Count total matching records
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM task_tracker %s", whereClause)
	var total int

	if len(args) > 0 {
		countStmt, err := r.db.PrepareNamedContext(ctx, countQuery)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to prepare count query: %w", err)
		}
		defer countStmt.Close()

		if err := countStmt.GetContext(ctx, &total, args); err != nil {
			return nil, 0, fmt.Errorf("failed to count tasks: %w", err)
		}
	} else {
		if err := r.db.GetContext(ctx, &total, countQuery); err != nil {
			return nil, 0, fmt.Errorf("failed to count tasks: %w", err)
		}
	}

	// Fetch paginated results
	args["limit"] = filters.Limit
	args["offset"] = filters.Offset

	query := fmt.Sprintf(`
		SELECT task_id, schedule_id, created_at, service_name, schema_name,
		       table_name, job_name, status, retry_count, max_retries, cancelled_at, cancelled_by
		FROM task_tracker
		%s
		ORDER BY created_at DESC
		LIMIT :limit OFFSET :offset
	`, whereClause)

	stmt, err := r.db.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to prepare query: %w", err)
	}
	defer stmt.Close()

	var tasks []*model.TaskTracker
	if err := stmt.SelectContext(ctx, &tasks, args); err != nil {
		r.logger.Error("Failed to list tasks", "error", err)
		return nil, 0, fmt.Errorf("failed to list tasks: %w", err)
	}

	r.logger.Debug("Listed tasks",
		"count", len(tasks),
		"total", total,
		"filters", filters,
	)

	return tasks, total, nil
}
