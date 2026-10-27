package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

var (
	// ErrNotFound is returned when a record is not found in the database
	ErrNotFound = errors.New("record not found")
	// ErrDuplicateKey is returned when attempting to insert a duplicate record
	ErrDuplicateKey = errors.New("duplicate key violation")
	// ErrRunNotCancellable is returned when Cancel affects zero rows (not found or already terminal)
	ErrRunNotCancellable = errors.New("scheduler not found or already in terminal state")
	// ErrRunExists is returned by CreateTx when a run row with the
	// same run_id already exists. The INSERT does nothing, so the
	// transaction stays usable.
	ErrRunExists = errors.New("run row already exists")
	// ErrActiveRunConflict is returned when an INSERT violates the partial
	// unique index uq_runs_active_per_schedule — another active
	// (pending|running) run already exists for the same schedule_name. This is the
	// concurrent-activation loser; SaveRun maps it to run.ErrScheduleHasActiveRun.
	ErrActiveRunConflict = errors.New("schedule already has an active run")
)

// activeScheduleConstraint is the name of the partial unique index that enforces
// one active run per schedule. A 23505 mentioning it means a concurrent activation
// won the race, as opposed to a primary-key (run_id) collision.
const activeScheduleConstraint = "uq_runs_active_per_schedule"

// isUniqueViolation reports whether err is a PostgreSQL unique_violation (23505).
func isUniqueViolation(err error) bool {
	return strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505")
}

// RunTableRepository defines all operations for runs table
type RunTableRepository interface {
	Create(ctx context.Context, row *RunRow) error
	GetByID(ctx context.Context, runID uuid.UUID) (*RunRow, error)
	// CancelTx writes the cancel of a run within an existing transaction:
	// status (cancelling or cancelled), cancelled_at, cancelled_by and
	// cancellation_reason, and completed_at when the status is cancelled.
	// cancelledAt is the cancellation instant the Run aggregate produced, so
	// the row matches the value the gRPC response reports.
	CancelTx(ctx context.Context, tx *sqlx.Tx, runID uuid.UUID, status run.RunStatus, cancelledBy, reason string, cancelledAt time.Time) error
	HasActiveRun(ctx context.Context, scheduleName string) (bool, error)
	// GetLastRunPerSchedule returns the most recent run row per schedule name.
	// Only returns schedules that have at least one run.
	GetLastRunPerSchedule(ctx context.Context) (map[string]LastRunData, error)
	// GetActiveRun returns the most recently created PENDING or RUNNING run for a schedule.
	// Returns nil, nil when no active run exists (never returns ErrNotFound).
	GetActiveRun(ctx context.Context, scheduleName string) (*RunRow, error)
	// New: tx-accepting variants for atomic HTTP handler
	UpdateInitializationStatusTx(ctx context.Context, tx *sqlx.Tx, runID uuid.UUID, status string) error
	CreateTx(ctx context.Context, tx *sqlx.Tx, row *RunRow) error
	// Task count helpers for event-driven run finalization
	SetTotalTaskCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, total int32) error
	SetTerminalTaskCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, terminal int32) error
	// UpdateStatusTx updates the status column within an existing transaction.
	UpdateStatusTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, status string) error
	// FinalizeRunTx sets status and completed_at = NOW() for terminal-state transitions.
	FinalizeRunTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, status string) error
	// UpdateRunRowTx applies every dirty runs column named in fields
	// as a single UPDATE statement within an existing transaction. Callers must
	// hold a SELECT ... FOR UPDATE lock on the row (LoadRunForUpdate provides it).
	UpdateRunRowTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, fields RunRowUpdate) error
	// LoadByIDForUpdateTx retrieves and row-locks the run row for the given id
	// using SELECT ... FOR UPDATE. Must be called within a transaction.
	LoadByIDForUpdateTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID) (*RunRow, error)
}

// LastRunData holds the summary of the most recent run for a schedule.
type LastRunData struct {
	ScheduleName string
	RunID        uuid.UUID
	Status       run.RunStatus
	CreatedAt    time.Time
	IsActive     bool
}

type runTableRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

// NewRunTableRepository creates a new RunTableRepository
func NewRunTableRepository(db *sqlx.DB, logger *slog.Logger) RunTableRepository {
	return &runTableRepository{
		db:     db,
		logger: logger,
	}
}

// Create inserts a new run record into the database
func (r *runTableRepository) Create(ctx context.Context, row *RunRow) error {
	meta, err := row.GetServiceMetadata()
	if err != nil {
		return err
	}
	metaJSON, err := marshalServiceMetadata(meta)
	if err != nil {
		return fmt.Errorf("marshal service_metadata: %w", err)
	}

	query := `
		INSERT INTO runs (
			run_id, schedule_name, status, created_at,
			started_at, completed_at, last_progress_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, service_metadata,
			total_task_count, terminal_task_count,
			kind, source_run_id, initiated_by, operation, release_id, promotion_seq
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7,
			$8, $9, $10,
			$11, $12,
			$13, $14,
			$15, $16, $17, $18, $19, $20
		)
	`

	_, err = r.db.ExecContext(ctx, query,
		row.ScheduleID, row.ScheduleName, row.Status, row.CreatedAt,
		row.StartedAt, row.CompletedAt, row.LastProgressAt,
		row.CancelledAt, row.CancelledBy, row.CancellationReason,
		row.InitializationStatus, metaJSON,
		row.TotalTaskCount, row.TerminalTaskCount,
		kindWithDefault(row.Kind), row.SourceRunID, identity.OrSystem(row.InitiatedBy),
		operationWithDefault(row.Operation), row.ReleaseID, row.PromotionSeq,
	)
	if err != nil {
		// Check for duplicate key error (PostgreSQL error code 23505)
		if isUniqueViolation(err) {
			if strings.Contains(err.Error(), activeScheduleConstraint) {
				r.logger.Warn("Active-run conflict creating run",
					"schedule_name", row.ScheduleName,
				)
				return ErrActiveRunConflict
			}
			r.logger.Warn("Duplicate key violation when creating run",
				"run_id", row.ScheduleID,
				"schedule_name", row.ScheduleName,
			)
			return ErrDuplicateKey
		}
		r.logger.Error("Failed to create run",
			"run_id", row.ScheduleID,
			"schedule_name", row.ScheduleName,
			"error", err,
		)
		return fmt.Errorf("failed to create run: %w", err)
	}

	r.logger.Info("Created run",
		"run_id", row.ScheduleID,
		"schedule_name", row.ScheduleName,
		"status", row.Status,
	)

	return nil
}

// CreateTx inserts a new run record within an existing transaction.
// The insert names run_id as its conflict target: a row with the same id
// makes it a no-op reported as ErrRunExists, without aborting the transaction.
// The one-active-run-per-schedule index is not a conflict target, so a
// different id with the same active schedule_name still fails, as
// ErrActiveRunConflict.
func (r *runTableRepository) CreateTx(ctx context.Context, tx *sqlx.Tx, row *RunRow) error {
	meta, err := row.GetServiceMetadata()
	if err != nil {
		return err
	}
	metaJSON, err := marshalServiceMetadata(meta)
	if err != nil {
		return fmt.Errorf("marshal service_metadata: %w", err)
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, schedule_name, status, created_at,
			started_at, completed_at, last_progress_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, service_metadata,
			total_task_count, terminal_task_count,
			kind, source_run_id, initiated_by, operation, release_id, promotion_seq
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7,
			$8, $9, $10,
			$11, $12,
			$13, $14,
			$15, $16, $17, $18, $19, $20
		)
		ON CONFLICT (run_id) DO NOTHING
	`,
		row.ScheduleID, row.ScheduleName, row.Status, row.CreatedAt,
		row.StartedAt, row.CompletedAt, row.LastProgressAt,
		row.CancelledAt, row.CancelledBy, row.CancellationReason,
		row.InitializationStatus, metaJSON,
		row.TotalTaskCount, row.TerminalTaskCount,
		kindWithDefault(row.Kind), row.SourceRunID, identity.OrSystem(row.InitiatedBy),
		operationWithDefault(row.Operation), row.ReleaseID, row.PromotionSeq,
	)
	if err != nil {
		if isUniqueViolation(err) {
			if strings.Contains(err.Error(), activeScheduleConstraint) {
				return ErrActiveRunConflict
			}
			return ErrDuplicateKey
		}
		return fmt.Errorf("failed to create run: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to create run: %w", err)
	}
	if rows == 0 {
		return ErrRunExists
	}
	return nil
}

// GetByID retrieves a run by run_id
func (r *runTableRepository) GetByID(ctx context.Context, runID uuid.UUID) (*RunRow, error) {
	query := `
		SELECT
			run_id, schedule_name, status, created_at,
			started_at, completed_at, last_progress_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, service_metadata,
			total_task_count, terminal_task_count,
			kind, source_run_id, initiated_by, operation, release_id, promotion_seq, planned_at, failure_reason
		FROM runs
		WHERE run_id = $1
	`

	var row RunRow
	err := r.db.GetContext(ctx, &row, query, runID)
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Debug("Run not found",
				"run_id", runID,
			)
			return nil, ErrNotFound
		}
		r.logger.Error("Failed to get run",
			"run_id", runID,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get run: %w", err)
	}
	meta, metaErr := row.GetServiceMetadata()
	if metaErr != nil {
		return nil, metaErr
	}
	row.ServiceMetadata = meta

	r.logger.Debug("Retrieved run",
		"run_id", runID,
		"schedule_name", row.ScheduleName,
		"status", row.Status,
	)

	return &row, nil
}

// CancelTx writes the cancel of a run within an existing transaction. status
// is cancelling (the run waits for its stops) or cancelled (the run has ended,
// so completed_at is stamped too); any other status is refused. Every instant
// it writes is cancelledAt, the one the aggregate produced, keeping the
// persisted row equal to the value returned to the caller. A run in any
// terminal status (run.TerminalRunStatuses) is left untouched and
// reported as ErrRunNotCancellable.
func (r *runTableRepository) CancelTx(ctx context.Context, tx *sqlx.Tx, runID uuid.UUID, status run.RunStatus,
	cancelledBy, reason string, cancelledAt time.Time) error {
	var completedAt *time.Time
	switch status {
	case run.RunStatusCancelling:
	case run.RunStatusCancelled:
		completedAt = &cancelledAt
	default:
		return fmt.Errorf("cancel run %s: status %q is not a cancel", runID, status)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status              = $1,
		    cancelled_at        = $2,
		    completed_at        = $3,
		    cancelled_by        = $4,
		    cancellation_reason = $5
		WHERE run_id = $6
		  AND NOT (status = ANY($7::text[]))
	`, status, cancelledAt, completedAt, cancelledBy, reason, runID, pq.Array(terminalRunStatuses()))
	if err != nil {
		return fmt.Errorf("failed to cancel scheduler tx: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrRunNotCancellable
	}
	return nil
}

// terminalRunStatuses returns the domain's terminal run statuses as
// column values.
func terminalRunStatuses() []string {
	return columnValues(run.TerminalRunStatuses())
}

// activeRunStatuses returns the domain's active run statuses (pending,
// running and cancelling) as column values.
func activeRunStatuses() []string {
	return columnValues(run.ActiveRunStatuses())
}

// inFlightTaskStatuses returns the domain's statuses of a task with an attempt
// in flight (dispatched, queued, running and stopping) as column values.
func inFlightTaskStatuses() []string {
	return columnValues(run.InFlightTaskStatuses())
}

// columnValues returns statuses as the text values their column stores.
func columnValues[S ~string](statuses []S) []string {
	out := make([]string, len(statuses))
	for i, s := range statuses {
		out[i] = string(s)
	}
	return out
}

// HasActiveRun checks if there's a pending, running or cancelling run of the
// schedule with the given name: a cancelling run still holds the schedule.
// Returns true if schedule is blocked (active run exists), false if can proceed
func (r *runTableRepository) HasActiveRun(ctx context.Context, scheduleName string) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1 FROM runs
			WHERE schedule_name = $1
			  AND status = ANY($2::text[])
			ORDER BY created_at DESC
			LIMIT 1
		)
	`

	var exists bool
	err := r.db.GetContext(ctx, &exists, query, scheduleName, pq.Array(activeRunStatuses()))
	if err != nil {
		r.logger.Error("Failed to check active schedule",
			"schedule_name", scheduleName,
			"error", err,
		)
		return false, fmt.Errorf("failed to check active schedule: %w", err)
	}

	r.logger.Debug("Checked active schedule",
		"schedule_name", scheduleName,
		"is_active", exists,
	)

	return exists, nil
}

// GetActiveRun returns the most recently created PENDING, RUNNING or CANCELLING run for the given schedule name.
// Returns nil, nil when no active run exists — never returns ErrNotFound.
func (r *runTableRepository) GetActiveRun(ctx context.Context, scheduleName string) (*RunRow, error) {
	query := `
		SELECT
			run_id, schedule_name, status, created_at,
			started_at, completed_at, last_progress_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, service_metadata,
			total_task_count, terminal_task_count,
			kind, source_run_id, initiated_by, operation, release_id, promotion_seq, planned_at, failure_reason
		FROM runs
		WHERE schedule_name = $1
		  AND status = ANY($2::text[])
		ORDER BY created_at DESC
		LIMIT 1
	`

	var row RunRow
	err := r.db.GetContext(ctx, &row, query, scheduleName, pq.Array(activeRunStatuses()))
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Debug("No active scheduler found", "schedule_name", scheduleName)
			return nil, nil
		}
		r.logger.Error("Failed to get active scheduler",
			"schedule_name", scheduleName,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get active scheduler: %w", err)
	}
	meta, metaErr := row.GetServiceMetadata()
	if metaErr != nil {
		return nil, metaErr
	}
	row.ServiceMetadata = meta

	r.logger.Debug("Found active scheduler",
		"schedule_name", scheduleName,
		"run_id", row.ScheduleID,
		"status", row.Status,
	)

	return &row, nil
}

// UpdateInitializationStatusTx updates initialization_status within an existing transaction.
func (r *runTableRepository) UpdateInitializationStatusTx(ctx context.Context, tx *sqlx.Tx, runID uuid.UUID, status string) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE runs SET initialization_status = $1 WHERE run_id = $2`,
		status, runID,
	)
	if err != nil {
		return fmt.Errorf("failed to update initialization_status: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

// SetTotalTaskCountTx sets total_task_count for the given run.
func (r *runTableRepository) SetTotalTaskCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, total int32) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET total_task_count = $2
		WHERE run_id = $1
	`, id, total)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("run row not found for id %s: %w", id, ErrNotFound)
	}
	return nil
}

// SetTerminalTaskCountTx writes terminal_task_count absolutely (overwriting
// any prior value) inside the caller's transaction. Callers must hold a
// SELECT ... FOR UPDATE lock on the run row before invoking;
// the aggregate's LoadRunForUpdate provides this. Decreasing writes succeed
// — the GREATEST monotonic-MAX semantics that lived here pre-aggregate
// existed only to guard against unsynchronised writers, which no longer
// exist after the load-mutate-save refactor.
func (r *runTableRepository) SetTerminalTaskCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, terminal int32) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET terminal_task_count = $2
		WHERE run_id = $1
	`, id, terminal)
	if err != nil {
		return fmt.Errorf("set terminal_task_count: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("run row not found for id %s: %w", id, ErrNotFound)
	}
	return nil
}

// LoadByIDForUpdateTx retrieves and row-locks the run row using SELECT ... FOR UPDATE.
// Returns ErrNotFound when no row exists for the given id.
func (r *runTableRepository) LoadByIDForUpdateTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID) (*RunRow, error) {
	var row RunRow
	err := tx.QueryRowxContext(ctx, `
		SELECT
			run_id, schedule_name, status, created_at,
			started_at, completed_at, last_progress_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, service_metadata,
			total_task_count, terminal_task_count,
			kind, source_run_id, initiated_by, operation, release_id, promotion_seq, planned_at, failure_reason
		FROM runs
		WHERE run_id = $1
		FOR UPDATE
	`, id).StructScan(&row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get run for update: %w", err)
	}
	meta, metaErr := row.GetServiceMetadata()
	if metaErr != nil {
		return nil, metaErr
	}
	row.ServiceMetadata = meta
	return &row, nil
}

// UpdateStatusTx updates the status column of the given run within a transaction.
func (r *runTableRepository) UpdateStatusTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, status string) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE runs SET status = $2 WHERE run_id = $1`,
		id, status,
	)
	if err != nil {
		return fmt.Errorf("update run status: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("run row not found for id %s: %w", id, ErrNotFound)
	}
	return nil
}

// FinalizeRunTx sets status and completed_at for a terminal-state transition.
func (r *runTableRepository) FinalizeRunTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, status string) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE runs SET status = $2, completed_at = NOW() WHERE run_id = $1`,
		id, status,
	)
	if err != nil {
		return fmt.Errorf("finalize run status: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("run row not found for id %s: %w", id, ErrNotFound)
	}
	return nil
}

// RunRowUpdate carries the runs columns a SaveRun must persist in
// one statement. Each pointer field is nil when the corresponding column is not
// dirty; a non-nil pointer means "write this value". This lets the repository
// build a single UPDATE over a fixed column allowlist while keeping the dirty
// decision in the calling adapter.
type RunRowUpdate struct {
	Status               *string
	InitializationStatus *string
	TotalTaskCount       *int32
	TerminalTaskCount    *int32
	StartedAt            *time.Time
	// LastProgressAt is the run's progress clock: the last dispatch or applied
	// task status change.
	LastProgressAt *time.Time
	// PlannedAt is when state chose and stored the run's tasks.
	PlannedAt *time.Time
	// FailureReason is why the run failed when no task of it failed.
	FailureReason *string
	// CompletedAtNow, when true, sets completed_at = NOW() (terminal transition).
	CompletedAtNow bool
}

// UpdateRunRowTx applies every dirty column in fields as a single parameterised
// UPDATE. Column names come from a fixed allowlist (never interpolated values),
// and values are bound as parameters. Returns ErrNotFound when the row is
// absent. A no-op fields set (nothing dirty) returns nil without touching the DB.
func (r *runTableRepository) UpdateRunRowTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, fields RunRowUpdate) error {
	setClauses := make([]string, 0, 8)
	args := make([]any, 0, 9)

	add := func(column string, value any) {
		args = append(args, value)
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", column, len(args)))
	}

	if fields.Status != nil {
		add("status", *fields.Status)
	}
	if fields.InitializationStatus != nil {
		add("initialization_status", *fields.InitializationStatus)
	}
	if fields.TotalTaskCount != nil {
		add("total_task_count", *fields.TotalTaskCount)
	}
	if fields.TerminalTaskCount != nil {
		add("terminal_task_count", *fields.TerminalTaskCount)
	}
	if fields.StartedAt != nil {
		add("started_at", *fields.StartedAt)
	}
	if fields.LastProgressAt != nil {
		add("last_progress_at", *fields.LastProgressAt)
	}
	if fields.PlannedAt != nil {
		add("planned_at", *fields.PlannedAt)
	}
	if fields.FailureReason != nil {
		add("failure_reason", *fields.FailureReason)
	}
	if fields.CompletedAtNow {
		setClauses = append(setClauses, "completed_at = NOW()")
	}

	if len(setClauses) == 0 {
		return nil
	}

	args = append(args, id)
	query := fmt.Sprintf(
		`UPDATE runs SET %s WHERE run_id = $%d`,
		strings.Join(setClauses, ", "), len(args),
	)

	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update run row: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("run row not found for id %s: %w", id, ErrNotFound)
	}
	return nil
}

// kindWithDefault returns "cron" if kind is empty, else kind — matching the
// schema-level default applied by the migration.
func kindWithDefault(kind string) string {
	if kind == "" {
		return "cron"
	}
	return kind
}

// operationWithDefault returns 'run' for the empty model.OperationRun value,
// else the operation as-is — the runs.operation column is a
// non-empty 4-value domain.
func operationWithDefault(op string) string {
	if op == "" {
		return "run"
	}
	return op
}

// GetLastRunPerSchedule returns the most recent row per schedule_name.
// Uses DISTINCT ON for efficient per-group latest-row selection.
func (r *runTableRepository) GetLastRunPerSchedule(ctx context.Context) (map[string]LastRunData, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT DISTINCT ON (schedule_name)
		  schedule_name,
		  run_id,
		  status,
		  created_at,
		  (status = ANY($1::text[])) AS is_active
		FROM runs
		ORDER BY schedule_name, created_at DESC
	`, pq.Array(activeRunStatuses()))
	if err != nil {
		return nil, fmt.Errorf("get last run per schedule: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]LastRunData)
	for rows.Next() {
		var d LastRunData
		var statusStr string
		if err := rows.Scan(&d.ScheduleName, &d.RunID, &statusStr, &d.CreatedAt, &d.IsActive); err != nil {
			return nil, fmt.Errorf("scan last run row: %w", err)
		}
		d.Status = run.RunStatus(statusStr)
		result[d.ScheduleName] = d
	}
	return result, rows.Err()
}
