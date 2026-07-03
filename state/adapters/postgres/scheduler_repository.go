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
)

var (
	// ErrNotFound is returned when a record is not found in the database
	ErrNotFound = errors.New("record not found")
	// ErrDuplicateKey is returned when attempting to insert a duplicate record
	ErrDuplicateKey = errors.New("duplicate key violation")
	// ErrNotCancellable is returned when Cancel affects zero rows (not found or already terminal)
	ErrNotCancellable = errors.New("scheduler not found or already in terminal state")
	// ErrActiveScheduleConflict is returned when an INSERT violates the partial
	// unique index uq_scheduler_tracker_active_per_schedule — another active
	// (pending|running) run already exists for the same schedule_name. This is the
	// concurrent-activation loser; SaveRun maps it to run.ErrScheduleHasActiveRun.
	ErrActiveScheduleConflict = errors.New("schedule already has an active run")
)

// activeScheduleConstraint is the name of the partial unique index that enforces
// one active run per schedule. A 23505 mentioning it means a concurrent activation
// won the race, as opposed to a primary-key (schedule_id) collision.
const activeScheduleConstraint = "uq_scheduler_tracker_active_per_schedule"

// isUniqueViolation reports whether err is a PostgreSQL unique_violation (23505).
func isUniqueViolation(err error) bool {
	return strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505")
}

// SchedulerTrackerRepository defines all operations for scheduler_tracker table
type SchedulerTrackerRepository interface {
	Create(ctx context.Context, tracker *SchedulerTracker) error
	GetByID(ctx context.Context, scheduleID uuid.UUID) (*SchedulerTracker, error)
	// CancelTx cancels a scheduler within an existing transaction. cancelledAt is
	// the authoritative cancellation instant produced by the Run aggregate; it is
	// persisted to both cancelled_at and completed_at so the row matches the value
	// the gRPC response reports.
	CancelTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy, reason string, cancelledAt time.Time) error
	HasActiveSchedule(ctx context.Context, scheduleName string) (bool, error)
	// GetLastRunPerSchedule returns the most recent scheduler_tracker row per schedule name.
	// Only returns schedules that have at least one run.
	GetLastRunPerSchedule(ctx context.Context) (map[string]LastRunData, error)
	// GetActiveScheduler returns the most recently created PENDING or RUNNING run for a schedule.
	// Returns nil, nil when no active run exists (never returns ErrNotFound).
	GetActiveScheduler(ctx context.Context, scheduleName string) (*SchedulerTracker, error)
	// ListStuckCandidates returns active (pending|running) runs whose dispatch has
	// silently stalled: the run has at least one task, no task in 'running', and the
	// most recent task's created_at is strictly older than cutoff. One indexed query.
	ListStuckCandidates(ctx context.Context, cutoff time.Time) ([]StuckCandidate, error)
	// New: tx-accepting variants for atomic HTTP handler
	UpdateInitializationStatusTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, status string) error
	CreateTx(ctx context.Context, tx *sqlx.Tx, tracker *SchedulerTracker) error
	// Task count helpers for event-driven run finalization
	SetTotalTaskCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, total int32) error
	SetTerminalTaskCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, terminal int32) error
	// UpdateStatusTx updates the status column within an existing transaction.
	UpdateStatusTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, status string) error
	// FinalizeRunTx sets status and completed_at = NOW() for terminal-state transitions.
	FinalizeRunTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, status string) error
	// UpdateRunRowTx applies every dirty scheduler_tracker column named in fields
	// as a single UPDATE statement within an existing transaction. Callers must
	// hold a SELECT ... FOR UPDATE lock on the row (LoadRunForUpdate provides it).
	UpdateRunRowTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, fields RunRowUpdate) error
	// GetByIDForUpdateTx retrieves and row-locks the scheduler_tracker row for the given id
	// using SELECT ... FOR UPDATE. Must be called within a transaction.
	GetByIDForUpdateTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID) (*SchedulerTracker, error)
}

// StuckCandidate identifies one active run whose dispatch has silently stalled.
type StuckCandidate struct {
	ScheduleName string
	ScheduleID   uuid.UUID
}

// LastRunData holds the summary of the most recent run for a schedule.
type LastRunData struct {
	ScheduleName string
	ScheduleID   uuid.UUID
	Status       run.SchedulerStatus
	CreatedAt    time.Time
	IsRunning    bool
}

type schedulerTrackerRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

// NewSchedulerTrackerRepository creates a new SchedulerTrackerRepository
func NewSchedulerTrackerRepository(db *sqlx.DB, logger *slog.Logger) SchedulerTrackerRepository {
	return &schedulerTrackerRepository{
		db:     db,
		logger: logger,
	}
}

// Create inserts a new scheduler_tracker record into the database
func (r *schedulerTrackerRepository) Create(ctx context.Context, tracker *SchedulerTracker) error {
	meta, err := tracker.GetServiceMetadata()
	if err != nil {
		return err
	}
	metaJSON, err := marshalServiceMetadata(meta)
	if err != nil {
		return fmt.Errorf("marshal service_metadata: %w", err)
	}

	query := `
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			started_at, completed_at, last_heartbeat_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, service_metadata,
			total_task_count, terminal_task_count,
			kind, source_run_id, initiated_by
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7,
			$8, $9, $10,
			$11, $12,
			$13, $14,
			$15, $16, $17
		)
	`

	_, err = r.db.ExecContext(ctx, query,
		tracker.ScheduleID, tracker.ScheduleName, tracker.Status, tracker.CreatedAt,
		tracker.StartedAt, tracker.CompletedAt, tracker.LastHeartbeatAt,
		tracker.CancelledAt, tracker.CancelledBy, tracker.CancellationReason,
		tracker.InitializationStatus, metaJSON,
		tracker.TotalTaskCount, tracker.TerminalTaskCount,
		kindWithDefault(tracker.Kind), tracker.SourceRunID, identity.OrSystem(tracker.InitiatedBy),
	)
	if err != nil {
		// Check for duplicate key error (PostgreSQL error code 23505)
		if isUniqueViolation(err) {
			if strings.Contains(err.Error(), activeScheduleConstraint) {
				r.logger.Warn("Active-run conflict creating scheduler_tracker",
					"schedule_name", tracker.ScheduleName,
				)
				return ErrActiveScheduleConflict
			}
			r.logger.Warn("Duplicate key violation when creating scheduler_tracker",
				"schedule_id", tracker.ScheduleID,
				"schedule_name", tracker.ScheduleName,
			)
			return ErrDuplicateKey
		}
		r.logger.Error("Failed to create scheduler_tracker",
			"schedule_id", tracker.ScheduleID,
			"schedule_name", tracker.ScheduleName,
			"error", err,
		)
		return fmt.Errorf("failed to create scheduler_tracker: %w", err)
	}

	r.logger.Info("Created scheduler_tracker",
		"schedule_id", tracker.ScheduleID,
		"schedule_name", tracker.ScheduleName,
		"status", tracker.Status,
	)

	return nil
}

// CreateTx inserts a new scheduler_tracker record within an existing transaction.
func (r *schedulerTrackerRepository) CreateTx(ctx context.Context, tx *sqlx.Tx, tracker *SchedulerTracker) error {
	meta, err := tracker.GetServiceMetadata()
	if err != nil {
		return err
	}
	metaJSON, err := marshalServiceMetadata(meta)
	if err != nil {
		return fmt.Errorf("marshal service_metadata: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			started_at, completed_at, last_heartbeat_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, service_metadata,
			total_task_count, terminal_task_count,
			kind, source_run_id, initiated_by
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7,
			$8, $9, $10,
			$11, $12,
			$13, $14,
			$15, $16, $17
		)
	`,
		tracker.ScheduleID, tracker.ScheduleName, tracker.Status, tracker.CreatedAt,
		tracker.StartedAt, tracker.CompletedAt, tracker.LastHeartbeatAt,
		tracker.CancelledAt, tracker.CancelledBy, tracker.CancellationReason,
		tracker.InitializationStatus, metaJSON,
		tracker.TotalTaskCount, tracker.TerminalTaskCount,
		kindWithDefault(tracker.Kind), tracker.SourceRunID, identity.OrSystem(tracker.InitiatedBy),
	)
	if err != nil {
		if isUniqueViolation(err) {
			if strings.Contains(err.Error(), activeScheduleConstraint) {
				return ErrActiveScheduleConflict
			}
			return ErrDuplicateKey
		}
		return fmt.Errorf("failed to create scheduler_tracker: %w", err)
	}
	return nil
}

// GetByID retrieves a scheduler_tracker by schedule_id
func (r *schedulerTrackerRepository) GetByID(ctx context.Context, scheduleID uuid.UUID) (*SchedulerTracker, error) {
	query := `
		SELECT
			schedule_id, schedule_name, status, created_at,
			started_at, completed_at, last_heartbeat_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, service_metadata,
			total_task_count, terminal_task_count,
			kind, source_run_id, initiated_by
		FROM scheduler_tracker
		WHERE schedule_id = $1
	`

	var tracker SchedulerTracker
	err := r.db.GetContext(ctx, &tracker, query, scheduleID)
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Debug("Scheduler tracker not found",
				"schedule_id", scheduleID,
			)
			return nil, ErrNotFound
		}
		r.logger.Error("Failed to get scheduler_tracker",
			"schedule_id", scheduleID,
			"error", err,
		)
		return nil, fmt.Errorf("failed to get scheduler_tracker: %w", err)
	}
	meta, metaErr := tracker.GetServiceMetadata()
	if metaErr != nil {
		return nil, metaErr
	}
	tracker.ServiceMetadata = meta

	r.logger.Debug("Retrieved scheduler_tracker",
		"schedule_id", scheduleID,
		"schedule_name", tracker.ScheduleName,
		"status", tracker.Status,
	)

	return &tracker, nil
}

// CancelTx cancels a scheduler within an existing transaction.
// Cancellation is terminal, so both cancelled_at and completed_at are stamped
// with cancelledAt — the single authoritative instant the aggregate produced —
// keeping the persisted row equal to the value returned to the caller.
func (r *schedulerTrackerRepository) CancelTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy, reason string, cancelledAt time.Time) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE scheduler_tracker
		SET status              = $1,
		    cancelled_at        = $2,
		    completed_at        = $2,
		    cancelled_by        = $3,
		    cancellation_reason = $4
		WHERE schedule_id = $5
		  AND status NOT IN ('succeeded', 'failed', 'cancelled')
	`, run.SchedulerStatusCancelled, cancelledAt, cancelledBy, reason, scheduleID)
	if err != nil {
		return fmt.Errorf("failed to cancel scheduler tx: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrNotCancellable
	}
	return nil
}

// HasActiveSchedule checks if there's a running or pending schedule with the given name
// Returns true if schedule is blocked (active run exists), false if can proceed
func (r *schedulerTrackerRepository) HasActiveSchedule(ctx context.Context, scheduleName string) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1 FROM scheduler_tracker
			WHERE schedule_name = $1
			  AND status IN ('pending', 'running')
			ORDER BY created_at DESC
			LIMIT 1
		)
	`

	var exists bool
	err := r.db.GetContext(ctx, &exists, query, scheduleName)
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

// GetActiveScheduler returns the most recently created PENDING or RUNNING run for the given schedule name.
// Returns nil, nil when no active run exists — never returns ErrNotFound.
func (r *schedulerTrackerRepository) GetActiveScheduler(ctx context.Context, scheduleName string) (*SchedulerTracker, error) {
	query := `
		SELECT
			schedule_id, schedule_name, status, created_at,
			started_at, completed_at, last_heartbeat_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, service_metadata,
			total_task_count, terminal_task_count,
			kind, source_run_id, initiated_by
		FROM scheduler_tracker
		WHERE schedule_name = $1
		  AND status IN ('pending', 'running')
		ORDER BY created_at DESC
		LIMIT 1
	`

	var tracker SchedulerTracker
	err := r.db.GetContext(ctx, &tracker, query, scheduleName)
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
	meta, metaErr := tracker.GetServiceMetadata()
	if metaErr != nil {
		return nil, metaErr
	}
	tracker.ServiceMetadata = meta

	r.logger.Debug("Found active scheduler",
		"schedule_name", scheduleName,
		"schedule_id", tracker.ScheduleID,
		"status", tracker.Status,
	)

	return &tracker, nil
}

// UpdateInitializationStatusTx updates initialization_status within an existing transaction.
func (r *schedulerTrackerRepository) UpdateInitializationStatusTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, status string) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE scheduler_tracker SET initialization_status = $1 WHERE schedule_id = $2`,
		status, scheduleID,
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

// SetTotalTaskCountTx sets total_task_count for the given schedule.
func (r *schedulerTrackerRepository) SetTotalTaskCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, total int32) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE scheduler_tracker
		SET total_task_count = $2
		WHERE schedule_id = $1
	`, id, total)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("scheduler_tracker row not found for id %s: %w", id, ErrNotFound)
	}
	return nil
}

// SetTerminalTaskCountTx writes terminal_task_count absolutely (overwriting
// any prior value) inside the caller's transaction. Callers must hold a
// SELECT ... FOR UPDATE lock on the scheduler_tracker row before invoking;
// the aggregate's LoadRunForUpdate provides this. Decreasing writes succeed
// — the GREATEST monotonic-MAX semantics that lived here pre-aggregate
// existed only to guard against unsynchronised writers, which no longer
// exist after the load-mutate-save refactor.
func (r *schedulerTrackerRepository) SetTerminalTaskCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, terminal int32) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE scheduler_tracker
		SET terminal_task_count = $2
		WHERE schedule_id = $1
	`, id, terminal)
	if err != nil {
		return fmt.Errorf("set terminal_task_count: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("scheduler_tracker row not found for id %s: %w", id, ErrNotFound)
	}
	return nil
}

// GetByIDForUpdateTx retrieves and row-locks the scheduler_tracker row using SELECT ... FOR UPDATE.
// Returns ErrNotFound when no row exists for the given id.
func (r *schedulerTrackerRepository) GetByIDForUpdateTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID) (*SchedulerTracker, error) {
	var tracker SchedulerTracker
	err := tx.QueryRowxContext(ctx, `
		SELECT
			schedule_id, schedule_name, status, created_at,
			started_at, completed_at, last_heartbeat_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, service_metadata,
			total_task_count, terminal_task_count,
			kind, source_run_id, initiated_by
		FROM scheduler_tracker
		WHERE schedule_id = $1
		FOR UPDATE
	`, id).StructScan(&tracker)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get scheduler_tracker for update: %w", err)
	}
	meta, metaErr := tracker.GetServiceMetadata()
	if metaErr != nil {
		return nil, metaErr
	}
	tracker.ServiceMetadata = meta
	return &tracker, nil
}

// UpdateStatusTx updates the status column for the given schedule within a transaction.
func (r *schedulerTrackerRepository) UpdateStatusTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, status string) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE scheduler_tracker SET status = $2 WHERE schedule_id = $1`,
		id, status,
	)
	if err != nil {
		return fmt.Errorf("update scheduler_tracker status: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("scheduler_tracker row not found for id %s: %w", id, ErrNotFound)
	}
	return nil
}

// FinalizeRunTx sets status and completed_at for a terminal-state transition.
func (r *schedulerTrackerRepository) FinalizeRunTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, status string) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE scheduler_tracker SET status = $2, completed_at = NOW() WHERE schedule_id = $1`,
		id, status,
	)
	if err != nil {
		return fmt.Errorf("finalize scheduler_tracker status: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("scheduler_tracker row not found for id %s: %w", id, ErrNotFound)
	}
	return nil
}

// RunRowUpdate carries the scheduler_tracker columns a SaveRun must persist in
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
	// CompletedAtNow, when true, sets completed_at = NOW() (terminal transition).
	CompletedAtNow bool
}

// UpdateRunRowTx applies every dirty column in fields as a single parameterised
// UPDATE. Column names come from a fixed allowlist (never interpolated values),
// and values are bound as parameters. Returns ErrNotFound when the row is
// absent. A no-op fields set (nothing dirty) returns nil without touching the DB.
func (r *schedulerTrackerRepository) UpdateRunRowTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, fields RunRowUpdate) error {
	setClauses := make([]string, 0, 6)
	args := make([]any, 0, 7)

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
	if fields.CompletedAtNow {
		setClauses = append(setClauses, "completed_at = NOW()")
	}

	if len(setClauses) == 0 {
		return nil
	}

	args = append(args, id)
	query := fmt.Sprintf(
		`UPDATE scheduler_tracker SET %s WHERE schedule_id = $%d`,
		strings.Join(setClauses, ", "), len(args),
	)

	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update scheduler_tracker run row: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("scheduler_tracker row not found for id %s: %w", id, ErrNotFound)
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

// GetLastRunPerSchedule returns the most recent row per schedule_name.
// Uses DISTINCT ON for efficient per-group latest-row selection.
func (r *schedulerTrackerRepository) GetLastRunPerSchedule(ctx context.Context) (map[string]LastRunData, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT DISTINCT ON (schedule_name)
		  schedule_name,
		  schedule_id,
		  status,
		  created_at,
		  (status IN ('pending', 'running')) AS is_running
		FROM scheduler_tracker
		ORDER BY schedule_name, created_at DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("get last run per schedule: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]LastRunData)
	for rows.Next() {
		var d LastRunData
		var statusStr string
		if err := rows.Scan(&d.ScheduleName, &d.ScheduleID, &statusStr, &d.CreatedAt, &d.IsRunning); err != nil {
			return nil, fmt.Errorf("scan last run row: %w", err)
		}
		d.Status = run.SchedulerStatus(statusStr)
		result[d.ScheduleName] = d
	}
	return result, rows.Err()
}

// ListStuckCandidates returns one row per active (pending|running) run whose
// dispatch has silently stalled. A run qualifies when it has at least one task,
// none of its tasks is in 'running', and its most recent task's created_at is
// strictly older than cutoff. The aggregation considers ALL of a run's tasks —
// there is no paging blind spot — and replaces the watchdog's per-schedule
// ListTasks fan-out with a single indexed query.
func (r *schedulerTrackerRepository) ListStuckCandidates(ctx context.Context, cutoff time.Time) ([]StuckCandidate, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT st.schedule_name, st.schedule_id
		FROM scheduler_tracker st
		JOIN task_tracker tt ON tt.schedule_id = st.schedule_id
		WHERE st.status IN ('pending', 'running')
		GROUP BY st.schedule_id, st.schedule_name
		HAVING count(*) FILTER (WHERE tt.status = 'running') = 0
		   AND max(tt.created_at) < $1
	`, cutoff)
	if err != nil {
		return nil, fmt.Errorf("list stuck candidates: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []StuckCandidate
	for rows.Next() {
		var c StuckCandidate
		if err := rows.Scan(&c.ScheduleName, &c.ScheduleID); err != nil {
			return nil, fmt.Errorf("scan stuck candidate: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
