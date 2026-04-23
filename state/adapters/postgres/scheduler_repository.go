package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carolsimone/continuo/state/domain/model"
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
)

// SchedulerTrackerRepository defines all operations for scheduler_tracker table
type SchedulerTrackerRepository interface {
	Create(ctx context.Context, tracker *model.SchedulerTracker) error
	GetByID(ctx context.Context, scheduleID uuid.UUID) (*model.SchedulerTracker, error)
	Update(ctx context.Context, tracker *model.SchedulerTracker) error
	Cancel(ctx context.Context, scheduleID uuid.UUID, cancelledBy, reason string) error
	// CancelTx cancels a scheduler within an existing transaction.
	CancelTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy, reason string) error
	List(ctx context.Context, filters SchedulerFilters) ([]*model.SchedulerTracker, int, error)
	HasActiveSchedule(ctx context.Context, scheduleName string) (bool, error)
	UpdateInitializationStatus(ctx context.Context, scheduleID uuid.UUID, status string) error
	ResetInProgressInitializations(ctx context.Context) (int, error)
	// GetLastRunPerSchedule returns the most recent scheduler_tracker row per schedule name.
	// Only returns schedules that have at least one run.
	GetLastRunPerSchedule(ctx context.Context) (map[string]LastRunData, error)
	// GetActiveScheduler returns the most recently created PENDING or RUNNING run for a schedule.
	// Returns nil, nil when no active run exists (never returns ErrNotFound).
	GetActiveScheduler(ctx context.Context, scheduleName string) (*model.SchedulerTracker, error)
	// New: tx-accepting variants for atomic HTTP handler
	UpdateTx(ctx context.Context, tx *sqlx.Tx, tracker *model.SchedulerTracker) error
	UpdateInitializationStatusTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, status string) error
	CreateTx(ctx context.Context, tx *sqlx.Tx, tracker *model.SchedulerTracker) error
	// Task count helpers for event-driven run finalization
	IncrementTerminalCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID) (terminal, total int32, err error)
	DecrementTerminalCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, n int32) error
	SetTotalTaskCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, total int32) error
	// UpdateStatusTx updates the status column within an existing transaction.
	UpdateStatusTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, status string) error
	// FinalizeRunTx sets status and completed_at = NOW() for terminal-state transitions.
	FinalizeRunTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, status string) error
	// GetByIDForUpdateTx retrieves and row-locks the scheduler_tracker row for the given id
	// using SELECT ... FOR UPDATE. Must be called within a transaction.
	GetByIDForUpdateTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID) (*model.SchedulerTracker, error)
}

// LastRunData holds the summary of the most recent run for a schedule.
type LastRunData struct {
	ScheduleName string
	ScheduleID   uuid.UUID
	Status       model.SchedulerStatus
	CreatedAt    time.Time
	IsRunning    bool
}

// SchedulerFilters defines query filters for List operation
type SchedulerFilters struct {
	Status       *model.SchedulerStatus
	ScheduleName *string
	Limit        int
	Offset       int
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
func (r *schedulerTrackerRepository) Create(ctx context.Context, tracker *model.SchedulerTracker) error {
	versionsJSON, err := json.Marshal(tracker.GetManifestVersions())
	if err != nil {
		return fmt.Errorf("marshal manifest_versions: %w", err)
	}

	query := `
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			started_at, completed_at, last_heartbeat_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, manifest_versions,
			total_task_count, terminal_task_count
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7,
			$8, $9, $10,
			$11, $12,
			$13, $14
		)
	`

	_, err = r.db.ExecContext(ctx, query,
		tracker.ScheduleID, tracker.ScheduleName, tracker.Status, tracker.CreatedAt,
		tracker.StartedAt, tracker.CompletedAt, tracker.LastHeartbeatAt,
		tracker.CancelledAt, tracker.CancelledBy, tracker.CancellationReason,
		tracker.InitializationStatus, versionsJSON,
		tracker.TotalTaskCount, tracker.TerminalTaskCount,
	)
	if err != nil {
		// Check for duplicate key error (PostgreSQL error code 23505)
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505") {
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
func (r *schedulerTrackerRepository) CreateTx(ctx context.Context, tx *sqlx.Tx, tracker *model.SchedulerTracker) error {
	versionsJSON, err := json.Marshal(tracker.GetManifestVersions())
	if err != nil {
		return fmt.Errorf("marshal manifest_versions: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO scheduler_tracker (
			schedule_id, schedule_name, status, created_at,
			started_at, completed_at, last_heartbeat_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, manifest_versions,
			total_task_count, terminal_task_count
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7,
			$8, $9, $10,
			$11, $12,
			$13, $14
		)
	`,
		tracker.ScheduleID, tracker.ScheduleName, tracker.Status, tracker.CreatedAt,
		tracker.StartedAt, tracker.CompletedAt, tracker.LastHeartbeatAt,
		tracker.CancelledAt, tracker.CancelledBy, tracker.CancellationReason,
		tracker.InitializationStatus, versionsJSON,
		tracker.TotalTaskCount, tracker.TerminalTaskCount,
	)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "23505") {
			return ErrDuplicateKey
		}
		return fmt.Errorf("failed to create scheduler_tracker: %w", err)
	}
	return nil
}

// GetByID retrieves a scheduler_tracker by schedule_id
func (r *schedulerTrackerRepository) GetByID(ctx context.Context, scheduleID uuid.UUID) (*model.SchedulerTracker, error) {
	query := `
		SELECT
			schedule_id, schedule_name, status, created_at,
			started_at, completed_at, last_heartbeat_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, manifest_versions,
			total_task_count, terminal_task_count
		FROM scheduler_tracker
		WHERE schedule_id = $1
	`

	var tracker model.SchedulerTracker
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
	tracker.ManifestVersions = tracker.GetManifestVersions()

	r.logger.Debug("Retrieved scheduler_tracker",
		"schedule_id", scheduleID,
		"schedule_name", tracker.ScheduleName,
		"status", tracker.Status,
	)

	return &tracker, nil
}

// Update updates scheduler status and timestamps
func (r *schedulerTrackerRepository) Update(ctx context.Context, tracker *model.SchedulerTracker) error {
	query := `
		UPDATE scheduler_tracker
		SET status = :status,
			started_at = :started_at,
			completed_at = :completed_at,
			last_heartbeat_at = :last_heartbeat_at
		WHERE schedule_id = :schedule_id
	`

	result, err := r.db.NamedExecContext(ctx, query, tracker)
	if err != nil {
		r.logger.Error("Failed to update scheduler_tracker",
			"schedule_id", tracker.ScheduleID,
			"error", err,
		)
		return fmt.Errorf("failed to update scheduler_tracker: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Debug("Scheduler tracker not found for update",
			"schedule_id", tracker.ScheduleID,
		)
		return ErrNotFound
	}

	r.logger.Info("Updated scheduler_tracker",
		"schedule_id", tracker.ScheduleID,
		"status", tracker.Status,
	)

	return nil
}

// Cancel marks a scheduler as cancelled with audit information
func (r *schedulerTrackerRepository) Cancel(ctx context.Context, scheduleID uuid.UUID, cancelledBy, reason string) error {
	query := `
		UPDATE scheduler_tracker
		SET status = $1,
			cancelled_at = $2,
			cancelled_by = $3,
			cancellation_reason = $4
		WHERE schedule_id = $5
		  AND status NOT IN ('succeeded', 'failed', 'cancelled')
	`

	result, err := r.db.ExecContext(ctx, query,
		model.SchedulerStatusCancelled,
		time.Now(),
		cancelledBy,
		reason,
		scheduleID,
	)
	if err != nil {
		r.logger.Error("Failed to cancel scheduler",
			"schedule_id", scheduleID,
			"error", err,
		)
		return fmt.Errorf("failed to cancel scheduler: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("Scheduler not found or already in terminal state",
			"schedule_id", scheduleID,
		)
		return ErrNotCancellable
	}

	r.logger.Info("Cancelled scheduler",
		"schedule_id", scheduleID,
		"cancelled_by", cancelledBy,
	)

	return nil
}

// CancelTx cancels a scheduler within an existing transaction.
func (r *schedulerTrackerRepository) CancelTx(ctx context.Context, tx *sqlx.Tx, scheduleID uuid.UUID, cancelledBy, reason string) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE scheduler_tracker
		SET status              = $1,
		    cancelled_at        = $2,
		    cancelled_by        = $3,
		    cancellation_reason = $4
		WHERE schedule_id = $5
		  AND status NOT IN ('succeeded', 'failed', 'cancelled')
	`, model.SchedulerStatusCancelled, time.Now(), cancelledBy, reason, scheduleID)
	if err != nil {
		return fmt.Errorf("cancel scheduler tx: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrNotCancellable
	}
	return nil
}

// List queries schedulers with filters and pagination
func (r *schedulerTrackerRepository) List(ctx context.Context, filters SchedulerFilters) ([]*model.SchedulerTracker, int, error) {
	// Build dynamic WHERE clause
	whereClauses := []string{}
	args := map[string]interface{}{}

	if filters.Status != nil {
		whereClauses = append(whereClauses, "status = :status")
		args["status"] = *filters.Status
	}

	if filters.ScheduleName != nil {
		whereClauses = append(whereClauses, "schedule_name ILIKE :schedule_name")
		args["schedule_name"] = "%" + *filters.ScheduleName + "%"
	}

	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Count total matching records
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM scheduler_tracker %s", whereClause)
	var total int

	if len(args) > 0 {
		countStmt, err := r.db.PrepareNamedContext(ctx, countQuery)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to prepare count query: %w", err)
		}
		defer countStmt.Close()

		if err := countStmt.GetContext(ctx, &total, args); err != nil {
			return nil, 0, fmt.Errorf("failed to count schedulers: %w", err)
		}
	} else {
		if err := r.db.GetContext(ctx, &total, countQuery); err != nil {
			return nil, 0, fmt.Errorf("failed to count schedulers: %w", err)
		}
	}

	// Fetch paginated results
	args["limit"] = filters.Limit
	args["offset"] = filters.Offset

	query := fmt.Sprintf(`
		SELECT schedule_id, schedule_name, status, created_at, started_at,
		       completed_at, last_heartbeat_at, cancelled_at, cancelled_by,
		       cancellation_reason, initialization_status, manifest_versions,
		       total_task_count, terminal_task_count
		FROM scheduler_tracker
		%s
		ORDER BY created_at DESC
		LIMIT :limit OFFSET :offset
	`, whereClause)

	stmt, err := r.db.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to prepare query: %w", err)
	}
	defer stmt.Close()

	var trackers []*model.SchedulerTracker
	if err := stmt.SelectContext(ctx, &trackers, args); err != nil {
		r.logger.Error("Failed to list schedulers", "error", err)
		return nil, 0, fmt.Errorf("failed to list schedulers: %w", err)
	}
	for _, tracker := range trackers {
		tracker.ManifestVersions = tracker.GetManifestVersions()
	}

	r.logger.Debug("Listed schedulers",
		"count", len(trackers),
		"total", total,
		"filters", filters,
	)

	return trackers, total, nil
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

// UpdateInitializationStatus updates the initialization_status for a specific schedule
func (r *schedulerTrackerRepository) UpdateInitializationStatus(ctx context.Context, scheduleID uuid.UUID, status string) error {
	query := `
		UPDATE scheduler_tracker
		SET initialization_status = $1
		WHERE schedule_id = $2
	`

	result, err := r.db.ExecContext(ctx, query, status, scheduleID)
	if err != nil {
		r.logger.Error("Failed to update initialization_status",
			"schedule_id", scheduleID,
			"status", status,
			"error", err,
		)
		return fmt.Errorf("failed to update initialization_status: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Debug("Scheduler tracker not found for initialization_status update",
			"schedule_id", scheduleID,
		)
		return ErrNotFound
	}

	r.logger.Info("Updated initialization_status",
		"schedule_id", scheduleID,
		"status", status,
	)

	return nil
}

// ResetInProgressInitializations resets all 'in_progress' initialization statuses to 'pending'
// This is used for crash recovery on service startup
func (r *schedulerTrackerRepository) ResetInProgressInitializations(ctx context.Context) (int, error) {
	query := `
		UPDATE scheduler_tracker
		SET initialization_status = 'pending'
		WHERE initialization_status = 'in_progress'
	`

	result, err := r.db.ExecContext(ctx, query)
	if err != nil {
		r.logger.Error("Failed to reset in_progress initializations", "error", err)
		return 0, fmt.Errorf("failed to reset in_progress initializations: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		r.logger.Warn("Reset in_progress initializations",
			"count", rows,
			"reason", "service startup recovery",
		)
	}

	return int(rows), nil
}

// GetActiveScheduler returns the most recently created PENDING or RUNNING run for the given schedule name.
// Returns nil, nil when no active run exists — never returns ErrNotFound.
func (r *schedulerTrackerRepository) GetActiveScheduler(ctx context.Context, scheduleName string) (*model.SchedulerTracker, error) {
	query := `
		SELECT
			schedule_id, schedule_name, status, created_at,
			started_at, completed_at, last_heartbeat_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, manifest_versions,
			total_task_count, terminal_task_count
		FROM scheduler_tracker
		WHERE schedule_name = $1
		  AND status IN ('pending', 'running')
		ORDER BY created_at DESC
		LIMIT 1
	`

	var tracker model.SchedulerTracker
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
	tracker.ManifestVersions = tracker.GetManifestVersions()

	r.logger.Debug("Found active scheduler",
		"schedule_name", scheduleName,
		"schedule_id", tracker.ScheduleID,
		"status", tracker.Status,
	)

	return &tracker, nil
}

// UpdateTx updates scheduler status and timestamps within an existing transaction.
func (r *schedulerTrackerRepository) UpdateTx(ctx context.Context, tx *sqlx.Tx, tracker *model.SchedulerTracker) error {
	query := `
		UPDATE scheduler_tracker
		SET status = :status,
			started_at = :started_at,
			completed_at = :completed_at,
			last_heartbeat_at = :last_heartbeat_at
		WHERE schedule_id = :schedule_id
	`
	result, err := tx.NamedExecContext(ctx, query, tracker)
	if err != nil {
		return fmt.Errorf("failed to update scheduler_tracker: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrNotFound
	}
	return nil
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

// IncrementTerminalCountTx atomically increments terminal_task_count and returns
// the new terminal count and the current total_task_count (-1 if NULL).
func (r *schedulerTrackerRepository) IncrementTerminalCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID) (terminal, total int32, err error) {
	err = tx.QueryRowxContext(ctx, `
		UPDATE scheduler_tracker
		SET terminal_task_count = terminal_task_count + 1
		WHERE schedule_id = $1
		RETURNING terminal_task_count, COALESCE(total_task_count, -1)
	`, id).Scan(&terminal, &total)
	if errors.Is(err, sql.ErrNoRows) {
		err = fmt.Errorf("scheduler_tracker row not found for id %s: %w", id, ErrNotFound)
	}
	return
}

// DecrementTerminalCountTx decrements terminal_task_count by n, floor 0.
func (r *schedulerTrackerRepository) DecrementTerminalCountTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID, n int32) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE scheduler_tracker
		SET terminal_task_count = GREATEST(terminal_task_count - $2, 0)
		WHERE schedule_id = $1
	`, id, n)
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

// GetByIDForUpdateTx retrieves and row-locks the scheduler_tracker row using SELECT ... FOR UPDATE.
// Returns ErrNotFound when no row exists for the given id.
func (r *schedulerTrackerRepository) GetByIDForUpdateTx(ctx context.Context, tx *sqlx.Tx, id uuid.UUID) (*model.SchedulerTracker, error) {
	var tracker model.SchedulerTracker
	err := tx.QueryRowxContext(ctx, `
		SELECT
			schedule_id, schedule_name, status, created_at,
			started_at, completed_at, last_heartbeat_at,
			cancelled_at, cancelled_by, cancellation_reason,
			initialization_status, manifest_versions,
			total_task_count, terminal_task_count
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
	tracker.ManifestVersions = tracker.GetManifestVersions()
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
	defer rows.Close()

	result := make(map[string]LastRunData)
	for rows.Next() {
		var d LastRunData
		var statusStr string
		if err := rows.Scan(&d.ScheduleName, &d.ScheduleID, &statusStr, &d.CreatedAt, &d.IsRunning); err != nil {
			return nil, fmt.Errorf("scan last run row: %w", err)
		}
		d.Status = model.SchedulerStatus(statusStr)
		result[d.ScheduleName] = d
	}
	return result, rows.Err()
}
