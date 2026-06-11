package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// RunRepositoryAdapter implements repository.RunRepository by composing the
// existing tuned SchedulerTrackerRepository methods. SaveRun consults
// run.Run.Changes() to dispatch column-by-column.
type RunRepositoryAdapter struct {
	tx        *sqlx.Tx
	schedRepo SchedulerTrackerRepository
}

// NewRunRepository constructs the adapter bound to tx.
// LoadRunForUpdate/SaveRun run inside tx (which may be nil outside a
// transaction, in which case those write methods return an error).
func NewRunRepository(
	tx *sqlx.Tx,
	schedRepo SchedulerTrackerRepository,
) *RunRepositoryAdapter {
	return &RunRepositoryAdapter{
		tx:        tx,
		schedRepo: schedRepo,
	}
}

// GetRun loads a Run snapshot for read-only use (no row lock).
func (r *RunRepositoryAdapter) GetRun(ctx context.Context, id uuid.UUID) (*run.Run, error) {
	tr, err := r.schedRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return hydrateRun(tr), nil
}

// LoadRunForUpdate loads a Run inside the bound transaction with SELECT … FOR UPDATE.
func (r *RunRepositoryAdapter) LoadRunForUpdate(ctx context.Context, id uuid.UUID) (*run.Run, error) {
	if r.tx == nil {
		return nil, fmt.Errorf("LoadRunForUpdate requires an active transaction")
	}
	tr, err := r.schedRepo.GetByIDForUpdateTx(ctx, r.tx, id)
	if err != nil {
		return nil, err
	}
	return hydrateRun(tr), nil
}

// SaveRun persists every dirty field of rn. It consults rn.Changes() to
// dispatch to the correct tuned SQL method, then calls rn.ResetChanges().
func (r *RunRepositoryAdapter) SaveRun(ctx context.Context, rn *run.Run) error {
	if r.tx == nil {
		return fmt.Errorf("SaveRun requires an active transaction")
	}
	ch := rn.Changes()

	if ch.IsCreated() {
		tr := dehydrateRun(rn)
		if err := r.schedRepo.CreateTx(ctx, r.tx, tr); err != nil {
			// The partial unique index uq_scheduler_tracker_active_per_schedule is
			// the DB-level backstop for the activation TOCTOU: a concurrent
			// activation that passed the HasActiveSchedule pre-check loses the
			// INSERT race here and gets the same domain error as the check-first path.
			if errors.Is(err, ErrActiveScheduleConflict) {
				return run.ErrScheduleHasActiveRun
			}
			return fmt.Errorf("create scheduler_tracker: %w", err)
		}
		rn.ResetChanges()
		return nil
	}

	// Cancellation is a guarded terminal transition (WHERE status NOT IN
	// terminal, mapped to ErrAlreadyTerminal). It writes cancelled_at,
	// completed_at, cancelled_by, cancellation_reason and status together, so it
	// stays on its own dedicated statement rather than the consolidated UPDATE.
	if ch.IsCancelDirty() {
		by := ""
		reason := ""
		if rn.CancelledBy() != nil {
			by = *rn.CancelledBy()
		}
		if rn.CancellationReason() != nil {
			reason = *rn.CancellationReason()
		}
		if err := r.schedRepo.CancelTx(ctx, r.tx, rn.ScheduleID(), by, reason); err != nil {
			if errors.Is(err, ErrNotCancellable) {
				return run.ErrAlreadyTerminal
			}
			return fmt.Errorf("cancel: %w", err)
		}
		rn.ResetChanges()
		return nil
	}

	// Every other dirty column is collected into a single UPDATE so SaveRun
	// issues one round trip for the run row it already holds FOR UPDATE.
	var fields RunRowUpdate
	if ch.IsInitStatusDirty() {
		v := string(rn.InitializationStatus())
		fields.InitializationStatus = &v
	}
	if ch.IsTotalTaskCountDirty() {
		if total := rn.TotalTaskCount(); total.Valid {
			v := total.Int32
			fields.TotalTaskCount = &v
		}
	}
	if ch.IsTerminalTaskCountDirty() {
		v := rn.TerminalTaskCount()
		fields.TerminalTaskCount = &v
	}
	if ch.IsStartedDirty() {
		if started := rn.StartedAt(); started != nil {
			fields.StartedAt = started
		}
	}
	if ch.IsStatusDirty() {
		v := string(rn.Status())
		fields.Status = &v
	}
	// A completed transition stamps completed_at = NOW() alongside the new status.
	if ch.IsCompletedDirty() {
		fields.CompletedAtNow = true
	}

	if err := r.schedRepo.UpdateRunRowTx(ctx, r.tx, rn.ScheduleID(), fields); err != nil {
		return fmt.Errorf("update run row: %w", err)
	}

	rn.ResetChanges()
	return nil
}

// HasActiveSchedule returns true when a PENDING or RUNNING Run exists for name.
func (r *RunRepositoryAdapter) HasActiveSchedule(ctx context.Context, name string) (bool, error) {
	return r.schedRepo.HasActiveSchedule(ctx, name)
}

// GetActiveScheduler returns the active Run for name, or nil when none exists.
func (r *RunRepositoryAdapter) GetActiveScheduler(ctx context.Context, name string) (*run.Run, error) {
	tr, err := r.schedRepo.GetActiveScheduler(ctx, name)
	if err != nil {
		return nil, err
	}
	if tr == nil {
		return nil, nil
	}
	return hydrateRun(tr), nil
}

// GetLastRunPerSchedule returns the most recent Run summary per schedule name.
func (r *RunRepositoryAdapter) GetLastRunPerSchedule(ctx context.Context) (map[string]repository.LastRunSummary, error) {
	raw, err := r.schedRepo.GetLastRunPerSchedule(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]repository.LastRunSummary, len(raw))
	for name, d := range raw {
		out[name] = repository.LastRunSummary{
			ScheduleName: d.ScheduleName,
			ScheduleID:   d.ScheduleID,
			Status:       d.Status,
			CreatedAt:    d.CreatedAt,
			IsRunning:    d.IsRunning,
		}
	}
	return out, nil
}

// hydrateRun translates a persisted SchedulerTracker into a Run aggregate.
func hydrateRun(tr *SchedulerTracker) *run.Run {
	meta := tr.GetServiceMetadata()
	return run.HydrateRun(
		tr.ScheduleID,
		tr.ScheduleName,
		tr.Status,
		run.InitStatus(tr.InitializationStatus),
		run.Kind(tr.Kind),
		tr.SourceRunID,
		tr.CreatedAt,
		tr.StartedAt, tr.CompletedAt, tr.LastHeartbeatAt, tr.CancelledAt,
		tr.CancelledBy, tr.CancellationReason,
		tr.TotalTaskCount,
		tr.TerminalTaskCount,
		meta,
	)
}

// dehydrateRun materialises a Run back into the SchedulerTracker shape that
// CreateTx accepts. Used by SaveRun on the created branch.
func dehydrateRun(r *run.Run) *SchedulerTracker {
	metaJSON, _ := json.Marshal(r.ServiceMetadata())
	return &SchedulerTracker{
		ScheduleID:           r.ScheduleID(),
		ScheduleName:         r.ScheduleName(),
		Status:               r.Status(),
		CreatedAt:            r.CreatedAt(),
		StartedAt:            r.StartedAt(),
		CompletedAt:          r.CompletedAt(),
		LastHeartbeatAt:      r.LastHeartbeatAt(),
		CancelledAt:          r.CancelledAt(),
		CancelledBy:          r.CancelledBy(),
		CancellationReason:   r.CancellationReason(),
		InitializationStatus: string(r.InitializationStatus()),
		ServiceMetadataRaw:   metaJSON,
		ServiceMetadata:      r.ServiceMetadata(),
		TotalTaskCount:       r.TotalTaskCount(),
		TerminalTaskCount:    r.TerminalTaskCount(),
		Kind:                 string(r.Kind()),
		SourceRunID:          r.SourceRunID(),
	}
}

// Compile-time interface assertion.
var _ repository.RunRepository = (*RunRepositoryAdapter)(nil)
