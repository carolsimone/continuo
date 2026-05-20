package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/ports"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// RunRepositoryAdapter implements ports.RunRepository by composing the
// existing tuned SchedulerTrackerRepository methods. SaveRun consults
// run.Run.Changes() to dispatch column-by-column.
type RunRepositoryAdapter struct {
	db        *sqlx.DB
	schedRepo SchedulerTrackerRepository
	taskRepo  TaskTrackerRepository
	logger    *slog.Logger
}

// NewRunRepository constructs the adapter.
func NewRunRepository(
	db *sqlx.DB,
	schedRepo SchedulerTrackerRepository,
	taskRepo TaskTrackerRepository,
	logger *slog.Logger,
) *RunRepositoryAdapter {
	return &RunRepositoryAdapter{
		db:        db,
		schedRepo: schedRepo,
		taskRepo:  taskRepo,
		logger:    logger,
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

// LoadRunForUpdate loads a Run inside an existing transaction with SELECT … FOR UPDATE.
func (r *RunRepositoryAdapter) LoadRunForUpdate(ctx context.Context, tx *sqlx.Tx, id uuid.UUID) (*run.Run, error) {
	tr, err := r.schedRepo.GetByIDForUpdateTx(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	return hydrateRun(tr), nil
}

// SaveRun persists every dirty field of rn. It consults rn.Changes() to
// dispatch to the correct tuned SQL method, then calls rn.ResetChanges().
func (r *RunRepositoryAdapter) SaveRun(ctx context.Context, tx *sqlx.Tx, rn *run.Run) error {
	ch := rn.Changes()

	if ch.IsCreated() {
		tr := dehydrateRun(rn)
		if err := r.schedRepo.CreateTx(ctx, tx, tr); err != nil {
			return fmt.Errorf("create scheduler_tracker: %w", err)
		}
		rn.ResetChanges()
		return nil
	}

	if ch.IsInitStatusDirty() {
		if err := r.schedRepo.UpdateInitializationStatusTx(ctx, tx, rn.ScheduleID(), string(rn.InitializationStatus())); err != nil {
			return fmt.Errorf("update init_status: %w", err)
		}
	}
	if ch.IsTotalTaskCountDirty() {
		total := rn.TotalTaskCount()
		if total.Valid {
			if err := r.schedRepo.SetTotalTaskCountTx(ctx, tx, rn.ScheduleID(), total.Int32); err != nil {
				return fmt.Errorf("set total: %w", err)
			}
		}
	}
	if ch.IsTerminalTaskCountDirty() {
		if err := r.schedRepo.SetTerminalTaskCountTx(ctx, tx, rn.ScheduleID(), rn.TerminalTaskCount()); err != nil {
			return fmt.Errorf("set terminal: %w", err)
		}
	}
	if ch.IsCancelDirty() {
		by := ""
		reason := ""
		if rn.CancelledBy() != nil {
			by = *rn.CancelledBy()
		}
		if rn.CancellationReason() != nil {
			reason = *rn.CancellationReason()
		}
		if err := r.schedRepo.CancelTx(ctx, tx, rn.ScheduleID(), by, reason); err != nil {
			if errors.Is(err, ErrNotCancellable) {
				return run.ErrAlreadyTerminal
			}
			return fmt.Errorf("cancel: %w", err)
		}
	} else if ch.IsCompletedDirty() {
		if err := r.schedRepo.FinalizeRunTx(ctx, tx, rn.ScheduleID(), string(rn.Status())); err != nil {
			return fmt.Errorf("finalize: %w", err)
		}
	} else if ch.IsStatusDirty() {
		if err := r.schedRepo.UpdateStatusTx(ctx, tx, rn.ScheduleID(), string(rn.Status())); err != nil {
			return fmt.Errorf("update status: %w", err)
		}
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
func (r *RunRepositoryAdapter) GetLastRunPerSchedule(ctx context.Context) (map[string]ports.LastRunSummary, error) {
	raw, err := r.schedRepo.GetLastRunPerSchedule(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]ports.LastRunSummary, len(raw))
	for name, d := range raw {
		out[name] = ports.LastRunSummary{
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
var _ ports.RunRepository = (*RunRepositoryAdapter)(nil)
