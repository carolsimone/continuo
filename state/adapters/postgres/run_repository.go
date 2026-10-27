package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// GetRun loads the tenant's Run snapshot for read-only use (no row lock).
func (r *RunRepository) GetRun(ctx context.Context, tenantID string, id uuid.UUID) (*run.Run, error) {
	tr, err := r.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	return hydrateRun(tr)
}

// LoadRunForUpdate loads the tenant's Run inside the bound transaction with
// SELECT … FOR UPDATE.
func (r *RunRepository) LoadRunForUpdate(ctx context.Context, tenantID string, id uuid.UUID) (*run.Run, error) {
	if r.tx == nil {
		return nil, fmt.Errorf("LoadRunForUpdate requires an active transaction")
	}
	tr, err := r.LoadByIDForUpdateTx(ctx, r.tx, tenantID, id)
	if err != nil {
		return nil, err
	}
	return hydrateRun(tr)
}

// SaveRun persists every dirty field of rn under rn's tenant. It consults
// rn.Changes() to dispatch to the correct tuned SQL method, then calls
// rn.ResetChanges().
func (r *RunRepository) SaveRun(ctx context.Context, rn *run.Run) error {
	if r.tx == nil {
		return fmt.Errorf("SaveRun requires an active transaction")
	}
	ch := rn.Changes()

	if ch.IsCreated() {
		tr, err := dehydrateRun(rn)
		if err != nil {
			return fmt.Errorf("dehydrate run: %w", err)
		}
		if err := r.CreateTx(ctx, r.tx, tr); err != nil {
			// The partial unique index uq_runs_active_per_schedule is
			// the DB-level backstop for the activation TOCTOU: a concurrent
			// activation that passed the HasActiveRun pre-check loses the
			// INSERT race here and gets the same domain error as the check-first path.
			if errors.Is(err, ErrActiveRunConflict) {
				return run.ErrScheduleHasActiveRun
			}
			if errors.Is(err, ErrRunExists) {
				return run.ErrRunAlreadyExists
			}
			return fmt.Errorf("create run: %w", err)
		}
		rn.ResetChanges()
		return nil
	}

	// A cancel is a guarded transition (WHERE status NOT IN terminal, mapped to
	// ErrAlreadyTerminal) to cancelling or cancelled. It writes the status,
	// cancelled_at, cancelled_by and cancellation_reason, and completed_at with
	// cancelled, on its own statement; the other columns the cancel moved (the
	// terminal count, the progress clock) follow in the consolidated UPDATE.
	if ch.IsCancelDirty() {
		by := ""
		reason := ""
		if rn.CancelledBy() != nil {
			by = *rn.CancelledBy()
		}
		if rn.CancellationReason() != nil {
			reason = *rn.CancellationReason()
		}
		// Run.Cancel always sets cancelled_at when it marks the change set
		// cancelDirty, so a nil here is a programming error rather than a
		// missing-value default to paper over.
		at := rn.CancelledAt()
		if at == nil {
			return fmt.Errorf("cancel save: aggregate marked cancel-dirty without a cancelled_at")
		}
		if err := r.CancelTx(ctx, r.tx, rn.TenantID(), rn.RunID(), rn.Status(), by, reason, *at); err != nil {
			if errors.Is(err, ErrRunNotCancellable) {
				return run.ErrAlreadyTerminal
			}
			return fmt.Errorf("cancel: %w", err)
		}
	}

	// Every other dirty column is collected into a single UPDATE so SaveRun
	// issues one round trip for the run row it already holds FOR UPDATE.
	var fields RunRowUpdate
	if ch.IsInitStatusDirty() {
		v := string(rn.InitializationStatus())
		fields.InitializationStatus = &v
	}
	if ch.IsTotalTaskCountDirty() {
		if total := rn.TotalTaskCount(); total != nil {
			v := *total
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
	if ch.IsHeartbeatDirty() {
		if heartbeat := rn.LastProgressAt(); heartbeat != nil {
			fields.LastProgressAt = heartbeat
		}
	}
	if ch.IsPlannedDirty() {
		fields.PlannedAt = rn.PlannedAt()
	}
	// The cancel's statement already wrote the status it moved.
	if ch.IsStatusDirty() && !ch.IsCancelDirty() {
		v := string(rn.Status())
		fields.Status = &v
	}
	// A completed transition stamps completed_at = NOW() alongside the new
	// status, and the reason a run failed without a failed task.
	if ch.IsCompletedDirty() {
		fields.CompletedAtNow = true
		if reason := rn.FailureReason(); reason != nil {
			v := string(*reason)
			fields.FailureReason = &v
		}
	}

	if err := r.UpdateRunRowTx(ctx, r.tx, rn.TenantID(), rn.RunID(), fields); err != nil {
		return fmt.Errorf("update run row: %w", err)
	}

	rn.ResetChanges()
	return nil
}

// GetActiveRun returns the tenant's active Run for name, or nil when none
// exists.
func (r *RunRepository) GetActiveRun(ctx context.Context, tenantID, name string) (*run.Run, error) {
	tr, err := r.getActiveRunRow(ctx, tenantID, name)
	if err != nil {
		return nil, err
	}
	if tr == nil {
		return nil, nil
	}
	return hydrateRun(tr)
}

// RunExists reports whether the tenant has a run row with id, reading inside
// the bound transaction.
func (r *RunRepository) RunExists(ctx context.Context, tenantID string, id uuid.UUID) (bool, error) {
	if r.tx == nil {
		return false, fmt.Errorf("RunExists requires an active transaction")
	}
	var exists bool
	if err := r.tx.GetContext(ctx, &exists,
		`SELECT EXISTS (SELECT 1 FROM runs WHERE tenant_id = $1 AND run_id = $2)`, tenantID, id); err != nil {
		return false, fmt.Errorf("check run %s exists: %w", id, err)
	}
	return exists, nil
}

// ListUnplannedActive reads the pending, running and cancelling run rows of
// every tenant whose planned_at is NULL, oldest first, inside the bound
// transaction.
func (r *RunRepository) ListUnplannedActive(ctx context.Context) ([]repository.RunRef, error) {
	if r.tx == nil {
		return nil, fmt.Errorf("ListUnplannedActive requires an active transaction")
	}
	var rows []runRefRow
	if err := r.tx.SelectContext(ctx, &rows, `
		SELECT tenant_id, run_id FROM runs
		WHERE status = ANY($1::text[]) AND planned_at IS NULL
		ORDER BY created_at, tenant_id, run_id`, pq.Array(activeRunStatuses())); err != nil {
		return nil, fmt.Errorf("list the active runs state did not plan: %w", err)
	}
	return runRefs(rows), nil
}

// CountReleaseRuns counts the tenant's run rows of kind that name releaseID,
// reading inside the bound transaction.
func (r *RunRepository) CountReleaseRuns(ctx context.Context, tenantID, releaseID string, kind run.Kind) (int, error) {
	if r.tx == nil {
		return 0, fmt.Errorf("CountReleaseRuns requires an active transaction")
	}
	var n int
	if err := r.tx.GetContext(ctx, &n,
		`SELECT count(*) FROM runs WHERE tenant_id = $1 AND release_id = $2 AND kind = $3`,
		tenantID, releaseID, string(kind)); err != nil {
		return 0, fmt.Errorf("count the %s runs of release %s: %w", kind, releaseID, err)
	}
	return n, nil
}

// hydrateRun translates a persisted RunRow into a Run aggregate.
func hydrateRun(tr *RunRow) (*run.Run, error) {
	meta, err := tr.GetServiceMetadata()
	if err != nil {
		return nil, err
	}
	rn := run.HydrateRun(
		tr.TenantID,
		tr.ScheduleID,
		tr.ScheduleName,
		tr.Status,
		run.InitStatus(tr.InitializationStatus),
		run.Kind(tr.Kind),
		tr.SourceRunID,
		tr.InitiatedBy,
		operationFromColumn(tr.Operation),
		tr.CreatedAt,
		tr.StartedAt, tr.CompletedAt, tr.LastProgressAt, tr.CancelledAt,
		tr.CancelledBy, tr.CancellationReason,
		nullInt32ToPtr(tr.TotalTaskCount),
		tr.TerminalTaskCount,
		meta,
	)
	rn.HydrateRelease(run.ReleaseStamp{ReleaseID: stringOrEmpty(tr.ReleaseID), PromotionSeq: int64OrZero(tr.PromotionSeq)})
	rn.HydratePlannedAt(tr.PlannedAt)
	if tr.FailureReason != nil {
		reason := run.RunFailureReason(*tr.FailureReason)
		rn.HydrateFailureReason(&reason)
	}
	return rn, nil
}

// optionalString maps "" to a NULL column value.
func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// stringOrEmpty maps a NULL column value to "".
func stringOrEmpty(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// optionalInt64 maps 0 to NULL, so a run that names no release stores no seq.
func optionalInt64(n int64) *int64 {
	if n == 0 {
		return nil
	}
	return &n
}

// int64OrZero reads a nullable bigint as 0 when NULL.
func int64OrZero(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// operationFromColumn maps the non-empty stored operation
// ('run'|'test'|'build'|'full_refresh') back to the domain model.Operation
// (empty string for run).
func operationFromColumn(s string) model.Operation {
	if s == "run" || s == "" {
		return model.OperationRun
	}
	return model.Operation(s)
}

// nullInt32ToPtr maps the storage-layer sql.NullInt32 to the domain's *int32:
// an invalid (NULL) value becomes nil.
func nullInt32ToPtr(n sql.NullInt32) *int32 {
	if !n.Valid {
		return nil
	}
	v := n.Int32
	return &v
}

// ptrToNullInt32 maps the domain's *int32 back to the storage-layer
// sql.NullInt32: nil becomes an invalid (NULL) value.
func ptrToNullInt32(p *int32) sql.NullInt32 {
	if p == nil {
		return sql.NullInt32{}
	}
	return sql.NullInt32{Int32: *p, Valid: true}
}

// dehydrateRun materialises a Run back into the RunRow shape that
// CreateTx accepts. Used by SaveRun on the created branch.
func dehydrateRun(r *run.Run) (*RunRow, error) {
	metaJSON, err := marshalServiceMetadata(r.ServiceMetadata())
	if err != nil {
		return nil, fmt.Errorf("marshal service_metadata: %w", err)
	}
	return &RunRow{
		TenantID:             r.TenantID(),
		ScheduleID:           r.RunID(),
		ScheduleName:         r.ScheduleName(),
		Status:               r.Status(),
		CreatedAt:            r.CreatedAt(),
		StartedAt:            r.StartedAt(),
		CompletedAt:          r.CompletedAt(),
		LastProgressAt:       r.LastProgressAt(),
		CancelledAt:          r.CancelledAt(),
		CancelledBy:          r.CancelledBy(),
		CancellationReason:   r.CancellationReason(),
		InitializationStatus: string(r.InitializationStatus()),
		ServiceMetadataRaw:   metaJSON,
		ServiceMetadata:      r.ServiceMetadata(),
		TotalTaskCount:       ptrToNullInt32(r.TotalTaskCount()),
		TerminalTaskCount:    r.TerminalTaskCount(),
		Kind:                 string(r.Kind()),
		SourceRunID:          r.SourceRunID(),
		InitiatedBy:          r.InitiatedBy(),
		Operation:            operationWithDefault(string(r.Operation())),
		ReleaseID:            optionalString(r.Release().ReleaseID),
		PromotionSeq:         optionalInt64(r.Release().PromotionSeq),
	}, nil
}

// Compile-time interface assertion.
var _ repository.RunRepository = (*RunRepository)(nil)
