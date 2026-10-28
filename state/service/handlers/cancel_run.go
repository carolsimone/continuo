package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// CancelRunHandler is the use case behind a cancel of a run: it ends the run's
// pending tasks, asks its in-flight attempts to stop and records the cancel's
// run.cancelled:v1 in the outbox, in the caller's transaction.
type CancelRunHandler struct {
	logger *slog.Logger
}

// NewCancelRunHandler constructs the cancel use case.
func NewCancelRunHandler(logger *slog.Logger) *CancelRunHandler {
	return &CancelRunHandler{logger: logger}
}

// CancelInTransaction cancels tenantID's run runID inside u's open
// transaction, which the caller commits or rolls back, and returns the run as
// the cancel left it: cancelling while an attempt is still being stopped,
// cancelled once nothing was in flight. A run that is already cancelling or
// cancelled is returned as it is, and nothing is written.
//
// A run the tenant does not have is repository.ErrRunNotFound; a run that
// ended succeeded, failed or skipped is run.ErrAlreadyTerminal.
func (h *CancelRunHandler) CancelInTransaction(ctx context.Context, u uow.UnitOfWork, tenantID string, runID uuid.UUID, by, reason string) (*run.Run, error) {
	r, err := u.Run().LoadRunForUpdate(ctx, tenantID, runID)
	if err != nil {
		return nil, fmt.Errorf("load the run: %w", err)
	}
	if r.Status() == run.RunStatusCancelled {
		return r, nil
	}
	events, err := r.Cancel(ctx, u.TaskCollection(r.TenantID()), by, reason, u.Clock().Now())
	if errors.Is(err, run.ErrAlreadyCancelling) {
		return r, nil
	}
	if err != nil {
		return nil, err
	}
	if err := u.Run().SaveRun(ctx, r); err != nil {
		return nil, fmt.Errorf("save: %w", err)
	}
	if err := u.Outbox().Append(ctx, events, uuid.Nil); err != nil {
		return nil, fmt.Errorf("outbox: %w", err)
	}
	u.AfterCommit(func() {
		h.logger.Info("Run cancelled", "tenant_id", r.TenantID(), "run_id", r.RunID(), "schedule_name", r.ScheduleName(),
			"status", string(r.Status()), "cancelled_by", by)
	})
	return r, nil
}
