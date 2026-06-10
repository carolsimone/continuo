package handlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/catalog"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// ScheduleCatalogHandler processes schedules.loaded:v1 events. Loads the
// ScheduleCatalog aggregate, invokes Reconcile (which enforces the
// empty-list guard), persists via SaveCatalog. The aggregate's
// ErrEmptyReconciliation is surfaced so the consumer NACKs instead of
// silently soft-deleting every active catalog row.
type ScheduleCatalogHandler struct {
	logger *slog.Logger
}

// NewScheduleCatalogHandler constructs a ScheduleCatalogHandler.
func NewScheduleCatalogHandler(logger *slog.Logger) *ScheduleCatalogHandler {
	return &ScheduleCatalogHandler{logger: logger}
}

// Handle orchestrates the catalog-loaded flow.
//
// Caller contract: u.Begin(ctx) has been called; the handler MUST NOT commit.
// Returning nil tells the binding to commit; returning an error triggers
// rollback. ErrEmptyReconciliation is returned wrapped, so the binding can
// distinguish "domain rejected the payload" from "transient infra error".
// (Today both produce a NACK; the binding may classify them differently later.)
func (h *ScheduleCatalogHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.ScheduleCatalogLoaded,
	_ uuid.UUID, // msgProcID — catalog events do not propagate provenance today
) error {
	c, err := u.Catalog().LoadCatalogForUpdate(ctx)
	if err != nil {
		return fmt.Errorf("load catalog: %w", err)
	}
	if err := c.Reconcile(evt.ScheduleNames, broadcastServiceMetadata(evt.ScheduleNames, evt.ServiceMetadata), u.Clock().Now()); err != nil {
		if errors.Is(err, catalog.ErrEmptyReconciliation) {
			h.logger.Error("schedules.loaded: empty schedule_names list — refusing to nuke catalog")
			return errors.Join(pkgevents.ErrPermanent, err)
		}
		return fmt.Errorf("reconcile: %w", err)
	}
	if err := u.Catalog().SaveCatalog(ctx, c); err != nil {
		return fmt.Errorf("save: %w", err)
	}
	h.logger.Info("Schedule catalog reconciled",
		"event_id", evt.EventID,
		"schedule_count", len(evt.ScheduleNames),
	)
	return nil
}

// broadcastServiceMetadata translates the wire-format metadata shape to the
// aggregate's typed map. The schedules.loaded:v1 wire format carries a global
// flat map of {serviceName -> metadata} (not per-schedule). We promote it by
// applying the same service metadata to every schedule name in the payload.
func broadcastServiceMetadata(names []string, global map[string]run.ServiceMetadata) map[string]map[string]run.ServiceMetadata {
	out := make(map[string]map[string]run.ServiceMetadata, len(names))
	for _, name := range names {
		m := make(map[string]run.ServiceMetadata, len(global))
		for svc, meta := range global {
			m[svc] = meta
		}
		out[name] = m
	}
	return out
}
