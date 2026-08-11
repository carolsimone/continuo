package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/carolsimone/continuo/state/service/uow"
	"github.com/google/uuid"
)

// promotedSeedsNamespace seeds the deterministic run id derived from a release
// id.
//
// IMMUTABLE: changing this value re-keys every run id derived from a release,
// so a promotion redelivered across the change would mint a second run and
// rebuild seeds that are already built.
var promotedSeedsNamespace = uuid.MustParse("3e7b1a2f-8c4d-4e9a-b5f0-1d2c3a4e5f60")

// PromotedSeedsHandler processes release.promoted:v1 and creates the run that
// materialises the release's changed seeds into the production schema.
//
// It creates the run and announces it; orchestrator projects the tasks onto it
// from the announcement. That split is the same one every other run uses — cron,
// single-node, rerun and rebase all have state mint the run first — and it is
// what puts this work inside the standard lifecycle, where a failed seed build
// is retried, recorded, and visible.
type PromotedSeedsHandler struct {
	logger *slog.Logger
}

// NewPromotedSeedsHandler constructs the handler.
func NewPromotedSeedsHandler(logger *slog.Logger) *PromotedSeedsHandler {
	return &PromotedSeedsHandler{logger: logger}
}

// Handle creates one run per promoted release that changed at least one seed.
//
// Caller contract: u.Begin(ctx) has been called; the handler MUST NOT commit.
// Returning nil tells the binding to commit; returning an error triggers
// rollback.
//
// A promotion that changed no seeds is a no-op: no run, no event. Creating an
// empty run would surface a task-less run on every release that touched only
// models, and it could never reach a terminal state on its own.
func (h *PromotedSeedsHandler) Handle(
	ctx context.Context,
	u uow.UnitOfWork,
	evt events.ReleasePromoted,
	msgProcID uuid.UUID,
) error {
	seeds := evt.ChangedSeeds()
	if len(seeds) == 0 {
		h.logger.Info("release.promoted: no changed seeds — no run created",
			"release_id", evt.ReleaseID,
		)
		return nil
	}

	nodes := make([]run.NodeID, 0, len(seeds))
	for _, s := range seeds {
		nodes = append(nodes, run.NodeID{
			ServiceName: s.ServiceName,
			SchemaName:  s.SchemaName,
			TableName:   s.TableName,
		})
	}

	runID := PromotedSeedsRunID(evt.ReleaseID)
	newRun, domainEvt, err := run.NewPromotedSeedsRun(
		runID,
		promotedSeedsScheduleName(runID),
		evt.ReleaseID,
		nodes,
		u.Clock().Now(),
	)
	if err != nil {
		return fmt.Errorf("new promoted-seeds run: %w", err)
	}

	if err := u.Run().SaveRun(ctx, newRun); err != nil {
		return fmt.Errorf("save run: %w", err)
	}
	if err := u.Outbox().Append(ctx, []run.DomainEvent{domainEvt}, msgProcID); err != nil {
		return fmt.Errorf("append outbox: %w", err)
	}

	h.logger.Info("release.promoted: promoted-seeds run created",
		"release_id", evt.ReleaseID,
		"run_id", runID,
		"seed_count", len(nodes),
	)
	return nil
}

// PromotedSeedsRunID derives the run id for a release's promoted-seeds run.
//
// It is deterministic so that a redelivered release.promoted:v1 resolves to the
// run that already exists rather than minting a second one. SaveRun is an upsert
// on the run id, so the redelivery re-writes the same row instead of rebuilding
// seeds that are already built.
func PromotedSeedsRunID(releaseID string) uuid.UUID {
	return uuid.NewSHA1(promotedSeedsNamespace, []byte("schedule:"+releaseID))
}

// promotedSeedsScheduleName names the run in the UI. It follows the convention
// single-node runs use — a fixed prefix plus the head of the run id — because
// like those, this run has no schedule in the topology behind it.
func promotedSeedsScheduleName(runID uuid.UUID) string {
	return "promote-seed-" + strings.ReplaceAll(runID.String(), "-", "")[:8]
}
