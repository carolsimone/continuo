package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
)

// derivedRunConfig captures everything that distinguishes a rerun from a
// rebase. Both triggers run the identical dedup → snapshot → DispatchDerivedRun
// pipeline; only these three values differ, so they are baked into the handler
// at construction time rather than copied across two near-identical handlers.
type derivedRunConfig struct {
	// kind is the label threaded through Snapshot and DispatchDerivedRun
	// ("rerun" | "rebase") and surfaced in log output.
	kind string
	// streamName is the Redis stream the trigger arrives on, used as the
	// dedup key namespace.
	streamName string
	// selector materialises the projection for this kind: SourcePinnedDAG for
	// rerun, RebasePartition for rebase.
	selector snapshot.Selector
}

// DerivedRunHandler consumes a derived-run trigger (trigger.rerun:v1 or
// trigger.rebase:v1) and turns it into run.entries.dispatched:v1 +
// N × query.model:v1 via DispatchDerivedRun. The only thing that varies between
// the two triggers is the derivedRunConfig the handler is constructed with.
type DerivedRunHandler struct {
	uow         uow.UnitOfWork
	snapshotSvc SnapshotService
	logger      *slog.Logger
	cfg         derivedRunConfig
}

// NewHandleRerunHandler builds a DerivedRunHandler wired for trigger.rerun:v1:
// the SourcePinnedDAG selector reproduces the source run's pinned DAG.
func NewHandleRerunHandler(u uow.UnitOfWork, snapshotSvc SnapshotService, logger *slog.Logger) *DerivedRunHandler {
	return &DerivedRunHandler{
		uow:         u,
		snapshotSvc: snapshotSvc,
		logger:      logger,
		cfg: derivedRunConfig{
			kind:       "rerun",
			streamName: streams.TriggerRerunV1,
			selector:   snapshot.SourcePinnedDAG{},
		},
	}
}

// NewHandleRebaseHandler builds a DerivedRunHandler wired for trigger.rebase:v1:
// the RebasePartition selector reads the source's :EXECUTES set against the
// latest topology.
func NewHandleRebaseHandler(u uow.UnitOfWork, snapshotSvc SnapshotService, logger *slog.Logger) *DerivedRunHandler {
	return &DerivedRunHandler{
		uow:         u,
		snapshotSvc: snapshotSvc,
		logger:      logger,
		cfg: derivedRunConfig{
			kind:       "rebase",
			streamName: streams.TriggerRebaseV1,
			selector:   snapshot.RebasePartition{},
		},
	}
}

// Handle runs the shared derived-run pipeline: marshal+dedup, snapshot via the
// configured selector, then DispatchDerivedRun. A snapshot sentinel that maps
// to a dispatch-failed reason emits run.entries.dispatch_failed:v1 and completes
// the message; any other snapshot error propagates for the consumer to classify.
func (h *DerivedRunHandler) Handle(ctx context.Context, cmd domainModel.DerivedRunInput, messageID string, outboxEntryID *uuid.UUID) error {
	h.logger.Info("Processing derived run", "kind", h.cfg.kind, "message_id", messageID, "run_id", cmd.RunID, "source_run_id", cmd.SourceRunID)

	cmdPayload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}

	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	msgProcessingID, shouldSkip, err := messageprocessing.DedupWithOutboxEntryID(ctx, h.uow.MessageProcessingRepo(), h.logger, messageID, h.cfg.streamName, cmdPayload, outboxEntryID)
	if err != nil {
		return fmt.Errorf("dedup: %w", err)
	}
	if shouldSkip {
		return nil
	}

	sourceRunUUID, err := uuid.Parse(cmd.SourceRunID)
	if err != nil {
		return fmt.Errorf("invalid source_run_id %q: %w", cmd.SourceRunID, err)
	}

	projection, snapErr := h.snapshotSvc.Snapshot(ctx, snapshot.Params{
		RunID:        cmd.RunID,
		ScheduleName: cmd.ScheduleName,
		Kind:         h.cfg.kind,
		SourceRunID:  &sourceRunUUID,
		InitiatedBy:  cmd.InitiatedBy,
		Selector:     h.cfg.selector,
	})
	if snapErr != nil {
		if reason, ok := dispatchFailedReason(snapErr); ok {
			if ferr := EmitDispatchFailed(ctx, h.uow, h.logger, DispatchFailedParams{
				RunID:               cmd.RunID,
				ScheduleName:        cmd.ScheduleName,
				MessageProcessingID: msgProcessingID,
				Reason:              reason,
			}); ferr != nil {
				return ferr
			}
			if cerr := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); cerr != nil {
				return fmt.Errorf("mark completed: %w", cerr)
			}
			return h.uow.Commit()
		}
		return fmt.Errorf("snapshot %s: %w", h.cfg.kind, snapErr)
	}

	if err := DispatchDerivedRun(ctx, h.uow, h.logger, DerivedRunDispatch{
		RunID:               cmd.RunID,
		ScheduleName:        cmd.ScheduleName,
		Kind:                h.cfg.kind,
		MessageProcessingID: msgProcessingID,
		Projection:          projection,
	}); err != nil {
		return err
	}

	if err := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}
	return h.uow.Commit()
}
