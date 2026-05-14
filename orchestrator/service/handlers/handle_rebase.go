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
	"github.com/google/uuid"
)

// HandleRebaseHandler consumes trigger.rebase:v1 messages. The pipeline is
// shared with HandleRerunHandler via DispatchDerivedRun. The only differences
// are the selector (RebasePartition reads latest topology) and the kind/stream
// labels.
type HandleRebaseHandler struct {
	uow         uow.UnitOfWork
	snapshotSvc SnapshotService
	logger      *slog.Logger
}

func NewHandleRebaseHandler(u uow.UnitOfWork, snapshotSvc SnapshotService, logger *slog.Logger) *HandleRebaseHandler {
	return &HandleRebaseHandler{uow: u, snapshotSvc: snapshotSvc, logger: logger}
}

func (h *HandleRebaseHandler) Handle(ctx context.Context, cmd domainModel.RebaseInput, messageID string) error {
	h.logger.Info("Processing rebase", "message_id", messageID, "run_id", cmd.RunID, "source_run_id", cmd.SourceRunID)

	cmdPayload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}

	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	msgProcessingID, shouldSkip, err := messageprocessing.Dedup(ctx, h.uow.MessageProcessingRepo(), h.logger, messageID, "trigger.rebase:v1", cmdPayload)
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
		Kind:         "rebase",
		SourceRunID:  &sourceRunUUID,
		Selector:     snapshot.RebasePartition{},
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
		return fmt.Errorf("snapshot rebase: %w", snapErr)
	}

	if err := DispatchDerivedRun(ctx, h.uow, h.logger, DerivedRunDispatch{
		RunID:               cmd.RunID,
		ScheduleName:        cmd.ScheduleName,
		Kind:                "rebase",
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
