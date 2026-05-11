package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/orchestrator/domain/run"
	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	"github.com/carolsimone/continuo/orchestrator/service/uow"
	"github.com/google/uuid"
)

// HandleRerunHandler consumes trigger.rerun:v1 messages. The pipeline is
// shared with HandleRebaseHandler via DispatchDerivedRun — both produce
// run.entries.dispatched:v1 + N × query.model:v1, differing only in the
// selector and the kind/stream labels.
type HandleRerunHandler struct {
	uow         uow.UnitOfWork
	runRepo     run.Repository
	snapshotSvc SnapshotService
	logger      *slog.Logger
}

func NewHandleRerunHandler(u uow.UnitOfWork, runRepo run.Repository, snapshotSvc SnapshotService, logger *slog.Logger) *HandleRerunHandler {
	return &HandleRerunHandler{uow: u, runRepo: runRepo, snapshotSvc: snapshotSvc, logger: logger}
}

func (h *HandleRerunHandler) Handle(ctx context.Context, cmd domainModel.RerunInput, messageID string) error {
	h.logger.Info("Processing rerun", "message_id", messageID, "run_id", cmd.RunID, "source_run_id", cmd.SourceRunID)

	cmdPayload, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}

	if err := h.uow.Begin(ctx); err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer h.uow.Rollback() //nolint:errcheck

	msgProcessingID, shouldSkip, err := dedupMessage(ctx, h.uow, h.logger, messageID, "trigger.rerun:v1", cmdPayload)
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
		Kind:         "rerun",
		SourceRunID:  &sourceRunUUID,
		Selector:     snapshot.SourcePinnedDAG{},
	})
	if snapErr != nil {
		if errors.Is(snapErr, snapshot.ErrEmptyProjection) {
			if ferr := EmitDispatchFailed(ctx, h.uow, h.logger, DispatchFailed{
				RunID: cmd.RunID, ScheduleName: cmd.ScheduleName,
				Reason:              "rerun_yielded_empty_projection",
				StreamName:          "run.entries.dispatch_failed:v1",
				EventType:           "run_entries_dispatch_failed",
				MessageProcessingID: msgProcessingID,
			}); ferr != nil {
				return ferr
			}
			if cerr := h.uow.MessageProcessingRepo().UpdateState(ctx, msgProcessingID, "completed"); cerr != nil {
				return fmt.Errorf("mark completed: %w", cerr)
			}
			return h.uow.Commit()
		}
		return fmt.Errorf("snapshot rerun: %w", snapErr)
	}

	if err := DispatchDerivedRun(ctx, h.uow, h.logger, DerivedRunDispatch{
		RunID: cmd.RunID, ScheduleName: cmd.ScheduleName, Kind: "rerun",
		StreamForFailed:     "run.entries.dispatch_failed:v1",
		EventTypeForFailed:  "run_entries_dispatch_failed",
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

// dedupMessage is a shared private helper used by both consumer handlers
// to avoid duplicating the InsertIfNotExists dance.
func dedupMessage(
	ctx context.Context,
	u uow.UnitOfWork,
	logger *slog.Logger,
	messageID string,
	streamName string,
	payload []byte,
) (uuid.UUID, bool, error) {
	msgProc := &domain.MessageProcessing{
		MessageID:  messageID,
		StreamName: streamName,
		State:      "processing",
		Payload:    payload,
	}
	id, inserted, err := u.MessageProcessingRepo().InsertIfNotExists(ctx, msgProc)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("insert message processing: %w", err)
	}
	if !inserted {
		existing, err := u.MessageProcessingRepo().GetByMessageID(ctx, messageID)
		if err != nil {
			return uuid.Nil, false, fmt.Errorf("get existing message: %w", err)
		}
		if existing.State == "completed" || existing.State == "acked" {
			logger.Info("Message already processed, skipping", "message_id", messageID, "state", existing.State)
			return existing.ID, true, nil
		}
		logger.Warn("Message being processed by another instance", "message_id", messageID)
		return existing.ID, true, nil
	}
	return id, false, nil
}
