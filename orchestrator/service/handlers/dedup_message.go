package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/service/uow"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/google/uuid"
)

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
	msgProc := &messageprocessing.MessageProcessing{
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
