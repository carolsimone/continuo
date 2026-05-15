package messageprocessing

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

// Dedup inserts a message_processing row for messageID/streamName if one does
// not already exist. Returns (id, dup, err) where dup=true means the message
// has already been seen; callers should ACK and skip the work.
//
// Duplicate disposition:
//   - State is "completed" or "acked" → Info ("Message already processed, skipping").
//   - Any other state (typically "processing") → Warn ("Message being processed
//     by another instance"). Distinguishing the two helps operators spot
//     concurrent-consumer races vs. legitimate replays.
func Dedup(
	ctx context.Context,
	repo Repository,
	logger *slog.Logger,
	messageID string,
	streamName string,
	payload []byte,
) (uuid.UUID, bool, error) {
	msg := &MessageProcessing{
		MessageID:  messageID,
		StreamName: streamName,
		State:      "processing",
		Payload:    payload,
	}
	id, inserted, err := repo.InsertIfNotExists(ctx, msg)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("insert message processing: %w", err)
	}
	if inserted {
		return id, false, nil
	}
	existing, err := repo.GetByMessageIDAndStream(ctx, messageID, streamName)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("get existing message: %w", err)
	}
	if existing.State == "completed" || existing.State == "acked" {
		logger.Info("Message already processed, skipping",
			"message_id", messageID, "state", existing.State)
		return existing.ID, true, nil
	}
	logger.Warn("Message being processed by another instance",
		"message_id", messageID)
	return existing.ID, true, nil
}
