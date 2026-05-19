package messageprocessing

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
)

// ExtractOutboxEntryID parses the "outbox_entry_id" field from a Redis Stream
// message's fields map. Returns nil when the field is absent, empty, or
// unparseable — callers should treat that as "no upstream outbox row" and
// fall back to plain (message_id, stream_name) dedup.
//
// All pkg/outbox.Processor-emitted XADDs include outbox_entry_id by
// convention; non-outbox sources (HTTP triggers, scheduler ticks) do not.
func ExtractOutboxEntryID(fields map[string]interface{}) *uuid.UUID {
	raw, ok := fields["outbox_entry_id"]
	if !ok {
		return nil
	}
	s, ok := raw.(string)
	if !ok || s == "" {
		return nil
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return nil
	}
	return &id
}

// Dedup inserts a message_processing row for messageID/streamName if one does
// not already exist. Returns (id, dup, err) where dup=true means the message
// has already been seen; callers should ACK and skip the work.
//
// Duplicate disposition:
//   - State is "completed" or "acked" → Info ("Message already processed, skipping").
//   - Any other state (typically "processing") → Warn ("Message being processed
//     by another instance"). Distinguishing the two helps operators spot
//     concurrent-consumer races vs. legitimate replays.
//
// Use DedupWithOutboxEntryID instead when the inbound message originated from
// pkg/outbox — it adds a second uniqueness key that catches Processor-crash
// redeliveries (same outbox row republished with a fresh Redis message_id).
func Dedup(
	ctx context.Context,
	repo Repository,
	logger *slog.Logger,
	messageID string,
	streamName string,
	payload []byte,
) (uuid.UUID, bool, error) {
	return DedupWithOutboxEntryID(ctx, repo, logger, messageID, streamName, payload, nil)
}

// DedupWithOutboxEntryID is Dedup with an additional secondary uniqueness key:
// the upstream outbox row's UUID, when the inbound message carries one. This
// closes the correctness window in pkg/outbox.Processor where a crash between
// XADD and MarkProcessed causes the same outbox row to be republished with a
// fresh Redis message_id. The primary (message_id, stream_name) dedup would
// miss that redelivery; the outbox_entry_id key catches it.
//
// outboxEntryID may be nil for messages with no upstream outbox row (HTTP
// triggers, scheduler ticks, etc.), in which case the behavior is identical
// to Dedup.
func DedupWithOutboxEntryID(
	ctx context.Context,
	repo Repository,
	logger *slog.Logger,
	messageID string,
	streamName string,
	payload []byte,
	outboxEntryID *uuid.UUID,
) (uuid.UUID, bool, error) {
	msg := &MessageProcessing{
		MessageID:     messageID,
		StreamName:    streamName,
		State:         "processing",
		Payload:       payload,
		OutboxEntryID: outboxEntryID,
	}
	id, inserted, err := repo.InsertIfNotExists(ctx, msg)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("insert message processing: %w", err)
	}
	if inserted {
		return id, false, nil
	}
	// On conflict, look up the existing row by ID (not by composite key — the
	// conflicting row may have a different message_id if the dedup hit was on
	// outbox_entry_id).
	existing, err := repo.GetByID(ctx, id)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("get existing message after conflict: %w", err)
	}
	logArgs := []any{"message_id", messageID, "stream", streamName, "existing_id", existing.ID, "state", existing.State}
	if outboxEntryID != nil {
		logArgs = append(logArgs, "outbox_entry_id", outboxEntryID.String())
	}
	if existing.State == "completed" || existing.State == "acked" {
		logger.Info("Message already processed, skipping", logArgs...)
		return existing.ID, true, nil
	}
	logger.Warn("Message being processed by another instance", logArgs...)
	return existing.ID, true, nil
}
