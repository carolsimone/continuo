package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/event"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	"github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// ParsePRClosed decodes a remediation.pr_closed:v1 XMessage's payload into the
// case-base DTO. Structural errors are events.ErrPermanent — the consumer ACKs
// and drops the poison message.
func ParsePRClosed(msg goredis.XMessage) (event.PRClosed, error) {
	raw, ok := msg.Values["payload"].(string)
	if !ok || raw == "" {
		return event.PRClosed{}, fmt.Errorf("%w: missing or empty payload field in message %s", events.ErrPermanent, msg.ID)
	}
	var evt event.PRClosed
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		return event.PRClosed{}, fmt.Errorf("%w: unmarshal remediation.pr_closed payload (message %s): %v", events.ErrPermanent, msg.ID, err)
	}
	return evt, nil
}

// NewPrClosedBinding wires ParsePRClosed into the provenance handler, threading
// outbox_entry_id for dedup.
func NewPrClosedBinding(
	handler *handlers.PrClosedProvenanceHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParsePRClosed(msg)
		if err != nil {
			logger.Error("remediation.pr_closed (provenance): parse failure — discarding",
				"message_id", msg.ID, "error", err)
			return err
		}
		outboxEntryID := messageprocessing.ExtractOutboxEntryID(msg.Values)
		return handler.Handle(ctx, msg.ID, outboxEntryID, evt)
	}
}
