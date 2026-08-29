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

// ParseRemediationRequested decodes a remediation.requested:v2 XMessage's
// payload into the case-base DTO. Structural errors are events.ErrPermanent —
// the consumer ACKs and drops the poison message.
func ParseRemediationRequested(msg goredis.XMessage) (event.RemediationRequested, error) {
	raw, ok := msg.Values["payload"].(string)
	if !ok || raw == "" {
		return event.RemediationRequested{}, fmt.Errorf("%w: missing or empty payload field in message %s", events.ErrPermanent, msg.ID)
	}
	var evt event.RemediationRequested
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		return event.RemediationRequested{}, fmt.Errorf("%w: unmarshal remediation.requested payload (message %s): %v", events.ErrPermanent, msg.ID, err)
	}
	return evt, nil
}

// NewRemediationRequestedBinding wires ParseRemediationRequested into the
// rejections handler, threading outbox_entry_id for dedup.
func NewRemediationRequestedBinding(
	handler *handlers.RemediationRequestedRejectionsHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParseRemediationRequested(msg)
		if err != nil {
			logger.Error("remediation.requested (rejections): parse failure — discarding",
				"message_id", msg.ID, "error", err)
			return err
		}
		outboxEntryID := messageprocessing.ExtractOutboxEntryID(msg.Values)
		return handler.Handle(ctx, msg.ID, outboxEntryID, evt)
	}
}
