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

// ParsePROpened decodes a remediation.pr_opened:v1 XMessage's payload into
// the case-base DTO. Structural errors are events.ErrPermanent — the
// consumer ACKs and drops the poison message.
func ParsePROpened(msg goredis.XMessage) (event.PROpened, error) {
	raw, ok := msg.Values["payload"].(string)
	if !ok || raw == "" {
		return event.PROpened{}, fmt.Errorf("%w: missing or empty payload field in message %s", events.ErrPermanent, msg.ID)
	}
	var evt event.PROpened
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		return event.PROpened{}, fmt.Errorf("%w: unmarshal remediation.pr_opened payload (message %s): %v", events.ErrPermanent, msg.ID, err)
	}
	return evt, nil
}

// NewPrOpenedBinding wires ParsePROpened into the proposals handler,
// threading outbox_entry_id for dedup.
func NewPrOpenedBinding(
	handler *handlers.PrOpenedProposalsHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		evt, err := ParsePROpened(msg)
		if err != nil {
			logger.Error("remediation.pr_opened (proposals): parse failure — discarding",
				"message_id", msg.ID, "error", err)
			return err
		}
		outboxEntryID := messageprocessing.ExtractOutboxEntryID(msg.Values)
		return handler.Handle(ctx, msg.ID, outboxEntryID, evt)
	}
}
