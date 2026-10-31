package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/catalog/domain/event"
	"github.com/carolsimone/continuo/catalog/serialization"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// ParsePRClosed decodes a remediation.pr_closed:v1 entry into the tenant its
// tenant_id field names and the case-base event. A missing or undecodable
// payload, and an entry without a tenant, are events.ErrPermanent: the
// consumer dead-letters the entry, so nothing is written under no tenant.
func ParsePRClosed(msg goredis.XMessage) (string, event.PRClosed, error) {
	tenantID, err := tenantField(msg, "remediation.pr_closed")
	if err != nil {
		return "", event.PRClosed{}, err
	}
	raw, ok := msg.Values["payload"].(string)
	if !ok || raw == "" {
		return "", event.PRClosed{}, fmt.Errorf("%w: missing or empty payload field in message %s", events.ErrPermanent, msg.ID)
	}
	var dto serialization.PRClosedDTO
	if err := json.Unmarshal([]byte(raw), &dto); err != nil {
		return "", event.PRClosed{}, fmt.Errorf("%w: unmarshal remediation.pr_closed payload (message %s): %v", events.ErrPermanent, msg.ID, err)
	}
	return tenantID, dto.ToDomain(), nil
}

// NewPrClosedBinding wires ParsePRClosed into the provenance handler, which
// records the entry under its tenant. outbox_entry_id is extracted from the
// entry's fields and threaded to the handler, whose processed-message marker
// then also recognises a re-XADD of the same upstream outbox row under a fresh
// Redis message ID.
func NewPrClosedBinding(
	handler *handlers.PrClosedProvenanceHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		tenantID, evt, err := ParsePRClosed(msg)
		if err != nil {
			logger.Error("remediation.pr_closed (provenance): parse failure — dead-lettering",
				"message_id", msg.ID, "error", err)
			return err
		}
		return handler.Handle(ctx, tenantID, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values), evt)
	}
}
