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

// ParseRemediationRequested decodes a remediation.requested:v2 entry into the
// tenant its tenant_id field names and the case-base event. A missing or
// undecodable payload, and an entry without a tenant, are events.ErrPermanent:
// the consumer dead-letters the entry, so nothing is written under no tenant.
func ParseRemediationRequested(msg goredis.XMessage) (string, event.RemediationRequested, error) {
	tenantID, err := tenantField(msg, "remediation.requested")
	if err != nil {
		return "", event.RemediationRequested{}, err
	}
	raw, ok := msg.Values["payload"].(string)
	if !ok || raw == "" {
		return "", event.RemediationRequested{}, fmt.Errorf("%w: missing or empty payload field in message %s", events.ErrPermanent, msg.ID)
	}
	var dto serialization.RemediationRequestedDTO
	if err := json.Unmarshal([]byte(raw), &dto); err != nil {
		return "", event.RemediationRequested{}, fmt.Errorf("%w: unmarshal remediation.requested payload (message %s): %v", events.ErrPermanent, msg.ID, err)
	}
	return tenantID, dto.ToDomain(), nil
}

// NewRemediationRequestedBinding wires ParseRemediationRequested into the
// rejections handler, which records the entry under its tenant. outbox_entry_id
// is extracted from the entry's fields and threaded to the handler, whose
// processed-message marker then also recognises a re-XADD of the same upstream
// outbox row under a fresh Redis message ID.
func NewRemediationRequestedBinding(
	handler *handlers.RemediationRequestedRejectionsHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		tenantID, evt, err := ParseRemediationRequested(msg)
		if err != nil {
			logger.Error("remediation.requested (rejections): parse failure — dead-lettering",
				"message_id", msg.ID, "error", err)
			return err
		}
		return handler.Handle(ctx, tenantID, msg.ID, messageprocessing.ExtractOutboxEntryID(msg.Values), evt)
	}
}
