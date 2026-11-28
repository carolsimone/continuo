package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/catalog/domain/event"
	"github.com/carolsimone/continuo/catalog/service/handlers"
	"github.com/carolsimone/continuo/pkg/events"
	messageprocessing "github.com/carolsimone/continuo/pkg/messageprocessing"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// ParseHealTriggered decodes a heal.triggered:v1 entry into the
// tenant its envelope names and the case-base event. A malformed envelope, a
// payload that does not decode, another schema_version and an envelope without
// a tenant are events.ErrPermanent: the consumer dead-letters the entry, so
// nothing is written under no tenant.
func ParseHealTriggered(msg goredis.XMessage) (string, event.HealTriggered, error) {
	env, p, err := events.DecodeHealTriggered(stringFields(msg.Values))
	if err != nil {
		return "", event.HealTriggered{}, fmt.Errorf("%w: decode heal.triggered (message %s): %v",
			events.ErrPermanent, msg.ID, err)
	}
	if env.TenantID == "" {
		return "", event.HealTriggered{}, fmt.Errorf("%w: heal.triggered message %s carries no tenant_id",
			events.ErrPermanent, msg.ID)
	}
	return env.TenantID, healTriggeredEvent(p), nil
}

// healTriggeredEvent maps the decoded trigger to the case-base event,
// keeping the fields the case base records and, per node, whether the
// classifier marked it unfixable.
func healTriggeredEvent(p events.HealTriggered) event.HealTriggered {
	nodes := make([]event.HealTriggeredNode, len(p.Nodes))
	for i, n := range p.Nodes {
		nodes[i] = event.HealTriggeredNode{
			NodeID:         n.NodeID,
			Category:       n.Category,
			ErrorSignature: n.ErrorSignature,
			Reason:         n.Reason,
			ErrorExcerpt:   n.ErrorExcerpt,
			DBTLogURI:      n.DBTLogURI,
			Unfixable:      n.Unfixable != nil,
		}
	}
	return event.HealTriggered{
		Source:        p.Source,
		ReleaseID:     p.ReleaseID,
		CodeBundleURI: p.CodeBundleURI,
		ClassifiedAt:  p.ClassifiedAt,
		Nodes:         nodes,
	}
}

// NewHealTriggeredBinding wires ParseHealTriggered into the
// failure precedents handler, which records the entry under its tenant. outbox_entry_id
// is extracted from the entry's fields and threaded to the handler, whose
// processed-message marker then also recognises a re-XADD of the same upstream
// outbox row under a fresh Redis message ID.
func NewHealTriggeredBinding(
	handler *handlers.HealTriggeredFailurePrecedentsHandler,
	logger *slog.Logger,
) pkgredis.MessageHandler {
	return func(ctx context.Context, msg goredis.XMessage) error {
		tenantID, evt, err := ParseHealTriggered(msg)
		if err != nil {
			logger.Error("heal.triggered (failure precedents): parse failure — dead-lettering",
				"message_id", msg.ID, "error", err)
			return err
		}
		return handler.Handle(ctx, tenantID, pkgredis.DedupMessageID(ctx, msg), messageprocessing.ExtractOutboxEntryID(msg.Values), evt)
	}
}
