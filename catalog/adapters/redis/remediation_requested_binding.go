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

// ParseRemediationRequested decodes a remediation.requested:v3 entry into the
// tenant its envelope names and the case-base event. A malformed envelope, a
// payload that does not decode, another schema_version and an envelope without
// a tenant are events.ErrPermanent: the consumer dead-letters the entry, so
// nothing is written under no tenant.
func ParseRemediationRequested(msg goredis.XMessage) (string, event.RemediationRequested, error) {
	env, p, err := events.DecodeRemediationRequested(stringFields(msg.Values))
	if err != nil {
		return "", event.RemediationRequested{}, fmt.Errorf("%w: decode remediation.requested (message %s): %v",
			events.ErrPermanent, msg.ID, err)
	}
	if env.TenantID == "" {
		return "", event.RemediationRequested{}, fmt.Errorf("%w: remediation.requested message %s carries no tenant_id",
			events.ErrPermanent, msg.ID)
	}
	return env.TenantID, remediationRequestedEvent(p), nil
}

// remediationRequestedEvent maps the decoded trigger to the case-base event,
// keeping the fields the case base records and, per node, whether the
// classifier marked it unfixable.
func remediationRequestedEvent(p events.RemediationRequested) event.RemediationRequested {
	nodes := make([]event.RemediationRequestedNode, len(p.Nodes))
	for i, n := range p.Nodes {
		nodes[i] = event.RemediationRequestedNode{
			NodeID:         n.NodeID,
			Category:       n.Category,
			ErrorSignature: n.ErrorSignature,
			Reason:         n.Reason,
			ErrorExcerpt:   n.ErrorExcerpt,
			DBTLogURI:      n.DBTLogURI,
			Unfixable:      n.Unfixable != nil,
		}
	}
	return event.RemediationRequested{
		Source:        p.Source,
		ReleaseID:     p.ReleaseID,
		CodeBundleURI: p.CodeBundleURI,
		ClassifiedAt:  p.ClassifiedAt,
		Nodes:         nodes,
	}
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
		return handler.Handle(ctx, tenantID, pkgredis.DedupMessageID(ctx, msg), messageprocessing.ExtractOutboxEntryID(msg.Values), evt)
	}
}
