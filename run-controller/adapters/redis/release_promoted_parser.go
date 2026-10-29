package redis

import (
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/run-controller/domain/events"
	goredis "github.com/redis/go-redis/v9"
)

// ParseReleasePromoted decodes one release.promoted:v2 entry, envelope fields
// plus a typed payload, into the event state handles. A decode failure (no
// envelope, an unknown schema_version, no release_id, a promotion_seq below 1,
// no topology URI or checksum) or an envelope without a tenant_id is
// permanent: the consumer dead-letters the entry.
func ParseReleasePromoted(msg goredis.XMessage) (events.ReleasePromoted, error) {
	env, p, err := pkgevents.DecodeReleasePromoted(stringFields(msg.Values))
	if err != nil {
		return events.ReleasePromoted{}, fmt.Errorf("%w: decode release promotion (message %s): %v",
			pkgevents.ErrPermanent, msg.ID, err)
	}
	if env.TenantID == "" {
		return events.ReleasePromoted{}, fmt.Errorf("%w: release promotion (message %s) carries no tenant_id",
			pkgevents.ErrPermanent, msg.ID)
	}
	return events.ReleasePromoted{
		TenantID:       env.TenantID,
		ReleaseID:      p.ReleaseID,
		PromotionSeq:   p.PromotionSeq,
		TopologyURI:    p.TopologyURI,
		TopologySHA256: p.TopologySHA256,
		ChangedNodeIDs: p.ChangedNodeIDs,
		Bootstrap:      p.Bootstrap,
	}, nil
}

// stringFields copies a stream message's fields as strings. XREADGROUP returns
// every value as a string; any other value is formatted with %v.
func stringFields(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		if s, ok := v.(string); ok {
			out[k] = s
			continue
		}
		out[k] = fmt.Sprint(v)
	}
	return out
}
