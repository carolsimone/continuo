package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/catalog/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
)

// ParseReleasePromotedV2 decodes one release.promoted:v2 entry — envelope fields
// plus a typed payload — into the handler input, under the envelope's tenant.
// Any decode failure (missing or malformed envelope, unknown schema_version,
// empty release_id, a seq below 1, no topology URI or checksum) and an envelope
// without a tenant_id are permanent: the consumer dead-letters the entry, so
// nothing is ever written under no tenant.
func ParseReleasePromotedV2(msg goredis.XMessage) (model.PromoteReleaseInput, error) {
	env, p, err := events.DecodeReleasePromoted(stringFields(msg.Values))
	if err != nil {
		return model.PromoteReleaseInput{}, fmt.Errorf("%w: decode release promotion (message %s): %v",
			events.ErrPermanent, msg.ID, err)
	}
	if env.TenantID == "" {
		return model.PromoteReleaseInput{}, fmt.Errorf("%w: release promotion (message %s) carries no tenant_id",
			events.ErrPermanent, msg.ID)
	}
	return model.PromoteReleaseInput{
		TenantID:       env.TenantID,
		ReleaseID:      p.ReleaseID,
		PromotionSeq:   p.PromotionSeq,
		TopologyURI:    p.TopologyURI,
		TopologySHA256: p.TopologySHA256,
		ChangedNodeIDs: p.ChangedNodeIDs,
		Repo:           p.Repo,
		CommitSHA:      p.CommitSHA,
		PromotedAt:     p.PromotedAt,
		CodeBundleURI:  p.CodeBundleURI,
		Bootstrap:      p.Bootstrap,
	}, nil
}
