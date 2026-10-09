package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
)

// ParseReleasePromotedV2 decodes one release.promoted:v2 entry — envelope fields
// plus a typed payload — into the handler input. Any decode failure (missing or
// malformed envelope, unknown schema_version, empty release_id, a seq below 1,
// no topology URI or checksum) is permanent: the consumer dead-letters the entry.
func ParseReleasePromotedV2(msg goredis.XMessage) (model.PromoteReleaseInput, error) {
	_, p, err := events.DecodeReleasePromoted(stringFields(msg.Values))
	if err != nil {
		return model.PromoteReleaseInput{}, fmt.Errorf("%w: decode release promotion (message %s): %v",
			events.ErrPermanent, msg.ID, err)
	}
	return model.PromoteReleaseInput{
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
