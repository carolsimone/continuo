package ports

import (
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// PromotionTrigger is what one applied release.promoted:v2 message tells the
// seed reconcile: the promotion, what it did to the live-release pointer,
// whether it is the first promotion state applied (no release was live before
// it), the release's bootstrap flag and changed nodes, and the dedup row of the
// message's transaction, which rows written in that transaction name.
type PromotionTrigger struct {
	TenantID            string
	ReleaseID           string
	PromotionSeq        int64
	Outcome             topology.PointerOutcome
	FirstPromotion      bool
	Bootstrap           bool
	ChangedNodeIDs      []string
	MessageProcessingID uuid.UUID
}
