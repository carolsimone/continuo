package redis

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain/event"
	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
)

// ParseReleasePromoted decodes a release.promoted:v1 XMessage's `payload`
// field into an event.ReleasePromoted DTO. Returns events.ErrPermanent errors
// for a missing payload field, malformed JSON, empty release_id, or nil
// topology — the consumer ACKs + drops the poison message on these.
func ParseReleasePromoted(msg goredis.XMessage) (event.ReleasePromoted, error) {
	raw, ok := msg.Values["payload"].(string)
	if !ok || raw == "" {
		return event.ReleasePromoted{}, fmt.Errorf("%w: missing or empty payload field in message %s", events.ErrPermanent, msg.ID)
	}
	var evt event.ReleasePromoted
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		return event.ReleasePromoted{}, fmt.Errorf("%w: unmarshal release.promoted payload (message %s): %v", events.ErrPermanent, msg.ID, err)
	}
	if evt.ReleaseID == "" {
		return event.ReleasePromoted{}, fmt.Errorf("%w: release.promoted payload (message %s) has empty release_id", events.ErrPermanent, msg.ID)
	}
	if evt.Topology == nil {
		return event.ReleasePromoted{}, fmt.Errorf("%w: release.promoted payload (message %s) has nil topology", events.ErrPermanent, msg.ID)
	}
	// A node either pins a complete runtime manifest or pins none at all. A
	// half-filled reference cannot be executed and cannot be repaired by a
	// retry, so it is rejected here rather than persisted into the topology and
	// discovered at dispatch time.
	for i, n := range evt.Topology {
		ref := n.RuntimeManifestRef
		if err := ref.Validate(); err != nil {
			return event.ReleasePromoted{}, fmt.Errorf(
				"%w: release.promoted payload (message %s) topology[%d] (%s): %v",
				events.ErrPermanent, msg.ID, i, n.UniqueID, err)
		}
	}
	return evt, nil
}
