package redis

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain/event"
	goredis "github.com/redis/go-redis/v9"
)

// ParseReleasePromoted decodes a release.promoted:v1 XMessage's `payload`
// field into an event.ReleasePromoted DTO. Returns errors for missing payload
// field, malformed JSON, empty release_id, or nil topology — the binding
// treats all parser errors as permanent (ACK + drop).
func ParseReleasePromoted(msg goredis.XMessage) (event.ReleasePromoted, error) {
	raw, ok := msg.Values["payload"].(string)
	if !ok || raw == "" {
		return event.ReleasePromoted{}, fmt.Errorf("missing or empty payload field in message %s", msg.ID)
	}
	var evt event.ReleasePromoted
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		return event.ReleasePromoted{}, fmt.Errorf("unmarshal release.promoted payload (message %s): %w", msg.ID, err)
	}
	if evt.ReleaseID == "" {
		return event.ReleasePromoted{}, fmt.Errorf("release.promoted payload (message %s) has empty release_id", msg.ID)
	}
	if evt.Topology == nil {
		return event.ReleasePromoted{}, fmt.Errorf("release.promoted payload (message %s) has nil topology", msg.ID)
	}
	return evt, nil
}
