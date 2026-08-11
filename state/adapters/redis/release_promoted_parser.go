package redis

import (
	"encoding/json"
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/events"
	goredis "github.com/redis/go-redis/v9"
)

// ParseReleasePromoted decodes a release.promoted:v1 XMessage's `payload` field
// into the narrowed view state consumes. A missing payload, malformed JSON, or
// an empty release_id is permanent — the consumer ACKs and drops the message
// rather than redelivering a payload that can never parse.
//
// A nil topology is not an error here: a promotion carrying no nodes simply has
// no seeds to build, and the handler no-ops on it.
func ParseReleasePromoted(msg goredis.XMessage) (events.ReleasePromoted, error) {
	raw, ok := msg.Values["payload"].(string)
	if !ok || raw == "" {
		return events.ReleasePromoted{}, fmt.Errorf("%w: missing or empty payload field in message %s", pkgevents.ErrPermanent, msg.ID)
	}
	var evt events.ReleasePromoted
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		return events.ReleasePromoted{}, fmt.Errorf("%w: unmarshal release.promoted payload (message %s): %v", pkgevents.ErrPermanent, msg.ID, err)
	}
	if evt.ReleaseID == "" {
		return events.ReleasePromoted{}, fmt.Errorf("%w: release.promoted payload (message %s) has empty release_id", pkgevents.ErrPermanent, msg.ID)
	}
	return evt, nil
}
