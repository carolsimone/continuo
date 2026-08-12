package redis

import (
	"encoding/json"
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/events"
	goredis "github.com/redis/go-redis/v9"
)

// ParseReleaseSeedsPending decodes a release.seeds.pending:v1 XMessage's
// `payload` field.
//
// A missing payload, malformed JSON, an empty release_id, an empty node list, or
// a node missing its identity is permanent — the consumer ACKs and drops a
// message that can never parse rather than redelivering it forever. orchestrator
// does not emit this event when a release changed no seeds, so an empty list
// means a malformed message rather than a promotion with nothing to build.
func ParseReleaseSeedsPending(msg goredis.XMessage) (events.ReleaseSeedsPending, error) {
	raw, ok := msg.Values["payload"].(string)
	if !ok || raw == "" {
		return events.ReleaseSeedsPending{}, fmt.Errorf("%w: missing or empty payload field in message %s", pkgevents.ErrPermanent, msg.ID)
	}
	var evt events.ReleaseSeedsPending
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		return events.ReleaseSeedsPending{}, fmt.Errorf("%w: unmarshal release.seeds.pending payload (message %s): %v", pkgevents.ErrPermanent, msg.ID, err)
	}
	if evt.ReleaseID == "" {
		return events.ReleaseSeedsPending{}, fmt.Errorf("%w: release.seeds.pending payload (message %s) has empty release_id", pkgevents.ErrPermanent, msg.ID)
	}
	if len(evt.Nodes) == 0 {
		return events.ReleaseSeedsPending{}, fmt.Errorf("%w: release.seeds.pending payload (message %s) carries no nodes", pkgevents.ErrPermanent, msg.ID)
	}
	for i, n := range evt.Nodes {
		if n.ServiceName == "" || n.SchemaName == "" || n.TableName == "" {
			return events.ReleaseSeedsPending{}, fmt.Errorf(
				"%w: release.seeds.pending payload (message %s) node %d is missing service_name, schema_name, or table_name",
				pkgevents.ErrPermanent, msg.ID, i)
		}
	}
	return evt, nil
}
