// executor-controller/adapters/redis/validation_node_completed_parser.go
package redis

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// validationNodeCompletedDTO mirrors the flat JSON body k8s-controller emits in
// the "payload" field of a validation.node.completed:v1 message.
type validationNodeCompletedDTO struct {
	ReleaseID string `json:"release_id"`
	NodeID    string `json:"node_id"`
	Outcome   string `json:"outcome"`
	DBTLogURI string `json:"dbt_log_uri"`
}

// ParseValidationNodeCompleted translates a validation.node.completed:v1
// XMessage into a typed domain event. The wire format is a single "payload"
// field carrying the JSON body (matching the outbox publisher); outbox_entry_id
// rides alongside as provenance.
//
// All errors are permanent (malformed input never becomes valid on retry); the
// binding wraps them with events.ErrPermanent so the consumer ACKs rather than
// redelivering forever. outcome must be one of "ok" or "failed".
func ParseValidationNodeCompleted(msg goredis.XMessage) (events.ValidationNodeCompleted, error) {
	raw := stringField(msg.Values, "payload")
	if raw == "" {
		return events.ValidationNodeCompleted{}, fmt.Errorf("missing payload field")
	}

	var dto validationNodeCompletedDTO
	if err := json.Unmarshal([]byte(raw), &dto); err != nil {
		return events.ValidationNodeCompleted{}, fmt.Errorf("unmarshal payload: %w", err)
	}

	if dto.ReleaseID == "" {
		return events.ValidationNodeCompleted{}, fmt.Errorf("missing release_id")
	}
	if dto.NodeID == "" {
		return events.ValidationNodeCompleted{}, fmt.Errorf("missing node_id")
	}
	if dto.Outcome != "ok" && dto.Outcome != "failed" {
		return events.ValidationNodeCompleted{},
			fmt.Errorf("invalid outcome %q (want \"ok\" or \"failed\")", dto.Outcome)
	}

	// outbox_entry_id is the k8s-controller outbox row ID, carried as provenance.
	// Absent or empty → uuid.Nil (dedup falls back to (msg.ID, stream_name)).
	// Present-but-malformed → permanent error.
	var outboxEntryID uuid.UUID
	if s := stringField(msg.Values, "outbox_entry_id"); s != "" {
		var err error
		outboxEntryID, err = uuid.Parse(s)
		if err != nil {
			return events.ValidationNodeCompleted{}, fmt.Errorf("invalid outbox_entry_id: %w", err)
		}
	}

	return events.ValidationNodeCompleted{
		OutboxEntryID: outboxEntryID,
		ReleaseID:     dto.ReleaseID,
		NodeID:        dto.NodeID,
		Outcome:       dto.Outcome,
		DBTLogURI:     dto.DBTLogURI,
	}, nil
}
