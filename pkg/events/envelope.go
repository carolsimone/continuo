package events

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// EventIDNamespace is the UUIDv5 namespace of every event id derived from a
// natural key rather than taken from an outbox row id.
var EventIDNamespace = uuid.MustParse("2f0c5a7e-8d14-4b63-a1f9-6c3e5b2d7a40")

// DefaultTenantID is the tenant every event carries on a single-tenant install.
const DefaultTenantID = "default"

// occurredAtLayout renders occurred_at in UTC with microseconds, the precision
// Postgres timestamps keep.
const occurredAtLayout = "2006-01-02T15:04:05.000000Z07:00"

// Envelope is the header every new stream version carries. On the wire each
// field is a Redis stream field of its own, next to a "payload" field holding
// the stream's typed payload as JSON.
type Envelope struct {
	EventID       string
	TenantID      string
	OccurredAt    time.Time
	Producer      string
	SchemaVersion int
}

// Fields renders the envelope and payload as Redis stream fields.
func (e Envelope) Fields(payload any) (map[string]any, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal %T payload: %w", payload, err)
	}
	return map[string]any{
		"event_id":       e.EventID,
		"tenant_id":      e.TenantID,
		"occurred_at":    e.OccurredAt.UTC().Format(occurredAtLayout),
		"producer":       e.Producer,
		"schema_version": strconv.Itoa(e.SchemaVersion),
		"payload":        string(body),
	}, nil
}
