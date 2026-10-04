package outbox

import (
	"time"

	"github.com/google/uuid"
)

// MaxAttempts is how many times every outbox row is published before it is
// dead-lettered to outbox.dead_letter:v1. Failures back off from 1 s, doubling
// to 5 minutes, so the twelve waits between the thirteen attempts add up to
// about 24 minutes of outage.
const MaxAttempts = 13

// Entry is the canonical transactional-outbox row, shared across all services.
// Each service owns its own physical <service>_outbox table; this struct is the
// shared Go contract that every per-service table conforms to.
type Entry struct {
	ID                  uuid.UUID
	MessageProcessingID *uuid.UUID // provenance: nullable FK to message_processing(id)
	AggregateType       string
	AggregateID         uuid.UUID
	EventType           string
	Payload             []byte // JSONB; typed events.* struct marshaled here
	StreamName          string
	Status              string // "pending" | "scheduled" | "processed" | "failed"
	RetryCount          int
	CreatedAt           time.Time
	ProcessedAt         *time.Time
	ErrorMessage        *string
	NextAttemptAt       *time.Time // when a 'scheduled' (transiently-failed) row is next eligible; NULL = due now
}
