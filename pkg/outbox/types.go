package outbox

import (
	"time"

	"github.com/google/uuid"
)

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
	Status              string // "pending" | "processed" | "failed"
	RetryCount          int
	MaxRetries          int
	CreatedAt           time.Time
	ProcessedAt         *time.Time
	ErrorMessage        *string
}
