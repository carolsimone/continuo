package outbox

import (
	"time"

	"github.com/google/uuid"
)

// DefaultMaxRetries is the retry budget applied to an outbox Entry that is
// created without an explicit MaxRetries. The processor drops an entry to
// "failed" once RetryCount reaches MaxRetries (see processor.go), so this
// bounds how many times a transiently-failing publish is re-attempted before
// it is parked for inspection. This budget spans a bounded backoff window
// (~20–30 min) with capped exponential delays, allowing sustained retries
// during infrastructure recovery without leaving entries in limbo indefinitely.
const DefaultMaxRetries = 10

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
