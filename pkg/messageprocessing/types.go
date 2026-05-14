package messageprocessing

import (
	"time"

	"github.com/google/uuid"
)

// MessageProcessing tracks consumed Redis messages for exactly-once processing.
// Each service owns its own physical message_processing table in its own database;
// this struct is the shared Go contract.
type MessageProcessing struct {
	ID         uuid.UUID
	MessageID  string
	StreamName string
	State      string // "processing", "completed", "acked"
	Payload    []byte
	Error      *string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}
