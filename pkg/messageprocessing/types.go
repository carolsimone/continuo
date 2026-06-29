package messageprocessing

import (
	"time"

	"github.com/google/uuid"
)

// MessageProcessing tracks consumed Redis messages for exactly-once processing.
// Each service owns its own physical message_processing table in its own database;
// this struct is the shared Go contract.
type MessageProcessing struct {
	ID        uuid.UUID
	MessageID string
	// StreamName is the consumer's DEDUP SCOPE. Normally it holds the Redis stream
	// name. Exception: when multiple consumer groups read the SAME stream, each
	// group stores its consumer-group name here instead, so that their dedup rows
	// (and the V12 (outbox_entry_id, stream_name) unique index entries) remain
	// distinct and do not collide across groups.
	StreamName string
	State      string // "processing", "completed", "acked"
	Payload    []byte
	Error      *string
	// OutboxEntryID, when non-nil, is the upstream pkg/outbox row UUID that
	// produced this Redis message. It carries a secondary uniqueness invariant:
	// the same outbox row published twice (e.g., after the Processor crashes
	// between XADD and MarkProcessed) yields two Redis messages with different
	// stream-assigned message_ids but the same outbox_entry_id. Deduping on
	// outbox_entry_id catches the redelivery that (message_id, stream_name)
	// alone would miss.
	OutboxEntryID *uuid.UUID
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
