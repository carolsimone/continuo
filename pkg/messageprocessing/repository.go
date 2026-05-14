package messageprocessing

import (
	"context"

	"github.com/google/uuid"
)

// Repository defines operations on the per-service message_processing table.
// Implementations must operate against a table with columns:
// id (uuid PK), message_id (text), stream_name (text),
// state (text), payload (jsonb), error (text NULL),
// created_at (timestamptz), updated_at (timestamptz),
// with a composite UNIQUE constraint on (message_id, stream_name).
// The composite key is required because Redis Streams assign message IDs
// per-stream; a single outbox publisher can emit two messages to two
// streams in the same millisecond and produce identical message IDs.
type Repository interface {
	InsertIfNotExists(ctx context.Context, msgProc *MessageProcessing) (uuid.UUID, bool, error)
	GetByMessageIDAndStream(ctx context.Context, messageID, streamName string) (*MessageProcessing, error)
	UpdateState(ctx context.Context, id uuid.UUID, state string) error
}
