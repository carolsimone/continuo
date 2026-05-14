package messageprocessing

import (
	"context"

	"github.com/google/uuid"
)

// Repository defines operations on the per-service message_processing table.
// Implementations must operate against a table with columns:
// id (uuid PK), message_id (text UNIQUE), stream_name (text),
// state (text), payload (jsonb), error (text NULL),
// created_at (timestamptz), updated_at (timestamptz).
type Repository interface {
	InsertIfNotExists(ctx context.Context, msgProc *MessageProcessing) (uuid.UUID, bool, error)
	GetByMessageID(ctx context.Context, messageID string) (*MessageProcessing, error)
	UpdateState(ctx context.Context, id uuid.UUID, state string) error
}
