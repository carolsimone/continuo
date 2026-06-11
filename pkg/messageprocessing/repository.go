package messageprocessing

import (
	"context"
	"time"

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
	// GetByID fetches a row by its primary key. Dedup uses this to look up the
	// existing row after a conflict on either of the unique constraints
	// (message_id+stream_name or outbox_entry_id), since GetByMessageIDAndStream
	// would miss the second case.
	GetByID(ctx context.Context, id uuid.UUID) (*MessageProcessing, error)
	UpdateState(ctx context.Context, id uuid.UUID, state string) error
	// DeleteTerminalOlderThan purges dedup rows that have reached a terminal
	// state ('completed' or 'acked') and whose updated_at is older than the
	// retention window, computed from the DB clock (NOW() - retention). At most
	// limit rows are deleted per call so a caller can run a bounded LIMIT loop;
	// it returns the number of rows deleted. Rows still in 'processing' are never
	// purged — an in-flight or stuck message must keep its dedup guard.
	DeleteTerminalOlderThan(ctx context.Context, retention time.Duration, limit int) (int64, error)
}
