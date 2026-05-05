package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// RejectedTopologyRepository writes forensics rows for permanently-rejected
// manifest.loaded:v1 messages. Used from a non-transactional context — the
// consumer ACKs after this call regardless of outcome, so a failed Insert
// must NOT turn a permanent error into a transient one.
type RejectedTopologyRepository interface {
	Insert(ctx context.Context, messageID, reason string, payload json.RawMessage) error
}

type rejectedTopologyRepository struct {
	db *sqlx.DB
}

// NewRejectedTopologyRepository constructs a RejectedTopologyRepository.
func NewRejectedTopologyRepository(db *sqlx.DB) RejectedTopologyRepository {
	return &rejectedTopologyRepository{db: db}
}

// Insert writes a forensics row. payload must be valid JSON.
func (r *rejectedTopologyRepository) Insert(
	ctx context.Context,
	messageID, reason string,
	payload json.RawMessage,
) error {
	const q = `
		INSERT INTO rejected_topology_messages (message_id, reason, payload)
		VALUES ($1, $2, $3)`
	if _, err := r.db.ExecContext(ctx, q, messageID, reason, payload); err != nil {
		return fmt.Errorf("insert rejected_topology_messages: %w", err)
	}
	return nil
}
