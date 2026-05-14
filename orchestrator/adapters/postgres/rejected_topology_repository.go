package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/jmoiron/sqlx"
)

type rejectedTopologyRepository struct {
	db *sqlx.DB
}

// NewRejectedTopologyRepository constructs a rejected-topology repository.
// It writes forensics rows for permanently-rejected manifest.loaded:v1
// messages. Used from a non-transactional context — the consumer ACKs after
// this call regardless of outcome, so a failed Insert must NOT turn a
// permanent error into a transient one.
func NewRejectedTopologyRepository(db *sqlx.DB) repository.RejectedTopologyRepository {
	return &rejectedTopologyRepository{db: db}
}

var _ repository.RejectedTopologyRepository = (*rejectedTopologyRepository)(nil)

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
