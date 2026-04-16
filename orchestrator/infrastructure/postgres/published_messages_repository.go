package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// PublishedMessagesRepository handles published_messages table operations
type PublishedMessagesRepository interface {
	Exists(ctx context.Context, outboxEntryID uuid.UUID) (bool, error)
	Create(ctx context.Context, pm *domain.PublishedMessage) error
}

type publishedMessagesRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

// NewPublishedMessagesRepository creates a new PublishedMessagesRepository
func NewPublishedMessagesRepository(db *sqlx.DB, logger *slog.Logger) PublishedMessagesRepository {
	return &publishedMessagesRepository{
		db:     db,
		logger: logger,
	}
}

// Exists checks if an outbox entry has already been published
func (r *publishedMessagesRepository) Exists(
	ctx context.Context,
	outboxEntryID uuid.UUID,
) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM published_messages WHERE outbox_entry_id = $1)`

	err := r.db.GetContext(ctx, &exists, query, outboxEntryID)
	if err != nil {
		return false, fmt.Errorf("failed to check published messages: %w", err)
	}

	return exists, nil
}

// Create records a successful publish
func (r *publishedMessagesRepository) Create(
	ctx context.Context,
	pm *domain.PublishedMessage,
) error {
	query := `
		INSERT INTO published_messages (outbox_entry_id, redis_message_id)
		VALUES ($1, $2)
		ON CONFLICT (outbox_entry_id) DO NOTHING
	`

	_, err := r.db.ExecContext(ctx, query, pm.OutboxEntryID, pm.RedisMessageID)
	if err != nil {
		return fmt.Errorf("failed to create published message: %w", err)
	}

	return nil
}
