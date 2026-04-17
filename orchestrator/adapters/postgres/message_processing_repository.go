package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/google/uuid"
)

// MessageProcessingRepository handles message_processing table operations
type MessageProcessingRepository interface {
	InsertIfNotExists(ctx context.Context, msgProc *domain.MessageProcessing) (uuid.UUID, bool, error)
	GetByMessageID(ctx context.Context, messageID string) (*domain.MessageProcessing, error)
	UpdateState(ctx context.Context, id uuid.UUID, state string) error
}

// MessageProcessingExecutor abstracts sqlx.DB and sqlx.Tx for database operations
type MessageProcessingExecutor interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type messageProcessingRepository struct {
	executor MessageProcessingExecutor
	logger   *slog.Logger
}

// NewMessageProcessingRepository creates a new MessageProcessingRepository
func NewMessageProcessingRepository(executor MessageProcessingExecutor, logger *slog.Logger) MessageProcessingRepository {
	return &messageProcessingRepository{
		executor: executor,
		logger:   logger,
	}
}

// InsertIfNotExists inserts a new message processing record if it doesn't exist.
// Returns (id, inserted, error) where inserted=true if a new record was created.
func (r *messageProcessingRepository) InsertIfNotExists(
	ctx context.Context,
	msgProc *domain.MessageProcessing,
) (uuid.UUID, bool, error) {
	query := `
		INSERT INTO message_processing (message_id, stream_name, state, payload)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (message_id) DO NOTHING
		RETURNING id
	`

	var id uuid.UUID
	err := r.executor.QueryRowContext(
		ctx,
		query,
		msgProc.MessageID,
		msgProc.StreamName,
		msgProc.State,
		msgProc.Payload,
	).Scan(&id)

	if err == sql.ErrNoRows {
		// Conflict occurred, fetch existing ID
		existingQuery := `SELECT id FROM message_processing WHERE message_id = $1`
		err = r.executor.GetContext(ctx, &id, existingQuery, msgProc.MessageID)
		if err != nil {
			return uuid.Nil, false, fmt.Errorf("failed to get existing message: %w", err)
		}
		return id, false, nil
	}

	if err != nil {
		return uuid.Nil, false, fmt.Errorf("failed to insert message processing: %w", err)
	}

	return id, true, nil
}

// GetByMessageID retrieves a message processing record by Redis message ID
func (r *messageProcessingRepository) GetByMessageID(
	ctx context.Context,
	messageID string,
) (*domain.MessageProcessing, error) {
	var msgProc domain.MessageProcessing
	query := `SELECT * FROM message_processing WHERE message_id = $1`

	err := r.executor.GetContext(ctx, &msgProc, query, messageID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("message not found: %s", messageID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get message processing: %w", err)
	}

	return &msgProc, nil
}

// UpdateState updates the processing state of a message
func (r *messageProcessingRepository) UpdateState(
	ctx context.Context,
	id uuid.UUID,
	state string,
) error {
	query := `
		UPDATE message_processing
		SET state = $1, updated_at = NOW()
		WHERE id = $2
	`

	result, err := r.executor.ExecContext(ctx, query, state, id)
	if err != nil {
		return fmt.Errorf("failed to update state: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("no message processing record found with id: %s", id)
	}

	return nil
}
