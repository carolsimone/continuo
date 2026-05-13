package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/google/uuid"
)

// messageProcessingExecutor abstracts sqlx.DB and sqlx.Tx for message_processing operations.
type messageProcessingExecutor interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// messageProcessingRow is the adapter-internal scan struct for SELECT queries against message_processing.
type messageProcessingRow struct {
	ID         uuid.UUID `db:"id"`
	MessageID  string    `db:"message_id"`
	StreamName string    `db:"stream_name"`
	State      string    `db:"state"`
	Payload    []byte    `db:"payload"`
	Error      *string   `db:"error"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
}

func domainFromMessageProcessingRow(r *messageProcessingRow) *domain.MessageProcessing {
	return &domain.MessageProcessing{
		ID:         r.ID,
		MessageID:  r.MessageID,
		StreamName: r.StreamName,
		State:      r.State,
		Payload:    r.Payload,
		Error:      r.Error,
		CreatedAt:  r.CreatedAt,
		UpdatedAt:  r.UpdatedAt,
	}
}

// compile-time interface check
var _ repository.MessageProcessingRepository = (*messageProcessingRepository)(nil)

type messageProcessingRepository struct {
	executor messageProcessingExecutor
	logger   *slog.Logger
}

// NewMessageProcessingRepository creates a new MessageProcessingRepository.
func NewMessageProcessingRepository(executor messageProcessingExecutor, logger *slog.Logger) repository.MessageProcessingRepository {
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
	err := r.executor.QueryRowContext(ctx, query,
		msgProc.MessageID,
		msgProc.StreamName,
		msgProc.State,
		msgProc.Payload,
	).Scan(&id)

	if err == sql.ErrNoRows {
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

// GetByMessageID retrieves a message processing record by Redis message ID.
func (r *messageProcessingRepository) GetByMessageID(
	ctx context.Context,
	messageID string,
) (*domain.MessageProcessing, error) {
	var row messageProcessingRow
	query := `SELECT * FROM message_processing WHERE message_id = $1`

	err := r.executor.GetContext(ctx, &row, query, messageID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("message not found: %s", messageID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get message processing: %w", err)
	}

	return domainFromMessageProcessingRow(&row), nil
}

// UpdateState updates the processing state of a message.
func (r *messageProcessingRepository) UpdateState(
	ctx context.Context,
	id uuid.UUID,
	state string,
) error {
	query := `UPDATE message_processing SET state = $1, updated_at = NOW() WHERE id = $2`

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
