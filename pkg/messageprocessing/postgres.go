package messageprocessing

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// executor abstracts sqlx.DB and sqlx.Tx so the same Postgres repo can be used inside or outside a transaction.
type executor interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

type row struct {
	ID         uuid.UUID `db:"id"`
	MessageID  string    `db:"message_id"`
	StreamName string    `db:"stream_name"`
	State      string    `db:"state"`
	Payload    []byte    `db:"payload"`
	Error      *string   `db:"error"`
	CreatedAt  time.Time `db:"created_at"`
	UpdatedAt  time.Time `db:"updated_at"`
}

func fromRow(r *row) *MessageProcessing {
	return &MessageProcessing{
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

var _ Repository = (*postgresRepository)(nil)

type postgresRepository struct {
	exec   executor
	logger *slog.Logger
}

// NewPostgresRepository constructs a Postgres-backed Repository.
// The caller passes either *sqlx.DB (autocommit) or *sqlx.Tx (transactional).
func NewPostgresRepository(exec executor, logger *slog.Logger) Repository {
	return &postgresRepository{exec: exec, logger: logger}
}

func (r *postgresRepository) InsertIfNotExists(
	ctx context.Context, msgProc *MessageProcessing,
) (uuid.UUID, bool, error) {
	query := `
		INSERT INTO message_processing (message_id, stream_name, state, payload)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (message_id) DO NOTHING
		RETURNING id
	`
	var id uuid.UUID
	err := r.exec.QueryRowContext(ctx, query,
		msgProc.MessageID, msgProc.StreamName, msgProc.State, msgProc.Payload,
	).Scan(&id)
	if err == sql.ErrNoRows {
		err = r.exec.GetContext(ctx, &id,
			`SELECT id FROM message_processing WHERE message_id = $1`, msgProc.MessageID)
		if err != nil {
			return uuid.Nil, false, fmt.Errorf("get existing message: %w", err)
		}
		return id, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("insert message processing: %w", err)
	}
	return id, true, nil
}

func (r *postgresRepository) GetByMessageID(
	ctx context.Context, messageID string,
) (*MessageProcessing, error) {
	var rr row
	err := r.exec.GetContext(ctx, &rr,
		`SELECT * FROM message_processing WHERE message_id = $1`, messageID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("message not found: %s", messageID)
	}
	if err != nil {
		return nil, fmt.Errorf("get message processing: %w", err)
	}
	return fromRow(&rr), nil
}

func (r *postgresRepository) UpdateState(
	ctx context.Context, id uuid.UUID, state string,
) error {
	result, err := r.exec.ExecContext(ctx,
		`UPDATE message_processing SET state = $1, updated_at = NOW() WHERE id = $2`,
		state, id)
	if err != nil {
		return fmt.Errorf("update state: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("no message_processing row with id %s", id)
	}
	return nil
}
