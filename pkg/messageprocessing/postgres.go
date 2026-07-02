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
	ID            uuid.UUID  `db:"id"`
	MessageID     string     `db:"message_id"`
	StreamName    string     `db:"stream_name"`
	State         string     `db:"state"`
	Payload       []byte     `db:"payload"`
	Error         *string    `db:"error"`
	OutboxEntryID *uuid.UUID `db:"outbox_entry_id"`
	CreatedAt     time.Time  `db:"created_at"`
	UpdatedAt     time.Time  `db:"updated_at"`
}

func fromRow(r *row) *MessageProcessing {
	return &MessageProcessing{
		ID:            r.ID,
		MessageID:     r.MessageID,
		StreamName:    r.StreamName,
		State:         r.State,
		Payload:       r.Payload,
		Error:         r.Error,
		OutboxEntryID: r.OutboxEntryID,
		CreatedAt:     r.CreatedAt,
		UpdatedAt:     r.UpdatedAt,
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
	// Two unique constraints can fire here:
	//   1. UNIQUE (message_id, stream_name) — primary inbound dedup key.
	//   2. UNIQUE (outbox_entry_id, stream_name) WHERE outbox_entry_id IS NOT NULL —
	//      catches the rare case where a producer's outbox Processor crashed
	//      between XADD and MarkProcessed and republished the same outbox
	//      row, producing a fresh Redis message_id but the same upstream
	//      outbox_entry_id. Scoping by stream_name (the consuming group) mirrors
	//      the primary key, so when two consumer groups read the same source
	//      message each still processes it exactly once instead of one group
	//      silently claiming the shared outbox_entry_id.
	// ON CONFLICT DO NOTHING (no target) handles either violation. When the
	// insert is suppressed, we look up the existing row by whichever key
	// matched.
	query := `
		INSERT INTO message_processing (message_id, stream_name, state, payload, outbox_entry_id)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING
		RETURNING id
	`
	var id uuid.UUID
	err := r.exec.QueryRowContext(ctx, query,
		msgProc.MessageID, msgProc.StreamName, msgProc.State, msgProc.Payload, msgProc.OutboxEntryID,
	).Scan(&id)
	if err == sql.ErrNoRows {
		// Either constraint was violated. Look up the existing row by either
		// key; outbox_entry_id IS NOT NULL guard keeps the OR safe when the
		// caller didn't supply one.
		err = r.exec.GetContext(ctx, &id, `
			SELECT id FROM message_processing
			WHERE (message_id = $1 AND stream_name = $2)
			   OR (outbox_entry_id IS NOT NULL AND outbox_entry_id = $3)
			LIMIT 1
		`, msgProc.MessageID, msgProc.StreamName, msgProc.OutboxEntryID)
		if err != nil {
			return uuid.Nil, false, fmt.Errorf("get existing message after conflict: %w", err)
		}
		return id, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("insert message processing: %w", err)
	}
	return id, true, nil
}

func (r *postgresRepository) AlreadyProcessed(
	ctx context.Context, messageID, streamName string, outboxEntryID *uuid.UUID,
) (bool, error) {
	// Mirrors the InsertIfNotExists conflict-lookup WHERE clause so the read-only
	// pre-check sees a row on either dedup axis: the primary (message_id,
	// stream_name) key or the secondary outbox_entry_id key (guarded so a nil
	// outboxEntryID never matches an unrelated NULL row).
	var exists bool
	err := r.exec.GetContext(ctx, &exists, `
		SELECT EXISTS (
			SELECT 1 FROM message_processing
			WHERE (message_id = $1 AND stream_name = $2)
			   OR (outbox_entry_id IS NOT NULL AND outbox_entry_id = $3)
		)
	`, messageID, streamName, outboxEntryID)
	if err != nil {
		return false, fmt.Errorf("check message already processed: %w", err)
	}
	return exists, nil
}

func (r *postgresRepository) GetByMessageIDAndStream(
	ctx context.Context, messageID, streamName string,
) (*MessageProcessing, error) {
	var rr row
	err := r.exec.GetContext(ctx, &rr,
		`SELECT * FROM message_processing WHERE message_id = $1 AND stream_name = $2`,
		messageID, streamName)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("message not found: %s on stream %s", messageID, streamName)
	}
	if err != nil {
		return nil, fmt.Errorf("get message processing: %w", err)
	}
	return fromRow(&rr), nil
}

func (r *postgresRepository) GetByID(
	ctx context.Context, id uuid.UUID,
) (*MessageProcessing, error) {
	var rr row
	err := r.exec.GetContext(ctx, &rr,
		`SELECT * FROM message_processing WHERE id = $1`, id)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("message_processing row not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("get message processing by id: %w", err)
	}
	return fromRow(&rr), nil
}

func (r *postgresRepository) DeleteTerminalOlderThan(
	ctx context.Context, retention time.Duration, limit int,
) (int64, error) {
	// Bounded delete of terminal dedup rows. A subquery picks up to limit
	// eligible ids (state in the terminal set and updated_at older than
	// NOW()-retention) and the outer DELETE removes exactly those, keeping each
	// statement's lock footprint small. 'processing' rows are excluded so an
	// in-flight (or stuck) message keeps its dedup guard. The cutoff uses the DB
	// clock (NOW()) to avoid host/DB skew; make_interval takes whole seconds.
	query := `
		DELETE FROM message_processing
		WHERE id IN (
			SELECT id FROM message_processing
			WHERE state IN ('completed', 'acked')
			  AND updated_at < NOW() - make_interval(secs => $1)
			ORDER BY updated_at ASC
			LIMIT $2
		)
	`
	result, err := r.exec.ExecContext(ctx, query, retention.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("delete terminal message_processing older than: %w", err)
	}
	n, _ := result.RowsAffected()
	return n, nil
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
