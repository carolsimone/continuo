package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// Executor abstracts sqlx.DB and sqlx.Tx for database operations
type Executor interface {
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// ProcessedEventsRepository defines operations for consumer-side dedup.
type ProcessedEventsRepository interface {
	// TryMarkProcessed atomically tries to record outbox_entry_id as processed.
	// Returns (true, nil)  — entry already processed; caller should skip.
	// Returns (false, nil) — newly inserted; caller should proceed.
	// Must be called inside a transaction so a later rollback undoes the insert.
	TryMarkProcessed(ctx context.Context, outboxEntryID uuid.UUID) (bool, error)
}

type processedEventsRepository struct {
	executor Executor
	logger   *slog.Logger
}

// NewProcessedEventsRepositoryWithTx creates a ProcessedEventsRepository backed by tx.
func NewProcessedEventsRepositoryWithTx(tx *sqlx.Tx, logger *slog.Logger) ProcessedEventsRepository {
	return &processedEventsRepository{executor: tx, logger: logger}
}

func (r *processedEventsRepository) TryMarkProcessed(ctx context.Context, outboxEntryID uuid.UUID) (bool, error) {
	res, err := r.executor.ExecContext(ctx,
		`INSERT INTO processed_events (outbox_entry_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		outboxEntryID)
	if err != nil {
		return false, fmt.Errorf("processed_events TryMarkProcessed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("processed_events TryMarkProcessed rows affected: %w", err)
	}
	return n == 0, nil // n==0 → conflict → already processed
}

// stuckOutboxRow is the internal scan struct for canonical k8s_outbox stuck-entry queries.
type stuckOutboxRow struct {
	ID           uuid.UUID  `db:"id"`
	AggregateID  uuid.UUID  `db:"aggregate_id"`
	EventType    string     `db:"event_type"`
	RetryCount   int        `db:"retry_count"`
	MaxRetries   int        `db:"max_retries"`
	CreatedAt    time.Time  `db:"created_at"`
	ProcessedAt  *time.Time `db:"processed_at"`
	ErrorMessage *string    `db:"error_message"`
}

// K8sOutboxStuckRepository implements handlers.StuckEntryRepository against k8s_outbox.
type K8sOutboxStuckRepository struct {
	db     *sqlx.DB
	logger *slog.Logger
}

// NewK8sOutboxStuckRepository creates a repository for stuck-entry operations on k8s_outbox.
func NewK8sOutboxStuckRepository(db *sqlx.DB, logger *slog.Logger) *K8sOutboxStuckRepository {
	return &K8sOutboxStuckRepository{db: db, logger: logger}
}

// GetStuckEntries returns canonical outbox entries whose retry budget is exhausted
// and that have been pending longer than stuckThresholdSeconds.
// Uses FOR UPDATE SKIP LOCKED so multiple resolver instances do not conflict.
func (r *K8sOutboxStuckRepository) GetStuckEntries(ctx context.Context, limit int, stuckThresholdSeconds int) ([]*pkgoutbox.Entry, error) {
	query := `
		SELECT id, aggregate_id, event_type, retry_count, max_retries,
		       created_at, processed_at, error_message
		FROM k8s_outbox
		WHERE status = 'pending'
		  AND retry_count >= max_retries
		  AND created_at < NOW() - ($1 || ' seconds')::INTERVAL
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	var rows []*stuckOutboxRow
	err := r.db.SelectContext(ctx, &rows, query, stuckThresholdSeconds, limit)
	if err != nil && err != sql.ErrNoRows {
		r.logger.Error("Failed to get stuck k8s outbox entries", "error", err)
		return nil, fmt.Errorf("failed to get stuck k8s outbox entries: %w", err)
	}

	entries := make([]*pkgoutbox.Entry, len(rows))
	for i, row := range rows {
		entries[i] = &pkgoutbox.Entry{
			ID:           row.ID,
			AggregateID:  row.AggregateID,
			EventType:    row.EventType,
			RetryCount:   row.RetryCount,
			MaxRetries:   row.MaxRetries,
			CreatedAt:    row.CreatedAt,
			ProcessedAt:  row.ProcessedAt,
			ErrorMessage: row.ErrorMessage,
		}
	}

	r.logger.Debug("Retrieved stuck k8s outbox entries", "count", len(entries))
	return entries, nil
}

// ForceMarkFailed marks an entry as failed with a 5-second timeout, also setting processed_at.
func (r *K8sOutboxStuckRepository) ForceMarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE k8s_outbox
		SET status = 'failed',
		    error_message = $1,
		    processed_at = $2
		WHERE id = $3
	`

	result, err := r.db.ExecContext(ctxWithTimeout, query, errorMessage, time.Now(), id)
	if err != nil {
		r.logger.Error("Failed to force mark k8s outbox entry as failed", "id", id, "error", err)
		return fmt.Errorf("failed to force mark k8s outbox entry as failed: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		r.logger.Warn("k8s outbox entry not found for force mark failed", "id", id)
		return fmt.Errorf("k8s outbox entry not found")
	}

	r.logger.Warn("Force marked k8s outbox entry as failed", "id", id, "error_message", errorMessage)
	return nil
}
