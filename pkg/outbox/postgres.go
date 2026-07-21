// pkg/outbox/postgres.go
package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// outboxRow is the adapter-internal scan struct.
type outboxRow struct {
	ID                  uuid.UUID  `db:"id"`
	MessageProcessingID *uuid.UUID `db:"message_processing_id"`
	AggregateType       string     `db:"aggregate_type"`
	AggregateID         uuid.UUID  `db:"aggregate_id"`
	EventType           string     `db:"event_type"`
	Payload             []byte     `db:"payload"`
	StreamName          string     `db:"stream_name"`
	Status              string     `db:"status"`
	RetryCount          int        `db:"retry_count"`
	MaxRetries          int        `db:"max_retries"`
	CreatedAt           time.Time  `db:"created_at"`
	ProcessedAt         *time.Time `db:"processed_at"`
	ErrorMessage        *string    `db:"error_message"`
	NextAttemptAt       *time.Time `db:"next_attempt_at"`
}

func entryFromRow(r *outboxRow) *Entry {
	return &Entry{
		ID:                  r.ID,
		MessageProcessingID: r.MessageProcessingID,
		AggregateType:       r.AggregateType,
		AggregateID:         r.AggregateID,
		EventType:           r.EventType,
		Payload:             r.Payload,
		StreamName:          r.StreamName,
		Status:              r.Status,
		RetryCount:          r.RetryCount,
		MaxRetries:          r.MaxRetries,
		CreatedAt:           r.CreatedAt,
		ProcessedAt:         r.ProcessedAt,
		ErrorMessage:        r.ErrorMessage,
		NextAttemptAt:       r.NextAttemptAt,
	}
}

type postgresRepository struct {
	exec             Executor
	tableName        string
	logger           *slog.Logger
	perAggregateFIFO bool
}

// Option configures a postgresRepository.
type Option func(*postgresRepository)

// WithPerAggregateOrdering makes GetPendingBatch return only the oldest pending
// row per aggregate, so rows sharing an aggregate_id are published in creation
// order — a later row is withheld until the earlier one is processed. Rows for
// different aggregates are unaffected and still drain in parallel. Use this for
// producers that write multiple ordered events for the same aggregate and need
// the consumer to observe them in order (e.g. executor's RUNNING before
// node_deployed).
func WithPerAggregateOrdering() Option {
	return func(r *postgresRepository) { r.perAggregateFIFO = true }
}

// newPostgresRepository builds the concrete repository. In-package callers (the
// processor) use it directly to reach ScheduleRetry / CountTerminal, which are
// deliberately NOT on the Repository interface so the ~10 service fakes that
// satisfy pkgoutbox.Repository need no changes.
func newPostgresRepository(exec Executor, tableName string, logger *slog.Logger, opts ...Option) *postgresRepository {
	r := &postgresRepository{exec: exec, tableName: tableName, logger: logger}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// NewPostgresRepository constructs a Repository bound to a specific physical
// table. Pass *sqlx.DB for autocommit operations (the Processor's GetPendingBatch
// holds its own tx) or *sqlx.Tx for transactional writes (the writer's Create
// must run inside the UoW transaction).
func NewPostgresRepository(exec Executor, tableName string, logger *slog.Logger, opts ...Option) Repository {
	return newPostgresRepository(exec, tableName, logger, opts...)
}

func (r *postgresRepository) Create(ctx context.Context, entry *Entry) error {
	if entry.ID == uuid.Nil {
		entry.ID = uuid.New()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	if entry.Status == "" {
		entry.Status = "pending"
	}
	if entry.MaxRetries == 0 {
		entry.MaxRetries = DefaultMaxRetries
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, message_processing_id, aggregate_type, aggregate_id,
			event_type, payload, stream_name,
			status, retry_count, max_retries, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, r.tableName)

	_, err := r.exec.ExecContext(ctx, query,
		entry.ID, entry.MessageProcessingID, entry.AggregateType, entry.AggregateID,
		entry.EventType, entry.Payload, entry.StreamName,
		entry.Status, entry.RetryCount, entry.MaxRetries, entry.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create outbox entry in %s: %w", r.tableName, err)
	}
	return nil
}

func (r *postgresRepository) GetPendingBatch(ctx context.Context, limit int) ([]*Entry, error) {
	// When per-aggregate ordering is enabled, withhold any row that has an
	// older still-pending-or-scheduled sibling for the same aggregate, so
	// events for one aggregate publish strictly in creation order. A
	// backed-off ('scheduled') sibling must still withhold younger rows —
	// otherwise a younger row could publish out of order while its older
	// sibling waits out its backoff. created_at is assigned per Create call
	// (time.Now), so siblings written in one writer transaction get distinct,
	// ordered timestamps.
	fifoClause := ""
	if r.perAggregateFIFO {
		fifoClause = fmt.Sprintf(`
		  AND NOT EXISTS (
		      SELECT 1 FROM %s older
		      WHERE older.aggregate_type = o.aggregate_type
		        AND older.aggregate_id   = o.aggregate_id
		        AND older.status IN ('pending', 'scheduled')
		        AND older.created_at < o.created_at
		  )`, r.tableName)
	}
	// Fresh 'pending' rows always have next_attempt_at NULL, so they are
	// always eligible; 'scheduled' rows (previously backed off after a
	// transient failure) become eligible once their deadline passes.
	// clock_timestamp() is the actual statement-execution wall clock — unlike
	// NOW(), which is fixed at transaction start — so the due-check reflects
	// when this SELECT actually runs, not when the enclosing tx began.
	query := fmt.Sprintf(`
		SELECT id, message_processing_id, aggregate_type, aggregate_id,
		       event_type, payload, stream_name,
		       status, retry_count, max_retries,
		       created_at, processed_at, error_message, next_attempt_at
		FROM %s o
		WHERE status IN ('pending', 'scheduled')
		  AND (next_attempt_at IS NULL OR next_attempt_at <= clock_timestamp())%s
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, r.tableName, fifoClause)

	var rows []*outboxRow
	if err := r.exec.SelectContext(ctx, &rows, query, limit); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("get pending batch from %s: %w", r.tableName, err)
	}

	entries := make([]*Entry, len(rows))
	for i, row := range rows {
		entries[i] = entryFromRow(row)
	}
	return entries, nil
}

func (r *postgresRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	// processed_at is stamped from the DB clock (NOW()) so retention cutoffs,
	// which also use NOW(), compare like-for-like and are immune to host/DB
	// clock skew.
	query := fmt.Sprintf(`UPDATE %s SET status = 'processed', processed_at = NOW() WHERE id = $1`, r.tableName)
	result, err := r.exec.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("mark processed in %s: %w", r.tableName, err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("outbox entry %s not found in %s", id, r.tableName)
	}
	return nil
}

func (r *postgresRepository) MarkProcessedBatch(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	// One UPDATE for the whole successful subset of a batch instead of one per
	// row. processed_at uses the DB clock (NOW()) so it lines up with the
	// retention sweeper's NOW()-based cutoff. pq.Array binds the UUID slice to
	// the ANY($1) array parameter.
	query := fmt.Sprintf(`UPDATE %s SET status = 'processed', processed_at = NOW() WHERE id = ANY($1)`, r.tableName)
	if _, err := r.exec.ExecContext(ctx, query, pq.Array(ids)); err != nil {
		return fmt.Errorf("mark processed batch in %s: %w", r.tableName, err)
	}
	return nil
}

func (r *postgresRepository) DeleteProcessedOlderThan(ctx context.Context, retention time.Duration, limit int) (int64, error) {
	// Bounded delete: a CTE selects up to limit eligible ids (status='processed'
	// and processed_at older than NOW()-retention) and the outer DELETE removes
	// exactly those. The LIMIT keeps each statement's lock footprint small so a
	// large backlog drains over several iterations without holding a long lock.
	// The retention window is evaluated against the DB clock to avoid host/DB
	// skew. make_interval takes whole seconds from the Go duration.
	query := fmt.Sprintf(`
		WITH expired AS (
			SELECT id FROM %s
			WHERE status = 'processed'
			  AND processed_at < NOW() - make_interval(secs => $1)
			ORDER BY processed_at ASC
			LIMIT $2
		)
		DELETE FROM %s WHERE id IN (SELECT id FROM expired)
	`, r.tableName, r.tableName)
	result, err := r.exec.ExecContext(ctx, query, retention.Seconds(), limit)
	if err != nil {
		return 0, fmt.Errorf("delete processed older than in %s: %w", r.tableName, err)
	}
	n, _ := result.RowsAffected()
	return n, nil
}

func (r *postgresRepository) MarkFailed(ctx context.Context, id uuid.UUID, errorMessage string) error {
	query := fmt.Sprintf(`UPDATE %s SET status = 'failed', error_message = $1 WHERE id = $2`, r.tableName)
	result, err := r.exec.ExecContext(ctx, query, errorMessage, id)
	if err != nil {
		return fmt.Errorf("mark failed in %s: %w", r.tableName, err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("outbox entry %s not found in %s", id, r.tableName)
	}
	return nil
}

func (r *postgresRepository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	query := fmt.Sprintf(`UPDATE %s SET retry_count = retry_count + 1 WHERE id = $1`, r.tableName)
	result, err := r.exec.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("increment retry in %s: %w", r.tableName, err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("outbox entry %s not found in %s", id, r.tableName)
	}
	return nil
}

// CountTerminal returns how many rows are parked in the terminal 'failed' state,
// i.e. dead-lettered rows awaiting operator/consumer attention.
func (r *postgresRepository) CountTerminal(ctx context.Context) (int, error) {
	var n int
	query := fmt.Sprintf(`SELECT count(*) FROM %s WHERE status = 'failed'`, r.tableName)
	if err := r.exec.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("count terminal in %s: %w", r.tableName, err)
	}
	return n, nil
}

// ScheduleRetry records a transient publish failure: it bumps retry_count,
// moves the row to 'scheduled', stamps the next eligible attempt time
// (clock_timestamp() + retryIn), and stores the error for visibility. The
// 'scheduled' status keeps a backed-off row out of any reader that selects only
// `status = 'pending'`, so a co-running replica without this due-gate cannot
// reclaim the row and retry it before next_attempt_at elapses; GetPendingBatch
// re-selects it once due via its status IN ('pending', 'scheduled') clause. The
// deadline is measured against clock_timestamp() — the statement-execution wall
// clock — not NOW(), which is fixed at transaction start: because this runs in
// the same batch transaction as the publish attempts, NOW() would measure the
// backoff from before those attempts ran and could leave the deadline already
// in the past. This matches the due-gate comparison in GetPendingBatch;
// make_interval takes whole seconds from the Go duration.
func (r *postgresRepository) ScheduleRetry(ctx context.Context, id uuid.UUID, retryIn time.Duration, errorMessage string) error {
	query := fmt.Sprintf(
		`UPDATE %s SET status = 'scheduled', retry_count = retry_count + 1, next_attempt_at = clock_timestamp() + make_interval(secs => $1), error_message = $2 WHERE id = $3`,
		r.tableName)
	result, err := r.exec.ExecContext(ctx, query, retryIn.Seconds(), errorMessage, id)
	if err != nil {
		return fmt.Errorf("schedule retry in %s: %w", r.tableName, err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("outbox entry %s not found in %s", id, r.tableName)
	}
	return nil
}
