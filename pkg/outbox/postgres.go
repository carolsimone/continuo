// pkg/outbox/postgres.go
package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync/atomic"
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
		CreatedAt:           r.CreatedAt,
		ProcessedAt:         r.ProcessedAt,
		ErrorMessage:        r.ErrorMessage,
		NextAttemptAt:       r.NextAttemptAt,
	}
}

type postgresRepository struct {
	exec      Executor
	tableName string
	logger    *slog.Logger
}

// newPostgresRepository builds the concrete repository. In-package callers (the
// processor) use it directly to reach ScheduleRetry / CountTerminal, which are
// deliberately NOT on the Repository interface so the ~10 service fakes that
// satisfy pkgoutbox.Repository need no changes.
func newPostgresRepository(exec Executor, tableName string, logger *slog.Logger) *postgresRepository {
	return &postgresRepository{exec: exec, tableName: tableName, logger: logger}
}

// NewPostgresRepository constructs a Repository bound to a specific physical
// table. Pass *sqlx.DB for autocommit operations (the Processor's GetPendingBatch
// holds its own tx) or *sqlx.Tx for transactional writes (the writer's Create
// must run inside the UoW transaction).
func NewPostgresRepository(exec Executor, tableName string, logger *slog.Logger) Repository {
	return newPostgresRepository(exec, tableName, logger)
}

// Create inserts entry. The outbox owns the ordering key: Create always stamps
// CreatedAt from nextCreatedAt, replacing any value the caller set, so the rows
// a process writes are ordered by when it wrote them. Repository describes how
// far publication follows that order across processes, commit order and the
// pipelined publish path.
func (r *postgresRepository) Create(ctx context.Context, entry *Entry) error {
	if entry.ID == uuid.Nil {
		entry.ID = uuid.New()
	}
	entry.CreatedAt = nextCreatedAt(time.Now())
	if entry.Status == "" {
		entry.Status = "pending"
	}

	// max_retries records the budget the row was written under (MaxAttempts);
	// the processor applies MaxAttempts directly and never reads the column.
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
		entry.Status, entry.RetryCount, MaxAttempts, entry.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create outbox entry in %s: %w", r.tableName, err)
	}
	return nil
}

// claimQuery selects up to $1 due rows in creation order and locks them,
// skipping rows another relay holds. It leaves out every row queued behind an
// older row of its aggregate that waits out a retry (scheduled, next attempt
// still ahead), so a long queue behind one retrying row cannot fill the batch
// and hold up every other aggregate in the table. Fresh 'pending' rows have
// next_attempt_at NULL and are always due. clock_timestamp() is the statement's
// own wall clock, not the transaction start. A row is older than another when
// it comes first in (created_at, id) order, the order the claim reads rows in,
// so of two rows with the same created_at the lower id is the older. The ORDER
// BY matches the partial index idx_<table>_claimable (created_at, id), and
// idx_<table>_open_by_aggregate serves the NOT EXISTS probe through its
// created_at bound. FOR UPDATE OF c locks only the claimed rows, never the
// older rows the probe reads.
func claimQuery(table string) string {
	return claimQueryWhere(table, "")
}

// refillClaimQuery is claimQuery for a later claim in the same transaction. It
// also leaves out the rows the transaction already claimed ($2): SKIP LOCKED
// skips only rows other transactions lock, so without this the claim would
// return them again. And it leaves out every aggregate the transaction withheld
// ($3 aggregate types and $4 aggregate ids, pairwise), so that aggregate's
// younger rows are neither returned nor locked. Postgres evaluates both as
// filters on the claim's index scan, the NOT IN as a hashed subplan rather
// than a join, so the claim still reads idx_<table>_claimable in order without
// a sort. aggregate_type and aggregate_id are NOT NULL, so NOT IN has no NULL
// case.
func refillClaimQuery(table string) string {
	return claimQueryWhere(table, `
		  AND c.id <> ALL($2)
		  AND (c.aggregate_type, c.aggregate_id) NOT IN (
		      SELECT w.aggregate_type, w.aggregate_id
		      FROM unnest($3::text[], $4::uuid[]) AS w(aggregate_type, aggregate_id))`)
}

// claimQueryWhere builds the claim with extra appended to its WHERE clause.
func claimQueryWhere(table, extra string) string {
	return fmt.Sprintf(`
		SELECT c.id, c.message_processing_id, c.aggregate_type, c.aggregate_id,
		       c.event_type, c.payload, c.stream_name,
		       c.status, c.retry_count,
		       c.created_at, c.processed_at, c.error_message, c.next_attempt_at
		FROM %[1]s c
		WHERE c.status IN ('pending', 'scheduled')
		  AND (c.next_attempt_at IS NULL OR c.next_attempt_at <= clock_timestamp())
		  AND NOT EXISTS (
		      SELECT 1 FROM %[1]s older
		      WHERE older.aggregate_type = c.aggregate_type
		        AND older.aggregate_id   = c.aggregate_id
		        AND older.status = 'scheduled'
		        AND older.next_attempt_at > clock_timestamp()
		        AND older.created_at <= c.created_at
		        AND (older.created_at, older.id) < (c.created_at, c.id))%[2]s
		ORDER BY c.created_at ASC, c.id ASC
		LIMIT $1
		FOR UPDATE OF c SKIP LOCKED`, table, extra)
}

// maxClaimRounds bounds how many claims one GetPendingBatch runs, so a table
// whose claimable rows are almost all withheld cannot keep a batch claiming.
const maxClaimRounds = 10

// GetPendingBatch claims up to limit due rows and returns those it may publish
// now, so the rows of one aggregate publish in the order they were created.
// The claim already leaves out rows queued behind an older row of their
// aggregate that waits out a retry. A second query in the same transaction
// finds each claimed row whose aggregate still has an older open row (pending
// or scheduled) outside the rows claimed so far — because another relay holds
// it, or because its retry fell due after the claim read it — and withholds
// that row together with every younger row of its aggregate in the batch.
// Withheld rows are left untouched; the transaction's end releases them.
//
// When a claim fills its page but withholding leaves the batch short of limit,
// GetPendingBatch claims again for the remaining room, past the rows it already
// holds and every aggregate it withheld, so rows that are held up elsewhere
// cannot fill the batch and keep it from rows it may publish. It claims at most
// maxClaimRounds times. Each claim's kept rows are appended in claim order. A
// later claim returns no row older than a row of its aggregate an earlier
// claim kept: such a row was then either held by another relay, which withheld
// its aggregate, or waiting out a retry, which kept its younger rows out of the
// claim. So the rows of one aggregate stay in (created_at, id) order, within
// the commit-order limit Repository describes.
func (r *postgresRepository) GetPendingBatch(ctx context.Context, limit int) ([]*Entry, error) {
	batch := []*Entry{}
	var claimedIDs []uuid.UUID
	withheld := map[aggregateKey]bool{}
	for round := 0; round < maxClaimRounds; round++ {
		room := limit - len(batch)
		claimed, err := r.claim(ctx, room, claimedIDs, withheld)
		if err != nil {
			return nil, err
		}
		if len(claimed) == 0 {
			break
		}
		for _, e := range claimed {
			claimedIDs = append(claimedIDs, e.ID)
		}
		blocked, err := r.blockedByOlderSiblings(ctx, claimed, claimedIDs)
		if err != nil {
			return nil, err
		}
		batch = append(batch, withholdBlocked(claimed, blocked, withheld)...)
		if len(claimed) < room || len(batch) == limit {
			break
		}
	}
	return batch, nil
}

// claim runs one claim of up to limit rows. The first claim of a transaction
// (no rows claimed yet) uses claimQuery; later ones use refillClaimQuery to
// leave out claimedIDs and the withheld aggregates.
func (r *postgresRepository) claim(ctx context.Context, limit int, claimedIDs []uuid.UUID, withheld map[aggregateKey]bool) ([]*Entry, error) {
	var rows []*outboxRow
	var err error
	if len(claimedIDs) == 0 {
		err = r.exec.SelectContext(ctx, &rows, claimQuery(r.tableName), limit)
	} else {
		types := make([]string, 0, len(withheld))
		aggregateIDs := make([]uuid.UUID, 0, len(withheld))
		for k := range withheld {
			types = append(types, k.aggregateType)
			aggregateIDs = append(aggregateIDs, k.aggregateID)
		}
		err = r.exec.SelectContext(ctx, &rows, refillClaimQuery(r.tableName), limit,
			pq.Array(claimedIDs), pq.Array(types), pq.Array(aggregateIDs))
	}
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("get pending batch from %s: %w", r.tableName, err)
	}
	entries := make([]*Entry, len(rows))
	for i, row := range rows {
		entries[i] = entryFromRow(row)
	}
	return entries, nil
}

// blockedByOlderSiblings returns the entries that have an open row of their
// aggregate that is older in (created_at, id) order and is not in claimed.
// claimed holds every row the transaction has claimed, entries included.
func (r *postgresRepository) blockedByOlderSiblings(ctx context.Context, entries []*Entry, claimed []uuid.UUID) (map[uuid.UUID]bool, error) {
	ids := make([]uuid.UUID, len(entries))
	for i, e := range entries {
		ids[i] = e.ID
	}
	query := fmt.Sprintf(`
		SELECT c.id FROM %[1]s c
		WHERE c.id = ANY($1)
		  AND EXISTS (
		      SELECT 1 FROM %[1]s older
		      WHERE older.aggregate_type = c.aggregate_type
		        AND older.aggregate_id   = c.aggregate_id
		        AND older.status IN ('pending', 'scheduled')
		        AND older.created_at <= c.created_at
		        AND (older.created_at, older.id) < (c.created_at, c.id)
		        AND NOT (older.id = ANY($2)))`, r.tableName)
	var blockedIDs []uuid.UUID
	if err := r.exec.SelectContext(ctx, &blockedIDs, query, pq.Array(ids), pq.Array(claimed)); err != nil {
		return nil, fmt.Errorf("find withheld rows in %s: %w", r.tableName, err)
	}
	blocked := make(map[uuid.UUID]bool, len(blockedIDs))
	for _, id := range blockedIDs {
		blocked[id] = true
	}
	return blocked, nil
}

type aggregateKey struct {
	aggregateType string
	aggregateID   uuid.UUID
}

// withholdBlocked keeps claim order and drops every blocked row, and every
// later row of a dropped row's aggregate. It records each dropped row's
// aggregate in withheld, and also drops the rows of aggregates withheld already.
func withholdBlocked(entries []*Entry, blocked map[uuid.UUID]bool, withheld map[aggregateKey]bool) []*Entry {
	out := make([]*Entry, 0, len(entries))
	for _, e := range entries {
		k := aggregateKey{e.AggregateType, e.AggregateID}
		if withheld[k] || blocked[e.ID] {
			withheld[k] = true
			continue
		}
		out = append(out, e)
	}
	return out
}

// lastCreatedAt is the latest created_at this process stamped, in Unix
// microseconds.
var lastCreatedAt atomic.Int64

// nextCreatedAt returns now at microsecond precision, which is what Postgres
// stores. When needed it is moved forward so it is strictly later than every
// value this process returned before. Rows one writer creates therefore keep
// their creation order in created_at, which orders their publication.
func nextCreatedAt(now time.Time) time.Time {
	for {
		last := lastCreatedAt.Load()
		next := now.UnixMicro()
		if next <= last {
			next = last + 1
		}
		if lastCreatedAt.CompareAndSwap(last, next) {
			return time.UnixMicro(next).UTC()
		}
	}
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
