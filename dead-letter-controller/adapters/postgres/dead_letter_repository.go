// Package postgres implements dead-letter-controller's persistence ports.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/repository"
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// Executor is what the repository runs statements on: the pool, or one transaction.
type Executor interface {
	sqlx.ExtContext
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

// DeadLetterRepository stores dead letters in the dead_letters table.
type DeadLetterRepository struct{ exec Executor }

var _ repository.DeadLetterRepository = (*DeadLetterRepository)(nil)

// NewDeadLetterRepository returns a repository running on exec, which is either
// a *sqlx.DB or a *sqlx.Tx.
func NewDeadLetterRepository(exec Executor) *DeadLetterRepository {
	return &DeadLetterRepository{exec: exec}
}

const columns = `id, dedup_key, source, failure_kind, stream, consumer_group, original_message_id, producer,
	error, delivery_count, fields, redrivable, original_event_type, failed_outbox_id, original_at, recorded_at,
	status, redriven_by, redrive_reason, redriven_at`

type row struct {
	ID                uuid.UUID      `db:"id"`
	DedupKey          string         `db:"dedup_key"`
	Source            string         `db:"source"`
	FailureKind       string         `db:"failure_kind"`
	Stream            string         `db:"stream"`
	Group             string         `db:"consumer_group"`
	OriginalMessageID string         `db:"original_message_id"`
	Producer          string         `db:"producer"`
	Error             string         `db:"error"`
	DeliveryCount     int64          `db:"delivery_count"`
	Fields            []byte         `db:"fields"`
	Redrivable        bool           `db:"redrivable"`
	OriginalEventType string         `db:"original_event_type"`
	FailedOutboxID    string         `db:"failed_outbox_id"`
	OriginalAt        time.Time      `db:"original_at"`
	RecordedAt        time.Time      `db:"recorded_at"`
	Status            string         `db:"status"`
	RedrivenBy        sql.NullString `db:"redriven_by"`
	RedriveReason     sql.NullString `db:"redrive_reason"`
	RedrivenAt        sql.NullTime   `db:"redriven_at"`
}

func (r row) toDomain() (deadletter.DeadLetter, error) {
	var fields map[string]string
	if err := json.Unmarshal(r.Fields, &fields); err != nil {
		return deadletter.DeadLetter{}, fmt.Errorf("dead letter %s fields: %w", r.ID, err)
	}
	dl := deadletter.DeadLetter{
		ID: r.ID, DedupKey: r.DedupKey, Source: deadletter.Source(r.Source), FailureKind: model.DeadLetterKind(r.FailureKind),
		Stream: r.Stream, Group: r.Group, OriginalMessageID: r.OriginalMessageID, Producer: r.Producer, Error: r.Error,
		DeliveryCount: r.DeliveryCount, Fields: fields, Redrivable: r.Redrivable, OriginalEventType: r.OriginalEventType,
		FailedOutboxID: r.FailedOutboxID, OriginalAt: r.OriginalAt.UTC(), RecordedAt: r.RecordedAt.UTC(),
		Status: deadletter.Status(r.Status),
	}
	if r.RedrivenAt.Valid {
		dl.Redrive = &deadletter.Redrive{Actor: r.RedrivenBy.String, Reason: r.RedriveReason.String, At: r.RedrivenAt.Time.UTC()}
	}
	return dl, nil
}

func toDomainAll(rows []row) ([]deadletter.DeadLetter, error) {
	out := make([]deadletter.DeadLetter, 0, len(rows))
	for _, r := range rows {
		dl, err := r.toDomain()
		if err != nil {
			return nil, err
		}
		out = append(out, dl)
	}
	return out, nil
}

// Insert stores dl unless a row with the same DedupKey exists; it reports
// whether it stored the row.
func (r *DeadLetterRepository) Insert(ctx context.Context, dl deadletter.DeadLetter) (bool, error) {
	fields := dl.Fields
	if fields == nil {
		fields = map[string]string{}
	}
	body, err := json.Marshal(fields)
	if err != nil {
		return false, err
	}
	res, err := r.exec.ExecContext(ctx, `INSERT INTO dead_letters (`+columns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NULL,NULL,NULL)
		ON CONFLICT (dedup_key) DO NOTHING`,
		dl.ID, dl.DedupKey, string(dl.Source), string(dl.FailureKind), dl.Stream, dl.Group, dl.OriginalMessageID,
		dl.Producer, dl.Error, dl.DeliveryCount, string(body), dl.Redrivable, dl.OriginalEventType, dl.FailedOutboxID,
		dl.OriginalAt, dl.RecordedAt, string(dl.Status))
	if err != nil {
		return false, fmt.Errorf("insert dead letter: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("insert dead letter: %w", err)
	}
	return n == 1, nil
}

// Get returns the dead letter with the given id, or deadletter.ErrNotFound.
func (r *DeadLetterRepository) Get(ctx context.Context, id uuid.UUID) (deadletter.DeadLetter, error) {
	var rw row
	err := r.exec.GetContext(ctx, &rw, `SELECT `+columns+` FROM dead_letters WHERE id = $1`, id)
	if errors.Is(err, sql.ErrNoRows) {
		return deadletter.DeadLetter{}, deadletter.ErrNotFound
	}
	if err != nil {
		return deadletter.DeadLetter{}, fmt.Errorf("get dead letter: %w", err)
	}
	return rw.toDomain()
}

// List returns the dead letters matching a normalized filter, newest first.
func (r *DeadLetterRepository) List(ctx context.Context, f deadletter.Filter) ([]deadletter.DeadLetter, error) {
	var rows []row
	err := r.exec.SelectContext(ctx, &rows, `SELECT `+columns+` FROM dead_letters
		WHERE status = $1 AND ($2 = '' OR source = $2) AND ($3 = '' OR stream = $3)
		ORDER BY recorded_at DESC, id LIMIT $4`, string(f.Status), string(f.Source), f.Stream, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	return toDomainAll(rows)
}

// CountOpen returns how many dead letters are open.
func (r *DeadLetterRepository) CountOpen(ctx context.Context) (int64, error) {
	var n int64
	if err := r.exec.GetContext(ctx, &n, `SELECT count(*) FROM dead_letters WHERE status = 'open'`); err != nil {
		return 0, fmt.Errorf("count open dead letters: %w", err)
	}
	return n, nil
}

// LockForRedrive returns the existing rows among ids, locked until the
// surrounding transaction ends.
func (r *DeadLetterRepository) LockForRedrive(ctx context.Context, ids []uuid.UUID) ([]deadletter.DeadLetter, error) {
	var rows []row
	err := r.exec.SelectContext(ctx, &rows, `SELECT `+columns+` FROM dead_letters
		WHERE id = ANY($1::uuid[]) ORDER BY id FOR UPDATE`, pq.Array(uuidStrings(ids)))
	if err != nil {
		return nil, fmt.Errorf("lock dead letters: %w", err)
	}
	return toDomainAll(rows)
}

// SaveRedrive persists the status and redrive record of a redriven dead letter.
func (r *DeadLetterRepository) SaveRedrive(ctx context.Context, dl deadletter.DeadLetter) error {
	if dl.Redrive == nil {
		return fmt.Errorf("dead letter %s has no redrive to save", dl.ID)
	}
	if _, err := r.exec.ExecContext(ctx, `UPDATE dead_letters
		SET status = $2, redriven_by = $3, redrive_reason = $4, redriven_at = $5 WHERE id = $1`,
		dl.ID, string(dl.Status), dl.Redrive.Actor, dl.Redrive.Reason, dl.Redrive.At); err != nil {
		return fmt.Errorf("save redrive: %w", err)
	}
	return nil
}

// DeleteExpired removes up to limit rows whose original message is at or before
// originalBefore and returns them. The cutoff instant itself counts as expired,
// matching the domain's redrive horizon check.
func (r *DeadLetterRepository) DeleteExpired(ctx context.Context, originalBefore time.Time, limit int) ([]deadletter.DeadLetter, error) {
	var rows []row
	err := r.exec.SelectContext(ctx, &rows, `DELETE FROM dead_letters WHERE id IN (
		SELECT id FROM dead_letters WHERE original_at <= $1 ORDER BY original_at LIMIT $2)
		RETURNING `+columns, originalBefore, limit)
	if err != nil {
		return nil, fmt.Errorf("delete expired dead letters: %w", err)
	}
	return toDomainAll(rows)
}

// Backlog counts the open dead letters per source, stream and failure kind, with
// the oldest recorded_at of each group.
func (r *DeadLetterRepository) Backlog(ctx context.Context) ([]deadletter.BacklogRow, error) {
	var rows []struct {
		Source string    `db:"source"`
		Stream string    `db:"stream"`
		Kind   string    `db:"failure_kind"`
		Open   int64     `db:"open"`
		Oldest time.Time `db:"oldest"`
	}
	err := r.exec.SelectContext(ctx, &rows, `SELECT source, stream, failure_kind, count(*) AS open, min(recorded_at) AS oldest
		FROM dead_letters WHERE status = 'open' GROUP BY source, stream, failure_kind`)
	if err != nil {
		return nil, fmt.Errorf("dead letter backlog: %w", err)
	}
	out := make([]deadletter.BacklogRow, 0, len(rows))
	for _, x := range rows {
		out = append(out, deadletter.BacklogRow{Source: deadletter.Source(x.Source), Stream: x.Stream,
			Kind: model.DeadLetterKind(x.Kind), Open: x.Open, OldestRecordedAt: x.Oldest.UTC()})
	}
	return out, nil
}

func uuidStrings(ids []uuid.UUID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}
