package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/state/domain/commandrequest"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// CommandRequestRepository implements repository.CommandRequestRepository over
// command_requests. Each method is one statement, which runs in the unit of
// work's transaction when one is open, else on the autocommit db.
type CommandRequestRepository struct {
	db *sqlx.DB
	tx *sqlx.Tx
}

var _ repository.CommandRequestRepository = (*CommandRequestRepository)(nil)

// NewCommandRequestRepository binds the repository to db and tx (nil outside a
// transaction).
func NewCommandRequestRepository(db *sqlx.DB, tx *sqlx.Tx) *CommandRequestRepository {
	return &CommandRequestRepository{db: db, tx: tx}
}

func (r *CommandRequestRepository) conn() sqlx.ExtContext {
	if r.tx != nil {
		return r.tx
	}
	return r.db
}

type commandRequestRow struct {
	TenantID    string    `db:"tenant_id"`
	RequestID   uuid.UUID `db:"request_id"`
	Command     string    `db:"command"`
	Fingerprint string    `db:"fingerprint"`
	RunID       uuid.UUID `db:"run_id"`
	CreatedAt   time.Time `db:"created_at"`
}

// Find reads the tenant's committed record of requestID. Inside a transaction
// it also sees the records that transaction claimed.
func (r *CommandRequestRepository) Find(ctx context.Context, tenantID string, requestID uuid.UUID) (commandrequest.Record, bool, error) {
	var row commandRequestRow
	err := sqlx.GetContext(ctx, r.conn(), &row, `
		SELECT tenant_id, request_id, command, fingerprint, run_id, created_at
		FROM command_requests
		WHERE tenant_id = $1 AND request_id = $2`, tenantID, requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return commandrequest.Record{}, false, nil
	}
	if err != nil {
		return commandrequest.Record{}, false, fmt.Errorf("find command request %s of tenant %s: %w", requestID, tenantID, err)
	}
	return commandrequest.Record{
		TenantID: row.TenantID, RequestID: row.RequestID, Command: row.Command,
		Fingerprint: row.Fingerprint, RunID: row.RunID, CreatedAt: row.CreatedAt,
	}, true, nil
}

// Claim inserts rec unless the tenant has a record under its request id. The
// insert waits on an uncommitted record with the same key, so two
// transactions claiming one id serialise: the second inserts nothing when the
// first commits, and inserts its row when the first rolls back.
func (r *CommandRequestRepository) Claim(ctx context.Context, rec commandrequest.Record) (bool, error) {
	res, err := r.conn().ExecContext(ctx, `
		INSERT INTO command_requests (tenant_id, request_id, command, fingerprint, run_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_id, request_id) DO NOTHING`,
		rec.TenantID, rec.RequestID, rec.Command, rec.Fingerprint, rec.RunID, rec.CreatedAt)
	if err != nil {
		return false, fmt.Errorf("claim command request %s of tenant %s: %w", rec.RequestID, rec.TenantID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("count the command request claimed: %w", err)
	}
	return n == 1, nil
}

// DeleteOlderThan deletes the records created more than age ago.
func (r *CommandRequestRepository) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	res, err := r.conn().ExecContext(ctx,
		`DELETE FROM command_requests WHERE created_at < now() - make_interval(secs => $1)`, age.Seconds())
	if err != nil {
		return 0, fmt.Errorf("delete the command requests older than %s: %w", age, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count the command requests deleted: %w", err)
	}
	return n, nil
}
