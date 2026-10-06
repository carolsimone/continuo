package postgres

import (
	"context"

	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	"github.com/jmoiron/sqlx"
)

// trimLockKey is the advisory lock that lets one replica trim at a time.
const trimLockKey int64 = 0x636f6e74696e7531

// AdvisoryLock is a session-level Postgres advisory lock held on one dedicated
// connection for the duration of a trim run.
type AdvisoryLock struct{ db *sqlx.DB }

var _ ports.TrimLock = (*AdvisoryLock)(nil)

// NewAdvisoryLock returns a lock backed by db.
func NewAdvisoryLock(db *sqlx.DB) *AdvisoryLock { return &AdvisoryLock{db: db} }

// TryAcquire takes the lock without waiting. It returns ok=false when another
// session holds it; otherwise release unlocks it and returns its connection to
// the pool.
func (l *AdvisoryLock) TryAcquire(ctx context.Context) (func(), bool, error) {
	conn, err := l.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, trimLockKey).Scan(&ok); err != nil {
		_ = conn.Close()
		return nil, false, err
	}
	if !ok {
		_ = conn.Close()
		return nil, false, nil
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, trimLockKey)
		_ = conn.Close()
	}, true, nil
}
