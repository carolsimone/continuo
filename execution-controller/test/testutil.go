package test

import (
	"context"
	"sync"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// SetupPostgres connects to an externally migrated executor test database.
// Every executor DB suite shares a session advisory lock, so each test owns
// all rows inserted after its empty-table check. A database containing rows
// is refused rather than clearing data belonging to another process.
func SetupPostgres(t *testing.T) (*sqlx.DB, func()) {
	t.Helper()
	cfg := pkgconfig.LoadPostgres(&pkgconfig.Validator{})
	if cfg.Host == "" {
		testdeps.Unavailable(t, "POSTGRES_HOST not set; configure an empty executor database and run its Flyway migrations")
	}
	db, err := sqlx.Connect("postgres", cfg.DSN())
	if err != nil {
		testdeps.Unavailable(t, "executor postgres unreachable: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	lockConn, err := db.Conn(ctx)
	cancel()
	require.NoError(t, err)
	t.Cleanup(func() {
		_, unlockErr := lockConn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(861248012)`)
		if unlockErr != nil {
			t.Errorf("release executor test lock: %v", unlockErr)
		}
		_ = lockConn.Close()
	})
	// The lock wait gets the full test timeout: advisory-lock waiters are not
	// granted fairly, so a short deadline would flake a serialized suite into
	// a failure when another package's test simply runs long.
	lockCtx, lockCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer lockCancel()
	_, err = lockConn.ExecContext(lockCtx, `SELECT pg_advisory_lock(861248012)`)
	require.NoError(t, err, "serialize executor database tests")

	for _, table := range []string{"execution_outbox", "deployments", "validation_aggregates", "cancelled_schedules", "message_processing"} {
		var present bool
		require.NoError(t, db.Get(&present, `SELECT to_regclass($1) IS NOT NULL`, table))
		require.True(t, present, "%s missing: run Flyway migrations from db/migration/execution before testing", table)
		var count int
		require.NoError(t, db.Get(&count, `SELECT count(*) FROM `+table))
		require.Zero(t, count, "%s contains data: use an empty, dedicated executor test database without running controllers", table)
	}

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanCancel()
			_, cleanErr := db.ExecContext(cleanCtx, `TRUNCATE execution_outbox, deployments, validation_aggregates, cancelled_schedules, message_processing`)
			if cleanErr != nil {
				t.Errorf("clean executor test-owned rows: %v", cleanErr)
			}
		})
	}
	t.Cleanup(cleanup)
	return db, cleanup
}

func setupPostgres(t *testing.T) (*sqlx.DB, func()) {
	return SetupPostgres(t)
}
