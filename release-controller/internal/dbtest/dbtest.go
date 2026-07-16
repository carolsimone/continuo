// Package dbtest holds shared setup for release-controller's DB-backed tests.
// Those tests live in two packages (adapters/postgres and integration_test) but
// run against the single continuo_release database, so the pieces that must
// agree across both — the DSN environment variable and the advisory-lock key —
// are defined here once.
package dbtest

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

// DSNEnv names the environment variable carrying the DSN of the Postgres
// database the DB-backed tests run against. The Makefile's test-go target
// derives it from the same connection parameters it exports as POSTGRES_*.
const DSNEnv = "RELEASE_TEST_PG_DSN"

// advisoryLockKey serialises the test binaries that share the continuo_release
// database. Both DB-backed packages TRUNCATE the same tables on setup, and
// `go test` runs separate packages in parallel, so without a lock they delete
// each other's rows mid-test. The value is arbitrary; it only has to be the
// same in every binary contending for this database.
const advisoryLockKey = 8641975320

// DSN returns the configured DSN, or the empty string when unset.
func DSN() string { return os.Getenv(DSNEnv) }

// RunSerialized runs m while holding a Postgres advisory lock, so that only one
// test binary at a time touches the shared database, and returns m's exit code.
// With the DSN unset it just runs m: the tests skip themselves individually.
//
// Call it from TestMain: os.Exit(dbtest.RunSerialized(m)).
func RunSerialized(m *testing.M) int {
	dsn := DSN()
	if dsn == "" {
		return m.Run()
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dbtest: open %s: %v\n", DSNEnv, err)
		return 1
	}
	defer func() { _ = db.Close() }()

	// A single connection keeps the session-scoped lock on one session for the
	// binary's lifetime instead of handing it back to the pool.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec("SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		fmt.Fprintf(os.Stderr, "dbtest: acquire advisory lock: %v\n", err)
		return 1
	}
	defer func() {
		_, _ = db.Exec("SELECT pg_advisory_unlock($1)", advisoryLockKey)
	}()

	return m.Run()
}
