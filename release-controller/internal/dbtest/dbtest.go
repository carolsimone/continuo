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
	"time"

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

// advisoryLockPollInterval is how often acquireAdvisoryLock retries
// pg_try_advisory_lock while another binary holds the lock.
const advisoryLockPollInterval = 500 * time.Millisecond

// advisoryLockLogEvery bounds how often acquireAdvisoryLock prints a waiting
// message to stderr, so a long wait is visible without spamming a line every
// poll.
const advisoryLockLogEvery = 10 * time.Second

// advisoryLockWaitTimeout bounds how long acquireAdvisoryLock will wait before
// giving up. A leaked lock from a killed test run would otherwise block the
// next run indefinitely with no diagnostic until the outer `go test` timeout.
const advisoryLockWaitTimeout = 3 * time.Minute

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

	if err := acquireAdvisoryLock(db, advisoryLockKey, advisoryLockPollInterval, advisoryLockLogEvery, advisoryLockWaitTimeout); err != nil {
		fmt.Fprintf(os.Stderr, "dbtest: %v\n", err)
		return 1
	}
	defer func() {
		_, _ = db.Exec("SELECT pg_advisory_unlock($1)", advisoryLockKey)
	}()

	return m.Run()
}

// acquireAdvisoryLock blocks until db's single session holds the Postgres
// advisory lock identified by key. It polls pg_try_advisory_lock rather than
// calling the blocking pg_advisory_lock, so a session left over from a killed
// test run produces a periodic, named diagnostic on stderr instead of hanging
// silently until go test's outer 20-minute timeout. It gives up and returns an
// error after timeout, which keeps the same serialization guarantee as a plain
// blocking lock (nothing proceeds until the lock is held) while making a stuck
// wait legible. key, pollInterval, and logEvery are parameters (rather than
// reading the package constants directly) so tests can exercise the retry and
// timeout paths on a private lock key, without waiting the full production
// durations or contending with the real continuo_release lock other test
// binaries may be holding at the same time.
func acquireAdvisoryLock(db *sql.DB, key int64, pollInterval, logEvery, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for {
		var acquired bool
		if err := db.QueryRow("SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
			return fmt.Errorf("acquire advisory lock: %w", err)
		}
		if acquired {
			return nil
		}
		now := time.Now()
		if now.After(deadline) {
			return fmt.Errorf(
				"timed out after %s waiting for the continuo_release advisory lock (key %d); "+
					"another test binary is likely holding it — check for a killed test-go run "+
					"that left a Postgres session open",
				timeout, key)
		}
		if now.Sub(lastLog) >= logEvery {
			fmt.Fprintf(os.Stderr,
				"dbtest: waiting for the continuo_release advisory lock (key %d), held by another test binary...\n",
				key)
			lastLog = now
		}
		time.Sleep(pollInterval)
	}
}
