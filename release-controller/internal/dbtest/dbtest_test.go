//go:build integration

package dbtest

import (
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLockKey is a private advisory lock key distinct from the package's real
// advisoryLockKey, so these tests contend only with each other and never with
// a concurrently running adapters/postgres or integration_test binary holding
// the real continuo_release lock.
const testLockKey = 8641975321

// openSingleSession opens its own *sql.DB pinned to one connection, so it
// gets its own Postgres session distinct from any other db opened this way —
// exactly how RunSerialized isolates a whole test binary onto one session.
func openSingleSession(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	return db
}

// TestAcquireAdvisoryLockSerializesConcurrentSessions proves the switch from
// the blocking pg_advisory_lock to a polling pg_try_advisory_lock loop keeps
// the same mutual-exclusion guarantee: many sessions contending for the lock
// at once must never both consider themselves inside the critical section.
// Five concurrent sessions mirrors the manual verification done when this
// change was written (5/5 clean runs on a previously-flaky invocation).
func TestAcquireAdvisoryLockSerializesConcurrentSessions(t *testing.T) {
	dsn := DSN()
	if dsn == "" {
		t.Skip("RELEASE_TEST_PG_DSN not set; skipping DB-backed test")
	}

	const sessions = 5
	var inCriticalSection int32
	var overlapDetected int32

	var wg sync.WaitGroup
	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db := openSingleSession(t, dsn)

			if err := acquireAdvisoryLock(db, testLockKey, 5*time.Millisecond, time.Hour, 10*time.Second); err != nil {
				t.Errorf("acquire advisory lock: %v", err)
				return
			}
			defer func() { _, _ = db.Exec("SELECT pg_advisory_unlock($1)", testLockKey) }()

			if !atomic.CompareAndSwapInt32(&inCriticalSection, 0, 1) {
				atomic.StoreInt32(&overlapDetected, 1)
			}
			time.Sleep(50 * time.Millisecond) // simulate the work the lock protects
			atomic.StoreInt32(&inCriticalSection, 0)
		}()
	}
	wg.Wait()

	assert.Zero(t, atomic.LoadInt32(&overlapDetected),
		"two sessions held the advisory lock at the same time")
}

// TestAcquireAdvisoryLockWaitsThenAcquiresOnceReleased proves a session that
// finds the lock held does not proceed early: it must keep retrying and only
// return once the holder releases it.
func TestAcquireAdvisoryLockWaitsThenAcquiresOnceReleased(t *testing.T) {
	dsn := DSN()
	if dsn == "" {
		t.Skip("RELEASE_TEST_PG_DSN not set; skipping DB-backed test")
	}

	holder := openSingleSession(t, dsn)
	require.NoError(t, acquireAdvisoryLock(holder, testLockKey, 5*time.Millisecond, time.Hour, 5*time.Second))

	waiter := openSingleSession(t, dsn)
	acquired := make(chan error, 1)
	go func() {
		acquired <- acquireAdvisoryLock(waiter, testLockKey, 20*time.Millisecond, time.Hour, 5*time.Second)
	}()

	select {
	case err := <-acquired:
		t.Fatalf("waiter acquired the lock while the holder still held it (err=%v)", err)
	case <-time.After(200 * time.Millisecond):
		// Still blocked, as expected.
	}

	_, err := holder.Exec("SELECT pg_advisory_unlock($1)", testLockKey)
	require.NoError(t, err)

	select {
	case err := <-acquired:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("waiter never acquired the lock after the holder released it")
	}
	_, _ = waiter.Exec("SELECT pg_advisory_unlock($1)", testLockKey)
}

// TestAcquireAdvisoryLockTimesOutWithDiagnostic proves a leaked lock produces
// a bounded, legible failure instead of hanging forever — the point of this
// fix. The error must name the advisory lock so a stuck run is diagnosable.
func TestAcquireAdvisoryLockTimesOutWithDiagnostic(t *testing.T) {
	dsn := DSN()
	if dsn == "" {
		t.Skip("RELEASE_TEST_PG_DSN not set; skipping DB-backed test")
	}

	holder := openSingleSession(t, dsn)
	require.NoError(t, acquireAdvisoryLock(holder, testLockKey, 5*time.Millisecond, time.Hour, 5*time.Second))
	defer func() { _, _ = holder.Exec("SELECT pg_advisory_unlock($1)", testLockKey) }()

	waiter := openSingleSession(t, dsn)
	err := acquireAdvisoryLock(waiter, testLockKey, 20*time.Millisecond, time.Hour, 150*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
	assert.Contains(t, err.Error(), "advisory lock")
}
