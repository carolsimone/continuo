package postgres_test

import (
	"context"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schemaTaskRun returns the run a task made by newSchemaTask belongs to.
func schemaTaskRun(t *testing.T, db *sqlx.DB, taskID uuid.UUID) uuid.UUID {
	t.Helper()
	var runID uuid.UUID
	require.NoError(t, db.Get(&runID, `SELECT schedule_id FROM task_tracker WHERE task_id = $1`, taskID))
	return runID
}

// insertLease takes the lease on a node for a task of a run.
func insertLease(db *sqlx.DB, tenant, nodeKey string, taskID, runID uuid.UUID) error {
	_, err := db.Exec(`
		INSERT INTO node_leases (tenant_id, node_key, task_id, run_id)
		VALUES ($1, $2, $3, $4)`, tenant, nodeKey, taskID, runID)
	return err
}

// A node has at most one holder: a second task cannot take the lease on a node
// another task holds, and can once the first lease is gone.
func TestNodeLeasesSchema_OneLeasePerNode(t *testing.T) {
	db := newTestDB(t)
	tenant := "schema-" + uuid.NewString()
	first, second := newSchemaTask(t, db), newSchemaTask(t, db)
	require.NoError(t, insertLease(db, tenant, "raw.orders", first, schemaTaskRun(t, db, first)))

	err := insertLease(db, tenant, "raw.orders", second, schemaTaskRun(t, db, second))
	require.Error(t, err)
	assert.Equal(t, "23505", pqCode(t, err))

	assert.NoError(t, insertLease(db, tenant, "raw.customers", second, schemaTaskRun(t, db, second)),
		"a different node is free")

	_, err = db.Exec(`DELETE FROM node_leases WHERE tenant_id = $1 AND node_key = 'raw.orders'`, tenant)
	require.NoError(t, err)
	_, err = db.Exec(`DELETE FROM node_leases WHERE task_id = $1`, second)
	require.NoError(t, err)
	assert.NoError(t, insertLease(db, tenant, "raw.orders", second, schemaTaskRun(t, db, second)),
		"the node is free once its lease is released")
}

// A task holds at most one lease: it cannot take a second node.
func TestNodeLeasesSchema_ATaskHoldsOneLease(t *testing.T) {
	db := newTestDB(t)
	tenant := "schema-" + uuid.NewString()
	taskID := newSchemaTask(t, db)
	runID := schemaTaskRun(t, db, taskID)
	require.NoError(t, insertLease(db, tenant, "raw.orders", taskID, runID))

	err := insertLease(db, tenant, "raw.customers", taskID, runID)
	require.Error(t, err)
	assert.Equal(t, "23505", pqCode(t, err))
}

// Deleting a task releases its lease.
func TestNodeLeasesSchema_DeletingATaskReleasesItsLease(t *testing.T) {
	db := newTestDB(t)
	tenant := "schema-" + uuid.NewString()
	taskID := newSchemaTask(t, db)
	require.NoError(t, insertLease(db, tenant, "raw.orders", taskID, schemaTaskRun(t, db, taskID)))

	_, err := db.Exec(`DELETE FROM task_tracker WHERE task_id = $1`, taskID)
	require.NoError(t, err)

	var n int
	require.NoError(t, db.Get(&n, `SELECT count(*) FROM node_leases WHERE tenant_id = $1`, tenant))
	assert.Zero(t, n)
}

// Releasing leases notifies the node_leases channel once per statement, after
// the transaction commits, however many leases the statement deletes.
func TestNodeLeasesSchema_ReleaseNotifies(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := "schema-" + uuid.NewString()
	first, second := newSchemaTask(t, db), newSchemaTask(t, db)
	require.NoError(t, insertLease(db, tenant, "raw.orders", first, schemaTaskRun(t, db, first)))
	require.NoError(t, insertLease(db, tenant, "raw.customers", second, schemaTaskRun(t, db, second)))
	listener := listenForReleases(t)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	var pid int
	require.NoError(t, tx.GetContext(ctx, &pid, `SELECT pg_backend_pid()`))
	_, err = tx.Exec(`DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
	require.NoError(t, err)
	assertNoNotificationFrom(t, listener, pid, "before the delete commits")
	require.NoError(t, tx.Commit())

	awaitNotificationFrom(t, listener, pid, "after the release committed")
	assertNoNotificationFrom(t, listener, pid, "after the one notification")
}

// A statement that deletes no lease sends no notification, so only a release
// wakes the tasks that wait. A delete of no row and then a release commit on
// one connection; notifications arrive in commit order, so the release's is
// the only one that connection sends.
func TestNodeLeasesSchema_ADeleteOfNoRowDoesNotNotify(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := "schema-" + uuid.NewString()
	taskID := newSchemaTask(t, db)
	require.NoError(t, insertLease(db, tenant, "raw.orders", taskID, schemaTaskRun(t, db, taskID)))
	listener := listenForReleases(t)
	conn, err := db.Connx(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var pid int
	require.NoError(t, conn.GetContext(ctx, &pid, `SELECT pg_backend_pid()`))
	deleteAndCommit := func(nodeKey string) int64 {
		tx, err := conn.BeginTxx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		res, err := tx.Exec(`DELETE FROM node_leases WHERE tenant_id = $1 AND node_key = $2`, tenant, nodeKey)
		require.NoError(t, err)
		n, err := res.RowsAffected()
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		return n
	}

	require.Zero(t, deleteAndCommit("raw.absent"), "the first delete matches no lease")
	require.Equal(t, int64(1), deleteAndCommit("raw.orders"), "the second releases one")

	awaitNotificationFrom(t, listener, pid, "after the release committed")
	assertNoNotificationFrom(t, listener, pid, "besides the release's")
}

// listenForReleases returns a listener on NodeLeasesChannel, the channel the
// lease waker listens on, closed when the test ends. The trigger's notification
// arriving there pins the channel name to the trigger's pg_notify.
func listenForReleases(t *testing.T) *pq.Listener {
	t.Helper()
	listener := pq.NewListener(pkgconfig.LoadPostgres(&pkgconfig.Validator{}).DSN(), 10*time.Millisecond, time.Second, nil)
	t.Cleanup(func() { _ = listener.Close() })
	require.NoError(t, listener.Listen(postgres.NodeLeasesChannel))
	return listener
}

// awaitNotificationFrom fails unless listener receives a node_leases
// notification sent by the backend pid within five seconds. Notifications
// other backends send, as tests of other packages release leases on the same
// database, are passed over.
func awaitNotificationFrom(t *testing.T, listener *pq.Listener, pid int, when string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case n := <-listener.Notify:
			if n != nil && n.BePid == pid {
				assert.Equal(t, postgres.NodeLeasesChannel, n.Channel)
				return
			}
		case <-deadline:
			t.Fatalf("no notification %s", when)
		}
	}
}

// assertNoNotificationFrom fails when listener receives, within a short
// window, a notification sent by the backend pid; notifications other backends
// send are passed over.
func assertNoNotificationFrom(t *testing.T, listener *pq.Listener, pid int, when string) {
	t.Helper()
	window := time.After(300 * time.Millisecond)
	for {
		select {
		case n := <-listener.Notify:
			if n != nil && n.BePid == pid {
				t.Fatalf("unexpected notification %s: %+v", when, n)
			}
		case <-window:
			return
		}
	}
}

// A task waits for exactly one thing, a node lease, or for nothing.
func TestTaskTrackerSchema_WaitReasonIsNodeLeaseOrNull(t *testing.T) {
	db := newTestDB(t)
	taskID := newSchemaTask(t, db)

	var reason *string
	require.NoError(t, db.Get(&reason, `SELECT wait_reason FROM task_tracker WHERE task_id = $1`, taskID))
	assert.Nil(t, reason, "a task waits for nothing until it is made to wait")

	setReason := func(r any) error {
		_, err := db.Exec(`UPDATE task_tracker SET wait_reason = $2, lease_wait_since = now() WHERE task_id = $1`, taskID, r)
		return err
	}
	for _, r := range run.WaitReasons() {
		assert.NoError(t, setReason(string(r)), "reason %s", r)
	}
	assert.NoError(t, setReason(nil))

	err := setReason("capacity")
	require.Error(t, err)
	assert.Equal(t, "23514", pqCode(t, err))
}
