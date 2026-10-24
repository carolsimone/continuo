package postgres_test

import (
	"context"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
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
	db := newTestDB(t)
	tenant := "schema-" + uuid.NewString()
	first, second := newSchemaTask(t, db), newSchemaTask(t, db)
	require.NoError(t, insertLease(db, tenant, "raw.orders", first, schemaTaskRun(t, db, first)))
	require.NoError(t, insertLease(db, tenant, "raw.customers", second, schemaTaskRun(t, db, second)))

	listener := pq.NewListener(pkgconfig.LoadPostgres(&pkgconfig.Validator{}).DSN(), 10*time.Millisecond, time.Second, nil)
	t.Cleanup(func() { _ = listener.Close() })
	require.NoError(t, listener.Listen("node_leases"))

	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	_, err = tx.Exec(`DELETE FROM node_leases WHERE tenant_id = $1`, tenant)
	require.NoError(t, err)
	assertNoNotification(t, listener, "before the delete commits")
	require.NoError(t, tx.Commit())

	select {
	case n := <-listener.Notify:
		require.NotNil(t, n)
		assert.Equal(t, "node_leases", n.Channel)
	case <-time.After(5 * time.Second):
		t.Fatal("no notification after the release committed")
	}
	assertNoNotification(t, listener, "after the one notification")
}

// assertNoNotification fails when listener receives a notification within a
// short window.
func assertNoNotification(t *testing.T, listener *pq.Listener, when string) {
	t.Helper()
	select {
	case n := <-listener.Notify:
		if n != nil {
			t.Fatalf("unexpected notification %s: %+v", when, n)
		}
	case <-time.After(300 * time.Millisecond):
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
