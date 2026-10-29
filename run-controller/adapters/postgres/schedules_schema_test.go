package postgres_test

import (
	"context"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schedulesChannel is the Postgres channel the statement trigger of schedules
// notifies.
const schedulesChannel = "schedules"

// insertSchedule stores a schedule that fires every minute in UTC.
func insertSchedule(db *sqlx.DB, tenant, name string) error {
	_, err := db.Exec(`
		INSERT INTO schedules (tenant_id, name, cron, timezone, next_fire_at, updated_by)
		VALUES ($1, $2, '* * * * *', 'UTC', now(), 'schema-test')`, tenant, name)
	return err
}

// schemaTenantWithSchedules returns a tenant of its own whose schedules the
// test deletes when it ends.
func schemaTenantWithSchedules(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	tenant := "schema-" + uuid.NewString()
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM schedules WHERE tenant_id = $1`, tenant) })
	return tenant
}

// A schedule is one row per tenant and name: the same name is free in another
// tenant, and a tenant may hold another name.
func TestSchedulesSchema_OneRowPerTenantAndName(t *testing.T) {
	db := newTestDB(t)
	tenant, other := schemaTenantWithSchedules(t, db), schemaTenantWithSchedules(t, db)
	require.NoError(t, insertSchedule(db, tenant, "nightly"))

	err := insertSchedule(db, tenant, "nightly")
	require.Error(t, err)
	assert.Equal(t, "23505", pqCode(t, err))

	assert.NoError(t, insertSchedule(db, tenant, "hourly"), "another name of the same tenant")
	assert.NoError(t, insertSchedule(db, other, "nightly"), "the same name of another tenant")
}

// A schedule row starts at version 1 with an empty description, unpaused, and
// has no next fire while it is paused; the columns the caller must name are
// required.
func TestSchedulesSchema_Defaults(t *testing.T) {
	db := newTestDB(t)
	tenant := schemaTenantWithSchedules(t, db)
	_, err := db.Exec(`
		INSERT INTO schedules (tenant_id, name, cron, timezone, updated_by)
		VALUES ($1, 'nightly', '0 2 * * *', 'Europe/Rome', 'schema-test')`, tenant)
	require.NoError(t, err)

	var got struct {
		Description string     `db:"description"`
		Paused      bool       `db:"paused"`
		NextFireAt  *time.Time `db:"next_fire_at"`
		Version     int64      `db:"version"`
		UpdatedAt   time.Time  `db:"updated_at"`
	}
	require.NoError(t, db.Get(&got, `
		SELECT description, paused, next_fire_at, version, updated_at
		  FROM schedules WHERE tenant_id = $1 AND name = 'nightly'`, tenant))
	assert.Empty(t, got.Description)
	assert.False(t, got.Paused)
	assert.Nil(t, got.NextFireAt)
	assert.Equal(t, int64(1), got.Version)
	assert.WithinDuration(t, time.Now(), got.UpdatedAt, time.Minute)

	_, err = db.Exec(`INSERT INTO schedules (tenant_id, name, timezone, updated_by) VALUES ($1, 'no-cron', 'UTC', 'schema-test')`, tenant)
	require.Error(t, err, "a schedule needs its cron expression")
	assert.Equal(t, "23502", pqCode(t, err))
}

// A committed change to schedules notifies the schedules channel once, however
// many rows the statement changes, and a change that rolls back notifies no
// one.
func TestSchedulesSchema_AChangeNotifies(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := schemaTenantWithSchedules(t, db)
	require.NoError(t, insertSchedule(db, tenant, "nightly"))
	require.NoError(t, insertSchedule(db, tenant, "hourly"))
	listener := listenForSchedules(t)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	var pid int
	require.NoError(t, tx.GetContext(ctx, &pid, `SELECT pg_backend_pid()`))
	_, err = tx.Exec(`UPDATE schedules SET paused = true, next_fire_at = NULL WHERE tenant_id = $1`, tenant)
	require.NoError(t, err)
	assertNoScheduleNotificationFrom(t, listener, pid, "before the update commits")
	require.NoError(t, tx.Commit())

	awaitScheduleNotificationFrom(t, listener, pid, "after the update committed")
	assertNoScheduleNotificationFrom(t, listener, pid, "after the one notification")
}

// An insert and a delete of a schedule notify the schedules channel too, and
// an update that rolls back does not.
func TestSchedulesSchema_AnInsertAndADeleteNotify(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := schemaTenantWithSchedules(t, db)
	listener := listenForSchedules(t)
	conn, err := db.Connx(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var pid int
	require.NoError(t, conn.GetContext(ctx, &pid, `SELECT pg_backend_pid()`))

	_, err = conn.ExecContext(ctx, `
		INSERT INTO schedules (tenant_id, name, cron, timezone, updated_by)
		VALUES ($1, 'nightly', '0 2 * * *', 'UTC', 'schema-test')`, tenant)
	require.NoError(t, err)
	awaitScheduleNotificationFrom(t, listener, pid, "after the insert")
	assertNoScheduleNotificationFrom(t, listener, pid, "after the insert's notification")

	tx, err := conn.BeginTxx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.Exec(`UPDATE schedules SET paused = true WHERE tenant_id = $1`, tenant)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	assertNoScheduleNotificationFrom(t, listener, pid, "after an update that rolled back")

	_, err = conn.ExecContext(ctx, `DELETE FROM schedules WHERE tenant_id = $1`, tenant)
	require.NoError(t, err)
	awaitScheduleNotificationFrom(t, listener, pid, "after the delete")
}

// A schedule's slot, the instant a fire was due, runs once: two runs of one
// schedule of a tenant cannot share a slot, while the same slot of another
// schedule or tenant, another slot, and runs that belong to no slot are free.
// The runs are finished, so the one-active-run index does not answer for the
// refusal.
func TestRunsSchema_OneRunPerScheduledSlot(t *testing.T) {
	db := newTestDB(t)
	tenant, other := "schema-"+uuid.NewString(), "schema-"+uuid.NewString()
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE tenant_id IN ($1, $2)`, tenant, other) })
	slot := time.Date(2026, 10, 28, 2, 0, 0, 0, time.UTC)
	insertRun := func(tenantID, schedule string, scheduledFor any) error {
		_, err := db.Exec(`
			INSERT INTO runs (tenant_id, run_id, schedule_name, status, scheduled_for)
			VALUES ($1, $2, $3, 'succeeded', $4)`, tenantID, uuid.New(), schedule, scheduledFor)
		return err
	}
	require.NoError(t, insertRun(tenant, "nightly", slot))

	err := insertRun(tenant, "nightly", slot)
	require.Error(t, err)
	assert.Equal(t, "23505", pqCode(t, err))
	var pqErr *pq.Error
	require.ErrorAs(t, err, &pqErr)
	assert.Equal(t, "uq_runs_scheduled_for", pqErr.Constraint)

	assert.NoError(t, insertRun(tenant, "nightly", slot.Add(24*time.Hour)), "another slot of the schedule")
	assert.NoError(t, insertRun(tenant, "hourly", slot), "the slot of another schedule")
	assert.NoError(t, insertRun(other, "nightly", slot), "the slot of another tenant")
	assert.NoError(t, insertRun(tenant, "nightly", nil), "a run that belongs to no slot")
	assert.NoError(t, insertRun(tenant, "nightly", nil), "a second run that belongs to no slot")
}

// listenForSchedules returns a listener on the schedules channel, closed when
// the test ends.
func listenForSchedules(t *testing.T) *pq.Listener {
	t.Helper()
	listener := pq.NewListener(pkgconfig.LoadPostgres(&pkgconfig.Validator{}).DSN(), 10*time.Millisecond, time.Second, nil)
	t.Cleanup(func() { _ = listener.Close() })
	require.NoError(t, listener.Listen(schedulesChannel))
	return listener
}

// awaitScheduleNotificationFrom fails unless listener receives a schedules
// notification sent by the backend pid within five seconds. Notifications
// other backends send, as tests of other packages change schedules on the same
// database, are passed over.
func awaitScheduleNotificationFrom(t *testing.T, listener *pq.Listener, pid int, when string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case n := <-listener.Notify:
			if n != nil && n.BePid == pid {
				assert.Equal(t, schedulesChannel, n.Channel)
				return
			}
		case <-deadline:
			t.Fatalf("no notification %s", when)
		}
	}
}

// assertNoScheduleNotificationFrom fails when listener receives, within a short
// window, a notification sent by the backend pid; notifications other backends
// send are passed over.
func assertNoScheduleNotificationFrom(t *testing.T, listener *pq.Listener, pid int, when string) {
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

// A schedule's version counts the operator's edits from 1, so a row at version
// 0 or below is refused: it would only ever be deleted and could not be edited.
func TestSchedulesSchema_VersionStartsAtOne(t *testing.T) {
	db := newTestDB(t)
	tenant := schemaTenantWithSchedules(t, db)
	insertAt := func(name string, version int64) error {
		_, err := db.Exec(`
			INSERT INTO schedules (tenant_id, name, cron, timezone, next_fire_at, version, updated_by)
			VALUES ($1, $2, '* * * * *', 'UTC', now(), $3, 'schema-test')`, tenant, name, version)
		return err
	}

	assert.NoError(t, insertAt("first", 1))
	for _, version := range []int64{0, -1} {
		err := insertAt("refused", version)
		require.Error(t, err, "version %d", version)
		assert.Equal(t, "23514", pqCode(t, err), "check_violation for version %d", version)
	}

	_, err := db.Exec(`UPDATE schedules SET version = 0 WHERE tenant_id = $1 AND name = 'first'`, tenant)
	require.Error(t, err, "an edit cannot take a row below version 1")
	assert.Equal(t, "23514", pqCode(t, err))
}
