//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/service/ports"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slotTenant returns a tenant no other test uses, whose runs are deleted when
// the test ends.
func slotTenant(t *testing.T, db *sqlx.DB) string {
	t.Helper()
	tenant := "slot-" + uuid.NewString()
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runs WHERE tenant_id = $1`, tenant) })
	return tenant
}

// cronRunOf is an unsaved cron run of the tenant's daily schedule, stamped
// with slot.
func cronRunOf(t *testing.T, tenant string, slot time.Time) *run.Run {
	t.Helper()
	rn, err := run.NewPendingRun(tenant, uuid.New(), "daily", run.KindCron, nil, identity.SystemUserID,
		model.OperationRun, run.ReleaseStamp{}, slot)
	require.NoError(t, err)
	require.NoError(t, rn.SetScheduledFor(slot))
	return rn
}

// A cron run is stored with the slot it was fired for, and every load brings
// the slot back; a run of another kind stores none.
func TestRunRepository_ACronRunStoresItsSlot(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := slotTenant(t, db)
	slot := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	u := postgres.NewPostgresUnitOfWork(db, ports.SystemClock{}, discardLogger())
	cronRun := cronRunOf(t, tenant, slot)
	trigger, err := run.NewPendingRun(tenant, uuid.New(), "adhoc", run.KindTrigger, nil, "okta|alice",
		model.OperationRun, run.ReleaseStamp{}, slot)
	require.NoError(t, err)
	saveRuns(t, u, cronRun, trigger)

	var stored *time.Time
	require.NoError(t, db.Get(&stored, `SELECT scheduled_for FROM runs WHERE tenant_id = $1 AND run_id = $2`, tenant, cronRun.RunID()))
	require.NotNil(t, stored)
	assert.True(t, slot.Equal(*stored), "scheduled_for: got %s", stored)

	read, err := u.Run().GetRun(ctx, tenant, cronRun.RunID())
	require.NoError(t, err)
	require.NotNil(t, read.ScheduledFor())
	assert.True(t, slot.Equal(*read.ScheduledFor()))

	other, err := u.Run().GetRun(ctx, tenant, trigger.RunID())
	require.NoError(t, err)
	assert.Nil(t, other.ScheduledFor(), "a triggered run has no slot")
}

// A second run of a slot that already has its run is run.ErrSlotTaken: the
// insert writes nothing, and the transaction it ran in goes on to commit what
// else it wrote.
func TestRunRepository_ASecondRunOfASlotIsSlotTakenAndTheTransactionGoesOn(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tenant := slotTenant(t, db)
	slot := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	first := cronRunOf(t, tenant, slot)
	saveRuns(t, postgres.NewPostgresUnitOfWork(db, ports.SystemClock{}, discardLogger()), first)
	_, err := db.Exec(`UPDATE runs SET status = 'succeeded', completed_at = now() WHERE tenant_id = $1 AND run_id = $2`, tenant, first.RunID())
	require.NoError(t, err)

	u := postgres.NewPostgresUnitOfWork(db, ports.SystemClock{}, discardLogger())
	require.NoError(t, u.Begin(ctx))
	defer func() { _ = u.Rollback() }()
	second := cronRunOf(t, tenant, slot)
	require.ErrorIs(t, u.Run().SaveRun(ctx, second), run.ErrSlotTaken)
	nextSlot := cronRunOf(t, tenant, slot.Add(24*time.Hour))
	require.NoError(t, u.Run().SaveRun(ctx, nextSlot), "the transaction is still usable")
	require.NoError(t, u.Commit())

	var ids []uuid.UUID
	require.NoError(t, db.Select(&ids, `SELECT run_id FROM runs WHERE tenant_id = $1 AND scheduled_for = $2`, tenant, slot))
	assert.Equal(t, []uuid.UUID{first.RunID()}, ids, "the slot keeps its one run")
	var later int
	require.NoError(t, db.Get(&later, `SELECT count(*) FROM runs WHERE tenant_id = $1 AND run_id = $2`, tenant, nextSlot.RunID()))
	assert.Equal(t, 1, later)
}
