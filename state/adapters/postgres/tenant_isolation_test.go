//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/state/domain/repository"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/carolsimone/continuo/state/service/attemptreconcile"
	"github.com/carolsimone/continuo/state/service/leasewaker"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// acmeTenant is a second tenant beside the install's default one.
const acmeTenant = "acme"

// tenantRun stores a run of tenant on release rel-1 with status and returns
// its id; the run, and through it its tasks and their leases, is deleted when
// the test ends.
func tenantRun(t *testing.T, db *sqlx.DB, tenant, status string) uuid.UUID {
	t.Helper()
	return tenantRunOn(t, db, tenant, "rel-1", status)
}

// tenantRunOn stores a run of tenant on releaseID with status and returns its
// id; the run, its outbox rows, and through it its tasks and their leases, are
// deleted when the test ends.
func tenantRunOn(t *testing.T, db *sqlx.DB, tenant, releaseID, status string) uuid.UUID {
	t.Helper()
	runID := uuid.New()
	_, err := db.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status, release_id, promotion_seq)
		VALUES ($1, $2, $3, $4, $5, 1)`, tenant, runID, "tenants-"+runID.String()[:8], status, releaseID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM state_outbox WHERE aggregate_id = $1`, runID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM runs WHERE tenant_id = $1 AND run_id = $2`, tenant, runID)
	})
	return runID
}

// tenantRelease stores, for tenant, a release of its own holding one dbt
// model of schema e2e_schema per table, and returns its id. It leaves the
// tenant's live release as it is, and the release is deleted when the test
// ends.
func tenantRelease(t *testing.T, db *sqlx.DB, tenant string, tables ...string) string {
	t.Helper()
	releaseID := "rel-" + uuid.NewString()[:8]
	specs := make([]topology.NodeSpec, 0, len(tables))
	for _, table := range tables {
		specs = append(specs, topology.NodeSpec{
			UniqueID: "e2e_schema." + table, ServiceName: "service-1", SchemaName: "e2e_schema", TableName: table,
			NodeType: "dbt-model", ContentHash: "h-" + table, ImageTag: "service-1:5", ScheduleName: table,
		})
	}
	rel, err := topology.NewRelease(tenant, releaseID, 1, "s3://continuo/k", "sha-"+releaseID, specs)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM topology_releases WHERE tenant_id = $1 AND release_id = $2`,
			tenant, releaseID)
	})
	ctx := context.Background()
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = postgres.NewTopologyRepository(db, tx).SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return releaseID
}

// tenantTask stores a task of tenant's run on the node of table with status and
// returns its id.
func tenantTask(t *testing.T, db *sqlx.DB, tenant string, runID uuid.UUID, table string, status run.TaskStatus) uuid.UUID {
	t.Helper()
	taskID := uuid.New()
	_, err := db.Exec(`
		INSERT INTO tasks (tenant_id, task_id, run_id, service_name, schema_name, table_name, job_name, status, max_retries, node_key)
		VALUES ($1, $2, $3, 'service-1', 'e2e_schema', $4, $4, $5, 2, $6)`,
		tenant, taskID, runID, table, string(status), string(leaseKey(table)))
	require.NoError(t, err)
	return taskID
}

// waitingTask stores a pending task of tenant's run waiting for the lease on
// the node of table since since, and returns its id.
func waitingTask(t *testing.T, db *sqlx.DB, tenant string, runID uuid.UUID, table string, since time.Time) uuid.UUID {
	t.Helper()
	taskID := tenantTask(t, db, tenant, runID, table, run.TaskStatusPending)
	waitSince(t, db, taskID, since)
	return taskID
}

// inTenantTx runs fn with tenant's TaskCollection bound to a transaction it
// commits.
func inTenantTx(t *testing.T, db *sqlx.DB, tenant string, fn func(tasks run.TaskCollection)) {
	t.Helper()
	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	fn(postgres.NewTaskCollection(db, discardLogger()).ForTenant(tenant).WithTx(tx))
	require.NoError(t, tx.Commit())
}

// acmeCleanup deletes the leases acme holds on tables when the test ends.
func acmeCleanup(t *testing.T, db *sqlx.DB, tables ...string) {
	t.Helper()
	keys := make([]string, 0, len(tables))
	for _, table := range tables {
		keys = append(keys, string(leaseKey(table)))
	}
	t.Cleanup(func() {
		for _, tenant := range []string{acmeTenant, pkgevents.DefaultTenantID} {
			for _, key := range keys {
				_, _ = db.ExecContext(context.Background(),
					`DELETE FROM node_leases WHERE tenant_id = $1 AND node_key = $2`, tenant, key)
			}
		}
	})
}

// With default and acme each using a node of the same key, acme's task waiting
// for its own lease on that node does not stand in default's queue: default's
// ready task takes default's lease at once.
func TestTenantIsolation_AnotherTenantsWaiterDoesNotDelayAClaim(t *testing.T) {
	db := newTestDB(t)
	table := leaseTables("shared")[0]
	acmeCleanup(t, db, table)

	acmeRun := tenantRun(t, db, acmeTenant, "running")
	waitingTask(t, db, acmeTenant, acmeRun, table, time.Now().Add(-time.Minute))

	defaultRun := tenantRun(t, db, pkgevents.DefaultTenantID, "running")
	ready := tenantTask(t, db, pkgevents.DefaultTenantID, defaultRun, table, run.TaskStatusPending)

	var taken []uuid.UUID
	inTenantTx(t, db, pkgevents.DefaultTenantID, func(tasks run.TaskCollection) {
		var err error
		taken, err = tasks.TakeLeases(context.Background(), []run.LeaseClaim{claim(defaultRun, ready, table)})
		require.NoError(t, err)
	})
	assert.Equal(t, []uuid.UUID{ready}, taken, "acme's waiter is not in default's queue")
	held, ok := leaseOn(t, db, pkgevents.DefaultTenantID, table)
	require.True(t, ok)
	assert.Equal(t, heldLease{TaskID: ready, RunID: defaultRun}, held)
	_, acmeHolds := leaseOn(t, db, acmeTenant, table)
	assert.False(t, acmeHolds, "acme's node stays free for acme's waiter")
}

// A run read with another tenant reads as not found, for a lock as for a
// snapshot, and a run read with its own tenant carries that tenant.
func TestTenantIsolation_AnotherTenantsRunReadsAsNotFound(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	defaultRun := tenantRun(t, db, pkgevents.DefaultTenantID, "running")
	acmeRun := tenantRun(t, db, acmeTenant, "running")

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		_, err := u.Run().LoadRunForUpdate(ctx, acmeTenant, defaultRun)
		assert.ErrorIs(t, err, postgres.ErrNotFound, "acme cannot lock default's run")
		_, err = u.Run().GetRun(ctx, acmeTenant, defaultRun)
		assert.ErrorIs(t, err, postgres.ErrNotFound, "acme cannot read default's run")
		exists, err := u.Run().RunExists(ctx, acmeTenant, defaultRun)
		require.NoError(t, err)
		assert.False(t, exists, "default's run does not exist for acme")

		own, err := u.Run().LoadRunForUpdate(ctx, acmeTenant, acmeRun)
		require.NoError(t, err)
		assert.Equal(t, acmeTenant, own.TenantID(), "a run carries the tenant of its row")
		theirs, err := u.Run().GetRun(ctx, pkgevents.DefaultTenantID, defaultRun)
		require.NoError(t, err)
		assert.Equal(t, pkgevents.DefaultTenantID, theirs.TenantID())
	})
}

// Each tenant has its own active run of a schedule: default and acme both run
// a schedule of the same name at once, a second active run in one tenant is
// still refused, and the active-run reads answer each tenant with its own run.
func TestTenantIsolation_ActiveRunIsPerTenant(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	daily := "daily-" + uuid.NewString()[:8]
	newRun := func(tenant string) *run.Run {
		r, err := run.NewPendingRun(tenant, uuid.New(), daily, run.KindCron, nil, "system", model.OperationRun,
			run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 1}, time.Now())
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = db.ExecContext(context.Background(), `DELETE FROM runs WHERE tenant_id = $1 AND run_id = $2`, tenant, r.RunID())
		})
		return r
	}
	defaultRun, acmeRun := newRun(pkgevents.DefaultTenantID), newRun(acmeTenant)

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		require.NoError(t, u.Run().SaveRun(ctx, defaultRun))
		require.NoError(t, u.Run().SaveRun(ctx, acmeRun), "acme runs the schedule while default does")
	})
	second := newPlanningUoW(db)
	require.NoError(t, second.Begin(ctx))
	assert.ErrorIs(t, second.Run().SaveRun(ctx, newRun(acmeTenant)), run.ErrScheduleHasActiveRun,
		"a second active run of the schedule in one tenant is refused")
	require.NoError(t, second.Rollback())

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		for tenant, want := range map[string]uuid.UUID{pkgevents.DefaultTenantID: defaultRun.RunID(), acmeTenant: acmeRun.RunID()} {
			active, err := u.Run().HasActiveRun(ctx, tenant, daily)
			require.NoError(t, err)
			assert.True(t, active, tenant)
			got, err := u.Run().GetActiveRun(ctx, tenant, daily)
			require.NoError(t, err)
			require.NotNil(t, got, tenant)
			assert.Equal(t, want, got.RunID(), "%s reads its own active run", tenant)
			assert.Equal(t, tenant, got.TenantID())
		}
		other := "tenants-" + uuid.NewString()
		active, err := u.Run().HasActiveRun(ctx, other, daily)
		require.NoError(t, err)
		assert.False(t, active, "a tenant with no run of the schedule has none active")
		got, err := u.Run().GetActiveRun(ctx, other, daily)
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}

// The waiters of a node, its grants and the work in flight a waiting task
// sees are each tenant's own: with default and acme waiting for a node of the
// same key, each tenant's waiter is the next in line for its own tenant's
// node, a lease default holds leaves acme's node free for acme's waiter, and
// a waiter is not counted as waiting on another tenant's holder.
func TestTenantIsolation_WaitersAndGrantsArePerTenant(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	table := leaseTables("shared")[0]
	acmeCleanup(t, db, table)
	since := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)

	defaultRun := tenantRun(t, db, pkgevents.DefaultTenantID, "running")
	defaultWaiter := waitingTask(t, db, pkgevents.DefaultTenantID, defaultRun, table, since)
	acmeRun := tenantRun(t, db, acmeTenant, "running")
	acmeWaiter := waitingTask(t, db, acmeTenant, acmeRun, table, since.Add(time.Minute))

	grantable, err := postgres.NewLeaseRepository(db, nil).Grantable(ctx, 1000)
	require.NoError(t, err)
	var ours []repository.LeaseWaiter
	for _, w := range inUTC(grantable) {
		if w.NodeKey == leaseKey(table) {
			ours = append(ours, w)
		}
	}
	assert.Equal(t, []repository.LeaseWaiter{
		{TenantID: pkgevents.DefaultTenantID, TaskID: defaultWaiter, RunID: defaultRun, NodeKey: leaseKey(table), WaitingSince: since},
		{TenantID: acmeTenant, TaskID: acmeWaiter, RunID: acmeRun, NodeKey: leaseKey(table), WaitingSince: since.Add(time.Minute)},
	}, ours, "each tenant's waiter is next in line for its own tenant's node")

	holderRun := tenantRun(t, db, pkgevents.DefaultTenantID, "running")
	holder := tenantTask(t, db, pkgevents.DefaultTenantID, holderRun, table, run.TaskStatusRunning)
	require.NoError(t, insertLease(db, pkgevents.DefaultTenantID, string(leaseKey(table)), holder, holderRun))

	inTenantTx(t, db, pkgevents.DefaultTenantID, func(tasks run.TaskCollection) {
		inFlight, err := tasks.HasInFlight(ctx, defaultRun)
		require.NoError(t, err)
		assert.True(t, inFlight, "default's waiter waits on default's running holder")
	})
	inTenantTx(t, db, acmeTenant, func(tasks run.TaskCollection) {
		inFlight, err := tasks.HasInFlight(ctx, acmeRun)
		require.NoError(t, err)
		assert.False(t, inFlight, "acme's waiter does not wait on default's holder")
		waits, err := tasks.GrantableWaits(ctx, acmeRun)
		require.NoError(t, err)
		require.Len(t, waits, 1, "default's lease leaves acme's node free")
		assert.Equal(t, acmeWaiter, waits[0].TaskID)
		took, err := tasks.TakeLeaseForWaiter(ctx, run.LeaseClaim{TaskID: acmeWaiter, RunID: acmeRun, NodeKey: leaseKey(table)})
		require.NoError(t, err)
		assert.True(t, took, "acme's waiter is granted acme's lease")
	})
	held, ok := leaseOn(t, db, acmeTenant, table)
	require.True(t, ok)
	assert.Equal(t, heldLease{TaskID: acmeWaiter, RunID: acmeRun}, held)
	held, ok = leaseOn(t, db, pkgevents.DefaultTenantID, table)
	require.True(t, ok)
	assert.Equal(t, heldLease{TaskID: holder, RunID: holderRun}, held, "default's lease stays default's holder's")
}

// One lease waker pass serves every tenant, each under its own tenant: two
// tenants each have a node of the same key whose holder ended without
// releasing its lease, and a task waiting for that node. The pass repairs
// both leases and grants each tenant's freed node to that tenant's waiter,
// dispatching it from the tenant's own release.
func TestLeaseWaker_GrantsEachTenantsFreedNodeToThatTenantsWaiter(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	table := leaseTables("freed")[0]
	tenants := []string{leaseTenant(t, db), leaseTenant(t, db)}
	since := time.Now().UTC().Add(-time.Hour)
	type waiter struct{ runID, taskID, holderID uuid.UUID }
	waiters := map[string]waiter{}
	for _, tenant := range tenants {
		rel := tenantRelease(t, db, tenant, table)
		holderRun := tenantRunOn(t, db, tenant, rel, "running")
		holder := tenantTask(t, db, tenant, holderRun, table, run.TaskStatusSucceeded)
		require.NoError(t, insertLease(db, tenant, string(leaseKey(table)), holder, holderRun))
		waitingRun := tenantRunOn(t, db, tenant, rel, "running")
		waiters[tenant] = waiter{runID: waitingRun, taskID: waitingTask(t, db, tenant, waitingRun, table, since), holderID: holder}
	}
	metrics := &leaseMetrics{}
	waker := leasewaker.New(scopedUoW(db, tenants...), testTaskDefaults, metrics, discardLogger())

	require.NoError(t, waker.Pass(ctx))

	for tenant, w := range waiters {
		assert.False(t, leaseOf(t, db, w.holderID), "%s's ended holder's lease is repaired", tenant)
		held, ok := leaseOn(t, db, tenant, table)
		require.True(t, ok, tenant)
		assert.Equal(t, heldLease{TaskID: w.taskID, RunID: w.runID}, held, "%s's node goes to %s's waiter", tenant, tenant)
		assert.Equal(t, leaseWait{Status: run.TaskStatusDispatched}, waitOf(t, db, w.taskID), tenant)
		var attempts int
		require.NoError(t, db.Get(&attempts, `SELECT count(*) FROM task_attempts WHERE tenant_id = $1 AND task_id = $2 AND attempt = 0`,
			tenant, w.taskID))
		assert.Equal(t, 1, attempts, "%s's waiter is dispatched with attempt 0 under its tenant", tenant)
		var dispatches int
		require.NoError(t, db.Get(&dispatches, `SELECT count(*) FROM state_outbox WHERE aggregate_id = $1 AND stream_name = $2`,
			w.runID, streams.TaskDispatchV1))
		assert.Equal(t, 1, dispatches, tenant)
	}
	assert.Equal(t, 2, metrics.repaired)
	assert.Len(t, metrics.granted, 2)
	assert.Equal(t, []int{0}, metrics.waiters)
}

// One attempt reconcile pass fails the stalled runs of every tenant, each
// under its own tenant: two tenants' runs, idle for ten minutes with a
// pending task and nothing in flight, both end failed as stalled with their
// pending task skipped.
func TestAttemptReconcile_FailsAStalledRunOfEachTenantUnderItsTenant(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	table := leaseTables("stalled")[0]
	type stalled struct{ runID, taskID uuid.UUID }
	runs := map[string]stalled{}
	tenants := []string{leaseTenant(t, db), leaseTenant(t, db)}
	for _, tenant := range tenants {
		runID := tenantRun(t, db, tenant, "running")
		taskID := tenantTask(t, db, tenant, runID, table, run.TaskStatusPending)
		_, err := db.Exec(`UPDATE runs SET last_progress_at = now() - interval '10 minutes' WHERE tenant_id = $1 AND run_id = $2`,
			tenant, runID)
		require.NoError(t, err)
		runs[tenant] = stalled{runID: runID, taskID: taskID}
	}
	loop := attemptreconcile.New(scopedUoW(db, tenants...), testTaskDefaults, noRunMetrics{}, nil, discardLogger())

	require.NoError(t, loop.Pass(ctx))

	for tenant, s := range runs {
		status, reason := runStatusOf(t, db, s.runID)
		assert.Equal(t, string(run.RunStatusFailed), status, tenant)
		require.NotNil(t, reason, tenant)
		assert.Equal(t, string(run.RunFailureStalled), *reason, tenant)
		assert.Equal(t, leaseWait{Status: run.TaskStatusSkipped}, waitOf(t, db, s.taskID), tenant)
	}
}
