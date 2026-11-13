package postgres_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	repository "github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GetByID reads one task's identity and image, and reports a missing task as
// run.ErrTaskNotFound.
func TestTaskCollection_GetByID(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	logger := discardLogger()
	schedRepo := postgres.NewRunRepository(db, logger)
	taskRepo := postgres.NewTaskCollection(db, logger)
	sched := createScheduler(t, schedRepo, "get-by-id-"+uuid.NewString()[:8])
	taskID := uuid.New()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM tasks WHERE task_id = $1`, taskID)
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, sched.ScheduleID)
	})
	require.NoError(t, taskRepo.Create(ctx, &postgres.TaskRow{
		TenantID: pkgevents.DefaultTenantID,
		TaskID:   taskID, ScheduleID: sched.ScheduleID, CreatedAt: time.Now(), ServiceName: "service-1",
		SchemaName: "raw", TableName: "orders", JobName: "job-orders", Status: run.TaskStatusSucceeded,
		ImageTag: "service-1:v3",
	}))
	collection := taskRepo.ForTenant("default")

	got, err := collection.GetByID(ctx, taskID)
	require.NoError(t, err)
	assert.Equal(t, taskID, got.TaskID)
	assert.Equal(t, sched.ScheduleID, got.ScheduleID)
	assert.Equal(t, run.NodeID{ServiceName: "service-1", SchemaName: "raw", TableName: "orders"}, got.Node())
	assert.Equal(t, "service-1:v3", got.ImageTag)
	assert.Equal(t, run.TaskStatusSucceeded, got.Status)

	_, err = collection.GetByID(ctx, uuid.New())
	assert.ErrorIs(t, err, run.ErrTaskNotFound)
}

// RunReleasesOf reads, through the unit of work's transaction, the release
// each task's run is stamped with. A task whose run names no release, and an
// id no task has, are absent. With a pool of one connection, held by the
// transaction, a read on another connection would wait until the deadline.
func TestTaskCollection_RunReleasesOf(t *testing.T) {
	db := newTestDB(t)
	logger := discardLogger()
	schedRepo := postgres.NewRunRepository(db, logger)
	taskRepo := postgres.NewTaskCollection(db, logger)
	suffix := uuid.NewString()[:8]
	release := "rel-" + suffix
	stamped := createScheduler(t, schedRepo, "run-releases-stamped-"+suffix)
	legacy := createScheduler(t, schedRepo, "run-releases-legacy-"+suffix)
	stampedTask, legacyTask := uuid.New(), uuid.New()
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM tasks WHERE task_id IN ($1, $2)`, stampedTask, legacyTask)
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id IN ($1, $2)`, stamped.ScheduleID, legacy.ScheduleID)
	})
	_, err := db.Exec(`UPDATE runs SET release_id = $2, promotion_seq = 1 WHERE run_id = $1`, stamped.ScheduleID, release)
	require.NoError(t, err)
	for id, sched := range map[uuid.UUID]uuid.UUID{stampedTask: stamped.ScheduleID, legacyTask: legacy.ScheduleID} {
		require.NoError(t, taskRepo.Create(context.Background(), &postgres.TaskRow{
			TenantID: pkgevents.DefaultTenantID,
			TaskID:   id, ScheduleID: sched, CreatedAt: time.Now(), ServiceName: "service-1",
			SchemaName: "raw", TableName: "orders", JobName: "job-orders", Status: run.TaskStatusSucceeded,
		}))
	}

	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	collection := taskRepo.ForTenant("default").WithTx(tx)

	got, err := collection.RunReleasesOf(ctx, []uuid.UUID{stampedTask, legacyTask, uuid.New()})

	require.NoError(t, err)
	assert.Equal(t, map[uuid.UUID]string{stampedTask: release}, got)
}

// ListByRunID pages a run's tasks in a stable order. Every task of a run is
// created with the same created_at, so the order among them comes from the
// task id: reading the run page by page lists each task exactly once, in
// ascending task id.
func TestTaskCollection_ListByRunIDPagesTasksOfOneCreatedAtInTaskIDOrder(t *testing.T) {
	const (
		taskCount = 450
		pageSize  = 200
	)
	ctx := context.Background()
	db := newTestDB(t)
	logger := discardLogger()
	schedRepo := postgres.NewRunRepository(db, logger)
	taskRepo := postgres.NewTaskCollection(db, logger)
	sched := createScheduler(t, schedRepo, "list-by-run-paging-"+uuid.NewString()[:8])
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM tasks WHERE run_id = $1`, sched.ScheduleID)
		_, _ = db.Exec(`DELETE FROM runs WHERE run_id = $1`, sched.ScheduleID)
	})
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	want := make([]string, 0, taskCount)
	for range taskCount {
		id := uuid.New()
		want = append(want, id.String())
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskRow{
			TenantID: pkgevents.DefaultTenantID,
			TaskID:   id, ScheduleID: sched.ScheduleID, CreatedAt: createdAt, ServiceName: "service-1",
			SchemaName: "raw", TableName: "orders", JobName: "job-" + id.String(), Status: run.TaskStatusPending,
		}))
	}
	sort.Strings(want)
	collection := taskRepo.ForTenant(pkgevents.DefaultTenantID)

	got := make([]string, 0, taskCount)
	for offset := 0; ; offset += pageSize {
		page, total, err := collection.ListByRunID(ctx, sched.ScheduleID, nil, pageSize, offset)
		require.NoError(t, err)
		require.Equal(t, taskCount, total)
		for _, task := range page {
			got = append(got, task.TaskID.String())
		}
		if len(page) < pageSize {
			break
		}
	}

	assert.Equal(t, want, got)
}

// Each task stores its own operation, plan reason, seed preflight and the seed
// it rebuilds after, and every read a dispatch is built from returns them: the
// attempt state a retry or a lease grant reads, the question about an open
// attempt, the ready candidates a success dispatches, and the task reads.
func TestTaskCollection_StoresTheDispatchSpecAndReadsItBack(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	seedKey, viewKey := topology.NodeKey("e2e_schema.seed_fx_rates_eur"), topology.NodeKey("e2e_schema.fx_rates_current")
	tenant := storeEdges(t, db, "fx", [][2]string{{"seed_fx_rates_eur", "fx_rates_current"}}, "seed_fx_rates_eur", "fx_rates_current")
	now := time.Now().UTC().Truncate(time.Microsecond)
	r, err := run.NewPendingRun(tenant, uuid.New(), "fx", run.KindCron, nil, identity.SystemUserID, model.OperationRun,
		run.ReleaseStamp{ReleaseID: "rel-1", PromotionSeq: 1}, now)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = $1`, r.RunID()) })
	pf := &seedreload.Preflight{Mode: seedreload.PreflightCheck, Family: warehouse.SchemaBound,
		Relation: "e2e_schema.seed_fx_rates_eur", Allowed: []string{"e2e_schema.fx_rates_current"}, Acknowledged: []string{}}
	seedSpec := run.DispatchSpec{Operation: model.OperationFullRefresh, Reason: "columns changed", Preflight: pf}
	viewSpec := run.DispatchSpec{Operation: model.OperationRun, Reason: "rebuild: dropped by the load of e2e_schema.seed_fx_rates_eur"}
	task := func(table string, key topology.NodeKey, status run.TaskStatus, spec run.DispatchSpec) run.Task {
		return run.Task{TaskID: run.TaskIDFor(tenant, r.RunID(), key), ScheduleID: r.RunID(), CreatedAt: now,
			ServiceName: "service-1", SchemaName: "e2e_schema", TableName: table, NodeKey: key, JobName: "job-" + table,
			Status: status, MaxRetries: 2, Operation: spec.Operation, PlanReason: spec.Reason, SeedPreflight: spec.Preflight}
	}
	seed := task("seed_fx_rates_eur", seedKey, run.TaskStatusDispatched, seedSpec)
	seed.DispatchedAt = &now
	view := task("fx_rates_current", viewKey, run.TaskStatusPending, viewSpec)
	view.RebuildOf = seedKey
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		require.NoError(t, u.Run().SaveRun(ctx, r))
		tasks := u.TaskCollection(tenant)
		require.NoError(t, tasks.BulkCreate(ctx, []run.Task{seed, view}))
		require.NoError(t, tasks.AddAttempts(ctx, []run.Attempt{{TaskID: seed.TaskID, Number: 0, JobName: seed.JobName,
			Status: run.AttemptDispatched, DispatchedAt: &now}}))
	})
	_, err = db.Exec(`UPDATE task_attempts SET next_check_at = now() - interval '1 second' WHERE task_id = $1`, seed.TaskID)
	require.NoError(t, err)

	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		state, found, err := u.TaskCollection(tenant).LoadAttemptState(ctx, seed.TaskID)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, seedSpec, state.Spec, "a retry or a grant of the seed reads its spec")

		q, found, err := u.Attempts().LockQuestion(ctx, repository.AttemptKey{TenantID: tenant, TaskID: seed.TaskID, Attempt: 0})
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, seedSpec, q.Spec, "a question about the seed's attempt carries its spec")

		_, err = u.TaskCollection(tenant).SetStatusAndAttempt(ctx, seed.TaskID, run.TaskStatusSucceeded, 0)
		require.NoError(t, err)
		candidates, err := u.TaskGraph(tenant).ReadyCandidates(ctx, r.RunID(), "rel-1", seedKey)
		require.NoError(t, err)
		require.Len(t, candidates, 1)
		assert.Equal(t, view.TaskID, candidates[0].TaskID)
		assert.Equal(t, viewSpec, candidates[0].Spec, "the view a success dispatches carries its own spec")
	})

	tasks := postgres.NewTaskCollection(db, discardLogger()).ForTenant(tenant)
	gotView, err := tasks.GetByID(ctx, view.TaskID)
	require.NoError(t, err)
	assert.Equal(t, seedKey, gotView.RebuildOf)
	assert.Equal(t, viewSpec, gotView.Spec())
	assert.Nil(t, gotView.SeedPreflight, "a task with no preflight stores NULL")
	gotSeed, err := tasks.GetByID(ctx, seed.TaskID)
	require.NoError(t, err)
	assert.Equal(t, seedSpec, gotSeed.Spec())
	assert.Empty(t, gotSeed.RebuildOf)
	inUoW(t, db, func(u *postgres.PostgresUnitOfWork) {
		listed, err := u.TaskCollection(tenant).ListRunTasks(ctx, r.RunID())
		require.NoError(t, err)
		require.Len(t, listed, 2)
		assert.Equal(t, viewSpec, listed[0].Spec())
		assert.Equal(t, seedKey, listed[0].RebuildOf)
		assert.Equal(t, seedSpec, listed[1].Spec())
	})
	var stored string
	require.NoError(t, db.Get(&stored, `SELECT seed_preflight::text FROM tasks WHERE task_id = $1`, seed.TaskID))
	assert.JSONEq(t, `{"mode":"check","engine_family":"schema_bound","relation":"e2e_schema.seed_fx_rates_eur",
		"allowed":["e2e_schema.fx_rates_current"],"acknowledged":[]}`, stored, "the column holds the dispatch's preflight shape")
}
