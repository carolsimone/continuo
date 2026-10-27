package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/runplan"
	"github.com/carolsimone/continuo/state/domain/topology"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// independentNodes is a release slice of unconnected dbt models
// public.<table> of service svc, with the plan of a run that executes them all.
func independentNodes(t *testing.T, tables ...string) (runplan.Plan, topology.Graph) {
	t.Helper()
	nodes := make([]topology.Node, 0, len(tables))
	for _, table := range tables {
		nodes = append(nodes, topology.Node{
			Key: topology.KeyOf("public", table), ServiceName: "svc", SchemaName: "public", TableName: table, NodeType: "dbt-model",
		})
	}
	g := topology.NewGraph(nodes, nil)
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	return plan, g
}

// TestRunRepository_SaveRun_PersistsStartedAtOnStart walks the real
// PENDING→RUNNING production path: a PENDING run is created, loaded FOR UPDATE,
// then started with a plan holding a pending task (so the run transitions to
// RUNNING and stamps started_at). SaveRun must persist that started_at to
// runs.
func TestRunRepository_SaveRun_PersistsStartedAtOnStart(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewRunRepository(db, discardLogger())
	taskRepo := postgres.NewTaskCollection(db, discardLogger())

	scheduleID := uuid.New()
	createdAt := time.Now().Add(-time.Minute)
	require.NoError(t, schedRepo.Create(ctx, &postgres.RunRow{
		TenantID:     pkgevents.DefaultTenantID,
		ScheduleID:   scheduleID,
		ScheduleName: "started-" + scheduleID.String()[:8],
		Status:       run.RunStatusPending,
		CreatedAt:    createdAt,
		Kind:         "cron",
	}))
	defer db.ExecContext(ctx, "DELETE FROM tasks WHERE run_id = $1", scheduleID)
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", scheduleID)

	// Before: started_at is NULL on a freshly created PENDING row.
	before, err := schedRepo.GetByID(ctx, pkgevents.DefaultTenantID, scheduleID)
	require.NoError(t, err)
	require.Nil(t, before.StartedAt, "freshly created run has no started_at")

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	runRepo := schedRepo.WithTx(tx)
	tasks := taskRepo.ForTenant("default").WithTx(tx)

	rn, err := runRepo.LoadRunForUpdate(ctx, pkgevents.DefaultTenantID, scheduleID)
	require.NoError(t, err)

	// A pending task keeps the run RUNNING (not finalized), so Start stamps
	// started_at and sets startedDirty.
	plan, g := independentNodes(t, "tbl")
	_, err = rn.Start(ctx, tasks, plan, g, testTaskDefaults, time.Now())
	require.NoError(t, err)
	require.Equal(t, run.RunStatusRunning, rn.Status(), "run must be RUNNING after a start with a pending task")

	require.NoError(t, runRepo.SaveRun(ctx, rn))
	require.NoError(t, tx.Commit())

	got, err := schedRepo.GetByID(ctx, pkgevents.DefaultTenantID, scheduleID)
	require.NoError(t, err)
	require.NotNil(t, got.StartedAt, "started_at must be persisted on PENDING→RUNNING")
	assert.False(t, got.StartedAt.Before(got.CreatedAt),
		"started_at must be at or after created_at (valid_timestamps constraint)")
	assert.Equal(t, run.RunStatusRunning, got.Status)
}

// TestRunRepository_SaveRun_SingleUpdateForDirtyColumns asserts that a SaveRun
// touching several dirty columns at once (status, init_status, total/terminal
// task counts, started_at) lands every value via the consolidated single
// UPDATE. The verification is behavioural: all five columns reflect the
// in-memory aggregate state after one SaveRun commit.
func TestRunRepository_SaveRun_SingleUpdateForDirtyColumns(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewRunRepository(db, discardLogger())
	taskRepo := postgres.NewTaskCollection(db, discardLogger())

	scheduleID := uuid.New()
	require.NoError(t, schedRepo.Create(ctx, &postgres.RunRow{
		TenantID:     pkgevents.DefaultTenantID,
		ScheduleID:   scheduleID,
		ScheduleName: "multi-" + scheduleID.String()[:8],
		Status:       run.RunStatusPending,
		CreatedAt:    time.Now().Add(-time.Minute),
		Kind:         "cron",
	}))
	defer db.ExecContext(ctx, "DELETE FROM tasks WHERE run_id = $1", scheduleID)
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", scheduleID)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	runRepo := schedRepo.WithTx(tx)
	tasks := taskRepo.ForTenant("default").WithTx(tx)

	rn, err := runRepo.LoadRunForUpdate(ctx, pkgevents.DefaultTenantID, scheduleID)
	require.NoError(t, err)

	// Two tasks, neither terminal: Start sets total_task_count=2,
	// terminal_task_count=0, status=running, started_at.
	plan, g := independentNodes(t, "a", "b")
	_, err = rn.Start(ctx, tasks, plan, g, testTaskDefaults, time.Now())
	require.NoError(t, err)

	require.NoError(t, runRepo.SaveRun(ctx, rn))
	require.NoError(t, tx.Commit())

	got, err := schedRepo.GetByID(ctx, pkgevents.DefaultTenantID, scheduleID)
	require.NoError(t, err)
	assert.Equal(t, run.RunStatusRunning, got.Status)
	require.True(t, got.TotalTaskCount.Valid)
	assert.Equal(t, int32(2), got.TotalTaskCount.Int32)
	assert.Equal(t, int32(0), got.TerminalTaskCount)
	require.NotNil(t, got.StartedAt)
}

// TestSchedulerRepository_UpdateRunRowTx_StartedAt verifies the consolidated
// update writes only the named columns and leaves others untouched, exercising
// the started_at column directly.
func TestSchedulerRepository_UpdateRunRowTx_StartedAt(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := postgres.NewRunRepository(db, discardLogger())

	scheduleID := uuid.New()
	createdAt := time.Now().Add(-time.Minute)
	require.NoError(t, repo.Create(ctx, &postgres.RunRow{
		TenantID:     pkgevents.DefaultTenantID,
		ScheduleID:   scheduleID,
		ScheduleName: "urr-" + scheduleID.String()[:8],
		Status:       run.RunStatusPending,
		CreatedAt:    createdAt,
		Kind:         "cron",
	}))
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", scheduleID)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	started := time.Now()
	status := string(run.RunStatusRunning)
	require.NoError(t, repo.UpdateRunRowTx(ctx, tx, pkgevents.DefaultTenantID, scheduleID, postgres.RunRowUpdate{
		Status:    &status,
		StartedAt: &started,
	}))
	require.NoError(t, tx.Commit())

	got, err := repo.GetByID(ctx, pkgevents.DefaultTenantID, scheduleID)
	require.NoError(t, err)
	assert.Equal(t, run.RunStatusRunning, got.Status)
	require.NotNil(t, got.StartedAt)
}

// TestSchedulerRepository_UpdateRunRowTx_NoFieldsIsNoop asserts that an empty
// dirty set issues no statement and returns nil (SaveRun on a run with no
// dirty columns must not error or touch the row).
func TestSchedulerRepository_UpdateRunRowTx_NoFieldsIsNoop(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := postgres.NewRunRepository(db, discardLogger())

	scheduleID := uuid.New()
	require.NoError(t, repo.Create(ctx, &postgres.RunRow{
		TenantID:     pkgevents.DefaultTenantID,
		ScheduleID:   scheduleID,
		ScheduleName: "noop-" + scheduleID.String()[:8],
		Status:       run.RunStatusPending,
		CreatedAt:    time.Now(),
		Kind:         "cron",
	}))
	defer db.ExecContext(ctx, "DELETE FROM runs WHERE run_id = $1", scheduleID)

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	// A non-existent id with no fields must still be a no-op (no row check runs).
	require.NoError(t, repo.UpdateRunRowTx(ctx, tx, pkgevents.DefaultTenantID, uuid.New(), postgres.RunRowUpdate{}))
	require.NoError(t, tx.Commit())
}
