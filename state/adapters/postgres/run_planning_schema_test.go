package postgres_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunPlanning_PlannedAtIsNullableAndStoresAnInstant(t *testing.T) {
	db := newTestDB(t)
	id := stampRun(t, db, int64(3))

	var planned *time.Time
	require.NoError(t, db.Get(&planned, `SELECT planned_at FROM scheduler_tracker WHERE schedule_id = $1`, id))
	assert.Nil(t, planned)

	at := time.Date(2026, 10, 14, 23, 0, 0, 0, time.UTC)
	_, err := db.Exec(`UPDATE scheduler_tracker SET planned_at = $2 WHERE schedule_id = $1`, id, at)
	require.NoError(t, err)
	require.NoError(t, db.Get(&planned, `SELECT planned_at FROM scheduler_tracker WHERE schedule_id = $1`, id))
	require.NotNil(t, planned)
	assert.True(t, at.Equal(*planned))
}

func TestRunPlanning_DispatchedAtIsNullableAndStoresAnInstant(t *testing.T) {
	db := newTestDB(t)
	id := stampRun(t, db, int64(3))
	require.NoError(t, stampTask(db, id, "raw.orders"))

	var dispatched *time.Time
	require.NoError(t, db.Get(&dispatched, `SELECT dispatched_at FROM task_tracker WHERE schedule_id = $1`, id))
	assert.Nil(t, dispatched)

	at := time.Date(2026, 10, 14, 23, 0, 41, 0, time.UTC)
	_, err := db.Exec(`UPDATE task_tracker SET dispatched_at = $2 WHERE schedule_id = $1`, id, at)
	require.NoError(t, err)
	require.NoError(t, db.Get(&dispatched, `SELECT dispatched_at FROM task_tracker WHERE schedule_id = $1`, id))
	require.NotNil(t, dispatched)
	assert.True(t, at.Equal(*dispatched))
}

func TestRunPlanning_ScheduleNodesAreIndexed(t *testing.T) {
	db := newTestDB(t)
	var def string
	require.NoError(t, db.Get(&def, `SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_topology_nodes_schedule'`))
	assert.Contains(t, def, "ON public.topology_nodes USING btree (tenant_id, release_id, schedule_name)")
}

// task_tracker keeps no index on status alone: pending tasks are rare in the
// table's statistics, and such an index would be read for one run's pending
// tasks instead of the run's own index.
func TestRunPlanning_TaskStatusHasNoIndexOfItsOwn(t *testing.T) {
	db := newTestDB(t)
	var n int
	require.NoError(t, db.Get(&n, `SELECT count(*) FROM pg_indexes WHERE tablename = 'task_tracker' AND indexname = 'idx_task_tracker_status'`))
	assert.Zero(t, n)
}
