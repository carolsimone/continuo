package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeRunRepository_List seeds two scheduler runs (cron + rerun) each with a
// task on (svc, sch, tbl), one extra task on (svc, sch, other) that must be
// excluded, and one execution per task.  It verifies ordering (rerun first),
// field projection, and that the unrelated task is filtered out.
func TestNodeRunRepository_List(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	execRepo := postgres.NewTaskExecutionRepository(db, discardLogger())
	nodeRunRepo := postgres.NewNodeRunRepository(db, discardLogger())

	svc := "svc-nr-" + uuid.New().String()[:8]
	sch := "sch-" + uuid.New().String()[:8]
	tbl := "tbl-" + uuid.New().String()[:8]
	other := "other-" + uuid.New().String()[:8]

	// --- scheduler 1: cron, succeeded ---
	sched1ID := uuid.New()
	sched1 := &postgres.SchedulerTracker{
		ScheduleID:           sched1ID,
		ScheduleName:         sch,
		Status:               run.SchedulerStatusSucceeded,
		Kind:                 "cron",
		CreatedAt:            time.Now().Add(-2 * time.Minute),
		InitializationStatus: "completed",
	}
	require.NoError(t, schedRepo.Create(ctx, sched1))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sched1ID)

	// task1: on the target node
	task1ID := uuid.New()
	task1 := &postgres.TaskTracker{
		TaskID:          task1ID,
		ScheduleID:      sched1ID,
		ServiceName:     svc,
		SchemaName:      sch,
		TableName:       tbl,
		JobName:         "job-1",
		Status:          run.TaskStatusSucceeded,
		RetryCount:      0,
		MaxRetries:      3,
		ManifestVersion: "m1",
		ImageTag:        "v1",
		CreatedAt:       time.Now().Add(-2 * time.Minute),
	}
	require.NoError(t, taskRepo.Create(ctx, task1))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", task1ID)

	// execution for task1
	exec1 := &postgres.TaskExecution{
		ID:          uuid.New(),
		TaskID:      task1ID,
		CreatedAt:   time.Now().Add(-2 * time.Minute),
		StartedAt:   ptrTime(time.Now().Add(-90 * time.Second)),
		CompletedAt: ptrTime(time.Now().Add(-60 * time.Second)),
	}
	require.NoError(t, execRepo.Create(ctx, exec1))
	defer db.ExecContext(ctx, "DELETE FROM task_execution WHERE id = $1", exec1.ID)

	// extra task on (svc, sch, other) — must NOT appear in results
	taskExtraID := uuid.New()
	taskExtra := &postgres.TaskTracker{
		TaskID:          taskExtraID,
		ScheduleID:      sched1ID,
		ServiceName:     svc,
		SchemaName:      sch,
		TableName:       other,
		JobName:         "job-extra",
		Status:          run.TaskStatusSucceeded,
		RetryCount:      0,
		MaxRetries:      3,
		ManifestVersion: "mx",
		ImageTag:        "vx",
		CreatedAt:       time.Now().Add(-2 * time.Minute),
	}
	require.NoError(t, taskRepo.Create(ctx, taskExtra))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", taskExtraID)

	// --- scheduler 2: rerun (source = sched1), failed — created more recently ---
	sched2ID := uuid.New()
	sched2 := &postgres.SchedulerTracker{
		ScheduleID:           sched2ID,
		ScheduleName:         sch,
		Status:               run.SchedulerStatusFailed,
		Kind:                 "rerun",
		SourceRunID:          &sched1ID,
		CreatedAt:            time.Now().Add(-1 * time.Minute),
		InitializationStatus: "completed",
	}
	require.NoError(t, schedRepo.Create(ctx, sched2))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sched2ID)

	// task2: on the target node, rerun
	task2ID := uuid.New()
	errMsg := "boom"
	task2 := &postgres.TaskTracker{
		TaskID:          task2ID,
		ScheduleID:      sched2ID,
		ServiceName:     svc,
		SchemaName:      sch,
		TableName:       tbl,
		JobName:         "job-2",
		Status:          run.TaskStatusFailed,
		RetryCount:      1,
		MaxRetries:      3,
		ManifestVersion: "m2",
		ImageTag:        "v2",
		CreatedAt:       time.Now().Add(-1 * time.Minute),
	}
	require.NoError(t, taskRepo.Create(ctx, task2))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", task2ID)

	// execution for task2 (with error_message)
	exec2 := &postgres.TaskExecution{
		ID:           uuid.New(),
		TaskID:       task2ID,
		CreatedAt:    time.Now().Add(-1 * time.Minute),
		StartedAt:    ptrTime(time.Now().Add(-50 * time.Second)),
		CompletedAt:  ptrTime(time.Now().Add(-30 * time.Second)),
		ErrorMessage: &errMsg,
	}
	require.NoError(t, execRepo.Create(ctx, exec2))
	defer db.ExecContext(ctx, "DELETE FROM task_execution WHERE id = $1", exec2.ID)

	// --- query ---
	rows, err := nodeRunRepo.List(ctx, svc, sch, tbl, 50)
	require.NoError(t, err)
	require.Len(t, rows, 2, "expected exactly 2 rows for (svc, sch, tbl)")

	// ordered by scheduler_tracker.created_at DESC: sched2 (rerun) first
	assert.Equal(t, sched2ID, rows[0].ScheduleID)
	assert.Equal(t, "rerun", rows[0].Kind)
	assert.Equal(t, "v2", rows[0].ImageTag)
	assert.Equal(t, "m2", rows[0].ManifestVersion)
	assert.NotNil(t, rows[0].StartedAt)
	assert.NotNil(t, rows[0].CompletedAt)
	require.NotNil(t, rows[0].ErrorMessage)
	assert.Equal(t, "boom", *rows[0].ErrorMessage)

	assert.Equal(t, sched1ID, rows[1].ScheduleID)
	assert.Equal(t, "cron", rows[1].Kind)
}

// TestNodeRunRepository_List_TaskWithoutExecution verifies that a task with no
// corresponding task_execution row produces a NodeRun with nil StartedAt and
// nil CompletedAt (LEFT JOIN semantics).
func TestNodeRunRepository_List_TaskWithoutExecution(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	nodeRunRepo := postgres.NewNodeRunRepository(db, discardLogger())

	svc := "svc-noe-" + uuid.New().String()[:8]
	sch := "sch-" + uuid.New().String()[:8]
	tbl := "tbl-" + uuid.New().String()[:8]

	schedID := uuid.New()
	sched := &postgres.SchedulerTracker{
		ScheduleID:           schedID,
		ScheduleName:         sch,
		Status:               run.SchedulerStatusRunning,
		Kind:                 "cron",
		CreatedAt:            time.Now(),
		InitializationStatus: "completed",
	}
	require.NoError(t, schedRepo.Create(ctx, sched))
	defer db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", schedID)

	taskID := uuid.New()
	task := &postgres.TaskTracker{
		TaskID:          taskID,
		ScheduleID:      schedID,
		ServiceName:     svc,
		SchemaName:      sch,
		TableName:       tbl,
		JobName:         "job-pending",
		Status:          run.TaskStatusPending,
		RetryCount:      0,
		MaxRetries:      3,
		ManifestVersion: "m1",
		ImageTag:        "v1",
		CreatedAt:       time.Now(),
	}
	require.NoError(t, taskRepo.Create(ctx, task))
	defer db.ExecContext(ctx, "DELETE FROM task_tracker WHERE task_id = $1", taskID)

	// No task_execution row inserted.

	rows, err := nodeRunRepo.List(ctx, svc, sch, tbl, 50)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Nil(t, rows[0].StartedAt)
	assert.Nil(t, rows[0].CompletedAt)
}

// TestNodeRunRepository_List_NoMatch verifies that querying for a node identity
// with no data returns an empty slice and no error.
func TestNodeRunRepository_List_NoMatch(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	nodeRunRepo := postgres.NewNodeRunRepository(db, discardLogger())

	rows, err := nodeRunRepo.List(ctx, "nonexistent-svc", "nonexistent-sch", "nonexistent-tbl", 50)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

func ptrTime(t time.Time) *time.Time { return &t }

// TestNodeRunRepository_ListNodes_AggregatesAndFilters seeds two nodes under the
// same service and asserts the catalog aggregates, ordering (newest last_run_at
// first), the service filter, and the exact table-name search filter.
func TestNodeRunRepository_ListNodes_AggregatesAndFilters(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	execRepo := postgres.NewTaskExecutionRepository(db, discardLogger())
	repo := postgres.NewNodeRunRepository(db, discardLogger())

	svc := "svc-ln-" + uuid.New().String()[:8]
	sch := "an"
	tblA := "fct_orders"
	tblB := "stg_customers"

	seed := func(table string, schedStatus run.SchedulerStatus, taskStatus run.TaskStatus, retry int, ageMin int, durSec int) {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: sch, Status: schedStatus, Kind: "cron",
			CreatedAt:            time.Now().Add(-time.Duration(ageMin) * time.Minute),
			InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		tid := uuid.New()
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: tid, ScheduleID: sid, ServiceName: svc, SchemaName: sch, TableName: table,
			JobName: "j", Status: taskStatus, RetryCount: retry, MaxRetries: 3,
			ManifestVersion: "m1", ImageTag: "v1",
			CreatedAt: time.Now().Add(-time.Duration(ageMin) * time.Minute),
		}))
		start := time.Now().Add(-time.Duration(ageMin) * time.Minute)
		end := start.Add(time.Duration(durSec) * time.Second)
		require.NoError(t, execRepo.Create(ctx, &postgres.TaskExecution{
			ID: uuid.New(), TaskID: tid, CreatedAt: start,
			StartedAt: ptrTime(start), CompletedAt: ptrTime(end),
		}))
	}

	// node A: 2 runs, 1 succeeded + 1 failed (50% success), one flaky (retry>0), durations 10s & 30s
	seed(tblA, run.SchedulerStatusSucceeded, run.TaskStatusSucceeded, 0, 30, 10)
	seed(tblA, run.SchedulerStatusFailed, run.TaskStatusFailed, 1, 5, 30) // most recent for A
	// node B: 1 run, succeeded, duration 4s — most recent overall
	seed(tblB, run.SchedulerStatusSucceeded, run.TaskStatusSucceeded, 0, 2, 4)

	nodes, total, err := repo.ListNodes(ctx, "", svc, "", 50, 0)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, nodes, 2)

	assert.Equal(t, tblB, nodes[0].TableName) // B (2m) before A (5m)
	assert.Equal(t, tblA, nodes[1].TableName)

	a := nodes[1]
	assert.Equal(t, 2, a.RunCount)
	assert.Equal(t, 50, a.SuccessRatePct)
	assert.Equal(t, 20, a.AvgDurationSec) // (10+30)/2
	assert.Equal(t, 29, a.P95DurationSec) // PERCENTILE_CONT(0.95) of {10,30} interpolates: 10 + 0.95*(30-10) = 29
	assert.Equal(t, 50, a.FlakyRatePct)   // 1 of 2 runs had retry>0
	assert.Equal(t, "failed", a.LastStatus)

	none, total2, err := repo.ListNodes(ctx, "", "no-such-svc", "", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, total2)
	assert.Empty(t, none)

	hit, total3, err := repo.ListNodes(ctx, "fct_orders", svc, "", 50, 0) // exact table-name match
	require.NoError(t, err)
	assert.Equal(t, 1, total3)
	require.Len(t, hit, 1)
	assert.Equal(t, tblA, hit[0].TableName)
}

// TestNodeRunRepository_ListNodes_PerNodeStatusIsolation verifies the aggregate
// keys off task_tracker.status, NOT scheduler_tracker.status.
func TestNodeRunRepository_ListNodes_PerNodeStatusIsolation(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	repo := postgres.NewNodeRunRepository(db, discardLogger())

	svc := "svc-iso-" + uuid.New().String()[:8]
	sid := uuid.New()
	require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusFailed, Kind: "cron",
		CreatedAt: time.Now().Add(-time.Minute), InitializationStatus: "completed",
	}))
	t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
	require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
		TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an", TableName: "ok_node",
		JobName: "j", Status: run.TaskStatusSucceeded, MaxRetries: 3, ManifestVersion: "m", ImageTag: "v",
		CreatedAt: time.Now().Add(-time.Minute),
	}))

	nodes, _, err := repo.ListNodes(ctx, "", svc, "", 50, 0)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, 100, nodes[0].SuccessRatePct, "node task succeeded => 100% despite failed schedule")
	assert.Equal(t, "succeeded", nodes[0].LastStatus)
}

// TestNodeRunRepository_ListNodes_Paging asserts limit/offset slice the result
// while total_count stays the full match count.
func TestNodeRunRepository_ListNodes_Paging(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	repo := postgres.NewNodeRunRepository(db, discardLogger())

	svc := "svc-pg-" + uuid.New().String()[:8]
	for i := 0; i < 3; i++ {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Duration(i) * time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an",
			TableName: "t" + uuid.New().String()[:4], JobName: "j", Status: run.TaskStatusSucceeded,
			MaxRetries: 3, ManifestVersion: "m", ImageTag: "v",
			CreatedAt: time.Now().Add(-time.Duration(i) * time.Minute),
		}))
	}

	page1, total, err := repo.ListNodes(ctx, "", svc, "", 2, 0)
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	assert.Len(t, page1, 2)

	page2, _, err := repo.ListNodes(ctx, "", svc, "", 2, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 1)

	seen := map[string]bool{}
	for _, n := range append(page1, page2...) {
		seen[n.TableName] = true
	}
	assert.Len(t, seen, 3, "page1 and page2 must be disjoint and cover all 3 nodes")
}

// TestNodeRunRepository_ListNodes_StablePagingOnTiedLastRun seeds many nodes
// under one scheduler so they share scheduler_tracker.created_at (identical
// last_run_at). Paging must stay disjoint and complete.
func TestNodeRunRepository_ListNodes_StablePagingOnTiedLastRun(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	repo := postgres.NewNodeRunRepository(db, discardLogger())

	svc := "svc-tie-" + uuid.New().String()[:8]
	sid := uuid.New()
	require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
		ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
		CreatedAt: time.Now().Add(-time.Minute), InitializationStatus: "completed",
	}))
	t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
	// 5 distinct nodes all under the SAME scheduler => identical last_run_at
	for i := 0; i < 5; i++ {
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an",
			TableName: "t" + uuid.New().String()[:6], JobName: "j", Status: run.TaskStatusSucceeded,
			MaxRetries: 3, ManifestVersion: "m", ImageTag: "v", CreatedAt: time.Now().Add(-time.Minute),
		}))
	}
	seen := map[string]int{}
	for off := 0; off < 5; off += 2 {
		page, total, err := repo.ListNodes(ctx, "", svc, "", 2, off)
		require.NoError(t, err)
		assert.Equal(t, 5, total)
		for _, n := range page {
			seen[n.TableName]++
		}
	}
	assert.Len(t, seen, 5, "all 5 tied-last_run nodes appear exactly once across pages")
	for tbl, c := range seen {
		assert.Equal(t, 1, c, "node %s appeared %d times", tbl, c)
	}
}

// TestNodeRunRepository_ListNodes_SkippedCountsTerminal verifies skipped counts
// as terminal: succeeded + skipped => 50% (not 100%).
func TestNodeRunRepository_ListNodes_SkippedCountsTerminal(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	repo := postgres.NewNodeRunRepository(db, discardLogger())
	svc := "svc-skip-" + uuid.New().String()[:8]
	mk := func(taskStatus run.TaskStatus, ageMin int) {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Duration(ageMin) * time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an", TableName: "n",
			JobName: "j", Status: taskStatus, MaxRetries: 3, ManifestVersion: "m", ImageTag: "v",
			CreatedAt: time.Now().Add(-time.Duration(ageMin) * time.Minute),
		}))
	}
	mk(run.TaskStatusSucceeded, 10)
	mk(run.TaskStatusSkipped, 5)
	nodes, _, err := repo.ListNodes(ctx, "", svc, "", 50, 0)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.Equal(t, 50, nodes[0].SuccessRatePct, "succeeded+skipped => 1/2 = 50%")
}

// TestNodeRunRepository_ListNodes_ExactTableMatch verifies the search term is an
// EXACT (case-insensitive) match on the table name: "fct_orders" returns only
// "fct_orders", never a substring sibling ("fctxorders") or a suffixed variant
// ("fct_orders_v2").
func TestNodeRunRepository_ListNodes_ExactTableMatch(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	repo := postgres.NewNodeRunRepository(db, discardLogger())
	svc := "svc-exact-" + uuid.New().String()[:8]
	mk := func(table string) {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an", TableName: table,
			JobName: "j", Status: run.TaskStatusSucceeded, MaxRetries: 3, ManifestVersion: "m", ImageTag: "v",
			CreatedAt: time.Now().Add(-time.Minute),
		}))
	}
	mk("fct_orders")
	mk("fctxorders")    // substring sibling — must NOT match
	mk("fct_orders_v2") // suffixed variant — must NOT match

	hit, total, err := repo.ListNodes(ctx, "fct_orders", svc, "", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, total, "exact match -> only fct_orders, not fctxorders or fct_orders_v2")
	require.Len(t, hit, 1)
	assert.Equal(t, "fct_orders", hit[0].TableName)

	// match is case-insensitive
	hitCI, totalCI, err := repo.ListNodes(ctx, "FCT_ORDERS", svc, "", 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, totalCI)
	require.Len(t, hitCI, 1)
	assert.Equal(t, "fct_orders", hitCI[0].TableName)
}

// TestNodeRunRepository_ListNodeNames returns distinct table names (deduped
// across schemas), service-filtered, sorted ascending.
func TestNodeRunRepository_ListNodeNames(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	repo := postgres.NewNodeRunRepository(db, discardLogger())

	svcA := "svc-names-a-" + uuid.New().String()[:8]
	svcB := "svc-names-b-" + uuid.New().String()[:8]
	mk := func(svc, schema, table string) {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: schema, TableName: table,
			JobName: "j", Status: run.TaskStatusSucceeded, MaxRetries: 3, ManifestVersion: "m", ImageTag: "v",
			CreatedAt: time.Now().Add(-time.Minute),
		}))
	}
	// svcA: orders + customers (customers in two schemas -> dedup to one)
	mk(svcA, "an", "orders")
	mk(svcA, "an", "customers")
	mk(svcA, "marts", "customers")
	// svcB: revenue
	mk(svcB, "an", "revenue")

	// service filter -> only svcA names, deduped + sorted
	namesA, err := repo.ListNodeNames(ctx, svcA)
	require.NoError(t, err)
	assert.Equal(t, []string{"customers", "orders"}, namesA)

	// filtering by svcB keeps the assertion deterministic in a shared DB
	namesB, err := repo.ListNodeNames(ctx, svcB)
	require.NoError(t, err)
	assert.Equal(t, []string{"revenue"}, namesB)
}

// TestNodeRunRepository_ListNodes_EmptyPageKeepsTotal verifies an empty page
// (offset beyond the end) still returns the true total_count.
func TestNodeRunRepository_ListNodes_EmptyPageKeepsTotal(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	schedRepo := postgres.NewSchedulerTrackerRepository(db, discardLogger())
	taskRepo := postgres.NewTaskTrackerRepository(db, discardLogger())
	repo := postgres.NewNodeRunRepository(db, discardLogger())
	svc := "svc-emp-" + uuid.New().String()[:8]
	for i := 0; i < 3; i++ {
		sid := uuid.New()
		require.NoError(t, schedRepo.Create(ctx, &postgres.SchedulerTracker{
			ScheduleID: sid, ScheduleName: "s", Status: run.SchedulerStatusSucceeded, Kind: "cron",
			CreatedAt: time.Now().Add(-time.Duration(i) * time.Minute), InitializationStatus: "completed",
		}))
		t.Cleanup(func() { db.ExecContext(ctx, "DELETE FROM scheduler_tracker WHERE schedule_id = $1", sid) })
		require.NoError(t, taskRepo.Create(ctx, &postgres.TaskTracker{
			TaskID: uuid.New(), ScheduleID: sid, ServiceName: svc, SchemaName: "an",
			TableName: "t" + uuid.New().String()[:6], JobName: "j", Status: run.TaskStatusSucceeded,
			MaxRetries: 3, ManifestVersion: "m", ImageTag: "v", CreatedAt: time.Now().Add(-time.Duration(i) * time.Minute),
		}))
	}
	page, total, err := repo.ListNodes(ctx, "", svc, "", 2, 5) // offset past the 3 rows
	require.NoError(t, err)
	assert.Empty(t, page)
	assert.Equal(t, 3, total, "total_count must survive an empty page")
}
