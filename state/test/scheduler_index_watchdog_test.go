package test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	svchandlers "github.com/carolsimone/continuo/state/service/handlers"
	svcports "github.com/carolsimone/continuo/state/service/ports"
	"github.com/carolsimone/continuo/state/service/uow"
)

// nopWriter discards log output in these tests.
type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// explainWithoutSeqScan returns the joined EXPLAIN plan rows for q on a session
// where sequential scans are disabled. On the tiny tables these tests seed, the
// cost-based planner would pick a seq scan regardless of available indexes;
// disabling seq scans makes the assertion deterministic — it verifies the index
// is *usable* for the query's predicate/ordering, which is what we care about.
func explainWithoutSeqScan(t *testing.T, db *sqlx.DB, q string) string {
	t.Helper()
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(context.Background(), "SET enable_seqscan = off")
	require.NoError(t, err)

	rows, err := conn.QueryContext(context.Background(), "EXPLAIN "+q)
	require.NoError(t, err)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n")
}

// newActivationStack wires the repositories, UoW factory, and activation handler
// against the test database. removed_at IS NULL rows in schedule_catalog are the
// activation precondition; the named schedules are seeded as active.
func newActivationStack(t *testing.T, db *sqlx.DB, names ...string) (func() uow.UnitOfWork, *svchandlers.ActivateScheduleHandler) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))
	schedulerRepo := postgres.NewSchedulerTrackerRepository(db, logger)
	taskRepo := postgres.NewTaskTrackerRepository(db, logger)
	execRepo := postgres.NewTaskExecutionRepository(db, logger)
	catalogRepo := postgres.NewScheduleCatalogRepository(db, logger)

	for _, n := range names {
		_, err := db.Exec(`
			INSERT INTO schedule_catalog (schedule_name, first_seen_at, last_seen_at, removed_at, service_metadata)
			VALUES ($1, NOW(), NOW(), NULL, '{}')
			ON CONFLICT (schedule_name) DO NOTHING
		`, n)
		require.NoError(t, err)
	}

	clk := svcports.SystemClock{}
	factory := func() uow.UnitOfWork {
		return postgres.NewPostgresUnitOfWork(db, schedulerRepo, taskRepo, execRepo, catalogRepo, clk, logger)
	}
	return factory, svchandlers.NewActivateScheduleHandler(logger)
}

// TestConcurrentActivation_OneWinnerOneActiveRunError fires two simultaneous
// activations for the same schedule. The partial unique index
// uq_scheduler_tracker_active_per_schedule guarantees exactly one active run is
// inserted; the loser receives the same active-run outcome as the check-first
// path. This is the DB-level backstop for the activation TOCTOU race.
func TestConcurrentActivation_OneWinnerOneActiveRunError(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	rawDB, cleanup := setupPostgres(t)
	defer cleanup()

	factory, handler := newActivationStack(t, rawDB, "race-schedule")

	const n = 2
	var wg sync.WaitGroup
	outcomes := make([]svchandlers.ActivationOutcome, n)
	errs := make([]error, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, outcome, err := handler.Handle(context.Background(), factory(), "race-schedule", run.KindCron, nil, identity.System(), model.OperationRun)
			outcomes[idx] = outcome
			errs[idx] = err
		}(i)
	}
	close(start)
	wg.Wait()

	activated, skipped := 0, 0
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i], "neither concurrent activation should hard-error")
		switch outcomes[i] {
		case svchandlers.OutcomeActivated:
			activated++
		case svchandlers.OutcomeSkippedActive:
			skipped++
		}
	}
	assert.Equal(t, 1, activated, "exactly one activation wins")
	assert.Equal(t, 1, skipped, "the loser is treated as active-run-exists")

	// Exactly one active (pending|running) row survives in the database.
	var active int
	require.NoError(t, rawDB.Get(&active, `
		SELECT count(*) FROM scheduler_tracker
		WHERE schedule_name = 'race-schedule' AND status IN ('pending','running')
	`))
	assert.Equal(t, 1, active, "the partial unique index allows only one active run")
}

// TestActivePerScheduleIndexUsedByHasActiveSchedule asserts the active-run
// lookup uses an index scan over idx_scheduler_tracker_schedule_name_created
// rather than a sequential scan.
func TestActiveScheduleLookupUsesIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	rawDB, cleanup := setupPostgres(t)
	defer cleanup()

	for i := 0; i < 200; i++ {
		_, err := rawDB.Exec(`
			INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, completed_at)
			VALUES (gen_random_uuid(), $1, 'succeeded', NOW() - ($2 || ' minutes')::interval, NOW())
		`, fmt.Sprintf("sched-%d", i%20), i)
		require.NoError(t, err)
	}
	_, err := rawDB.Exec(`ANALYZE scheduler_tracker`)
	require.NoError(t, err)

	plan := explainWithoutSeqScan(t, rawDB, `
		SELECT 1 FROM scheduler_tracker
		WHERE schedule_name = 'sched-3' AND status IN ('pending','running')
		ORDER BY created_at DESC LIMIT 1
	`)
	assert.Contains(t, plan, "idx_scheduler_tracker_schedule_name_created",
		"HasActiveSchedule must be able to use the schedule_name index; plan was:\n%s", plan)
}

// TestGetLastRunPerSchedule_UsesIndex asserts the DISTINCT ON last-run scan,
// hit by ListAllSchedules and the watchdog, walks the schedule_name index.
func TestGetLastRunPerSchedule_UsesIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	rawDB, cleanup := setupPostgres(t)
	defer cleanup()

	for i := 0; i < 200; i++ {
		_, err := rawDB.Exec(`
			INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, completed_at)
			VALUES (gen_random_uuid(), $1, 'succeeded', NOW() - ($2 || ' minutes')::interval, NOW())
		`, fmt.Sprintf("sched-%d", i%20), i)
		require.NoError(t, err)
	}
	_, err := rawDB.Exec(`ANALYZE scheduler_tracker`)
	require.NoError(t, err)

	plan := explainWithoutSeqScan(t, rawDB, `
		SELECT DISTINCT ON (schedule_name) schedule_name, schedule_id, status, created_at
		FROM scheduler_tracker
		ORDER BY schedule_name, created_at DESC
	`)
	assert.Contains(t, plan, "idx_scheduler_tracker_schedule_name_created",
		"DISTINCT ON last-run scan must be able to use the schedule_name index; plan was:\n%s", plan)
}

// TestListStuckCandidates_ConsidersAllTasksBeyondFiftyRows is the regression
// for the watchdog false-stuck bug: the prior watchdog only saw the newest 50
// tasks (ListTasks default page), so a RUNNING task at position 51 of a wide
// run made a healthy schedule falsely stuck-eligible. The server-side
// aggregation considers ALL tasks, so a single RUNNING task anywhere excludes
// the run regardless of how many tasks precede it.
func TestListStuckCandidates_ConsidersAllTasksBeyondFiftyRows(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	rawDB, cleanup := setupPostgres(t)
	defer cleanup()
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))
	repo := postgres.NewSchedulerTrackerRepository(rawDB, logger)

	scheduleID := uuid.New()
	_, err := rawDB.Exec(`
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at)
		VALUES ($1, 'wide', 'running', NOW() - INTERVAL '90 minutes')
	`, scheduleID)
	require.NoError(t, err)

	// 60 stale non-running tasks (older than the cutoff) ...
	old := time.Now().Add(-60 * time.Minute)
	for i := 0; i < 60; i++ {
		_, err := rawDB.Exec(`
			INSERT INTO task_tracker (task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries)
			VALUES (gen_random_uuid(), $1, $2, 'svc', 'sch', $3, $3, 'succeeded', 3)
		`, scheduleID, old.Add(time.Duration(i)*time.Second), fmt.Sprintf("t%d", i))
		require.NoError(t, err)
	}
	// ... plus ONE running task, newer than all the others — the "51st+" row the
	// old paged watchdog could not see.
	_, err = rawDB.Exec(`
		INSERT INTO task_tracker (task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries)
		VALUES (gen_random_uuid(), $1, $2, 'svc', 'sch', 'running-one', 'running-one', 'running', 3)
	`, scheduleID, time.Now().Add(-1*time.Minute))
	require.NoError(t, err)

	cutoff := time.Now().Add(-30 * time.Minute)
	got, err := repo.ListStuckCandidates(context.Background(), cutoff)
	require.NoError(t, err)
	assert.Empty(t, got, "a RUNNING task anywhere in the run means it is not stuck, even past 50 tasks")
}

// TestListStuckCandidates_StalledRunIsCandidate confirms a run with only stale,
// non-running tasks IS reported, while a run with no tasks is not.
func TestListStuckCandidates_StalledRunIsCandidate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	rawDB, cleanup := setupPostgres(t)
	defer cleanup()
	logger := slog.New(slog.NewTextHandler(nopWriter{}, nil))
	repo := postgres.NewSchedulerTrackerRepository(rawDB, logger)

	stalled := uuid.New()
	_, err := rawDB.Exec(`
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at)
		VALUES ($1, 'stalled', 'running', NOW() - INTERVAL '90 minutes')
	`, stalled)
	require.NoError(t, err)
	_, err = rawDB.Exec(`
		INSERT INTO task_tracker (task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries)
		VALUES (gen_random_uuid(), $1, $2, 'svc', 'sch', 'pending-one', 'pending-one', 'pending', 3)
	`, stalled, time.Now().Add(-45*time.Minute))
	require.NoError(t, err)

	// A run with no tasks must never be reported (it has not started).
	notStarted := uuid.New()
	_, err = rawDB.Exec(`
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at)
		VALUES ($1, 'not-started', 'pending', NOW() - INTERVAL '90 minutes')
	`, notStarted)
	require.NoError(t, err)

	cutoff := time.Now().Add(-30 * time.Minute)
	got, err := repo.ListStuckCandidates(context.Background(), cutoff)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "stalled", got[0].ScheduleName)
	assert.Equal(t, stalled, got[0].ScheduleID)
}
