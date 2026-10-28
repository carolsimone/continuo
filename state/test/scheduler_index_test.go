package test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
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

// TestConcurrentActivation_OneWinnerOneActiveRunError starts two runs of the
// same schedule at once. The partial unique index uq_runs_active_per_schedule
// guarantees exactly one active run is inserted; the loser receives the same
// active-run error as the check-first path. This is the DB-level backstop for
// the activation TOCTOU race.
func TestConcurrentActivation_OneWinnerOneActiveRunError(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	rawDB, cleanup := setupPostgres(t)
	defer cleanup()

	h := newBuiltHarness(rawDB)
	promoteSchedules(t, rawDB, "race-schedule")

	const n = 2
	var wg sync.WaitGroup
	errs := make([]error, n)
	start := make(chan struct{})

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = h.start(context.Background(), pkgevents.DefaultTenantID, "race-schedule", identity.FromUserID("okta|alice"), model.OperationRun)
		}(i)
	}
	close(start)
	wg.Wait()

	activated, skipped := 0, 0
	for i := 0; i < n; i++ {
		switch {
		case errs[i] == nil:
			activated++
		case errors.Is(errs[i], run.ErrScheduleHasActiveRun):
			skipped++
		default:
			t.Fatalf("neither concurrent activation should hard-error: %v", errs[i])
		}
	}
	assert.Equal(t, 1, activated, "exactly one activation wins")
	assert.Equal(t, 1, skipped, "the loser is treated as active-run-exists")

	// Exactly one active (pending|running) row survives in the database.
	var active int
	require.NoError(t, rawDB.Get(&active, `
		SELECT count(*) FROM runs
		WHERE schedule_name = 'race-schedule' AND status IN ('pending','running')
	`))
	assert.Equal(t, 1, active, "the partial unique index allows only one active run")
}

// TestActiveScheduleLookupUsesIndex asserts the active-run lookup of a
// tenant's schedule, as HasActiveRun queries it, reads an index of runs
// (idx_runs_schedule_name_created or the partial uq_runs_active_per_schedule)
// rather than scanning the table.
func TestActiveScheduleLookupUsesIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	rawDB, cleanup := setupPostgres(t)
	defer cleanup()

	for i := 0; i < 200; i++ {
		_, err := rawDB.Exec(`
			INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at, completed_at)
			VALUES ('default', gen_random_uuid(), $1, 'succeeded', NOW() - ($2 || ' minutes')::interval, NOW())
		`, fmt.Sprintf("sched-%d", i%20), i)
		require.NoError(t, err)
	}
	_, err := rawDB.Exec(`ANALYZE runs`)
	require.NoError(t, err)

	plan := explainWithoutSeqScan(t, rawDB, `
		SELECT EXISTS(
			SELECT 1 FROM runs
			WHERE tenant_id = 'default' AND schedule_name = 'sched-3'
			  AND status = ANY('{pending,running,cancelling}'::text[]))
	`)
	assert.Regexp(t, "idx_runs_schedule_name_created|uq_runs_active_per_schedule", plan,
		"HasActiveRun must be able to use an index of the schedule's runs; plan was:\n%s", plan)
	assert.NotContains(t, plan, "Seq Scan", "plan was:\n%s", plan)
}

// TestGetLastRunPerSchedule_UsesIndex asserts the DISTINCT ON last-run scan,
// hit by ListSchedules, walks the schedule_name index.
func TestGetLastRunPerSchedule_UsesIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	rawDB, cleanup := setupPostgres(t)
	defer cleanup()

	for i := 0; i < 200; i++ {
		_, err := rawDB.Exec(`
			INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at, completed_at)
			VALUES ('default', gen_random_uuid(), $1, 'succeeded', NOW() - ($2 || ' minutes')::interval, NOW())
		`, fmt.Sprintf("sched-%d", i%20), i)
		require.NoError(t, err)
	}
	_, err := rawDB.Exec(`ANALYZE runs`)
	require.NoError(t, err)

	plan := explainWithoutSeqScan(t, rawDB, `
		SELECT DISTINCT ON (schedule_name) schedule_name, run_id, status, created_at
		FROM runs
		WHERE tenant_id = 'default'
		ORDER BY schedule_name, created_at DESC
	`)
	assert.Contains(t, plan, "idx_runs_schedule_name_created",
		"DISTINCT ON last-run scan must be able to use the schedule_name index; plan was:\n%s", plan)
}
