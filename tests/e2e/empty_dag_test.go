package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/events"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// emptyDAGTimezone is the zone the crons of the empty-DAG schedules are read
// in.
const emptyDAGTimezone = "Europe/Paris"

// TestEmptyDAG_ManualStartIsRefused guards a manual start of a schedule the
// live release has no node of, whatever its operation. With no cron either,
// state does not know the name, and StartRun is NotFound. Once the name has a
// cron, StartRun knows it, and refuses its empty selection with
// FailedPrecondition: a test run of a schedule with no node is an empty
// selection, not a skipped run. Neither creates a run.
//
// Fixture: a unique schedule name, whose cron is a paused schedules row the
// test inserts by SQL, as UpsertSchedule refuses the first cron of a name the
// live release has no model of. Paused, the row never fires. state selects a
// schedule's nodes from the live release by schedule_name, so the test needs
// neither topology-controller nor a change to the dbt DAG.
func TestEmptyDAG_ManualStartIsRefused(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	// Cleanups run last in, first out: registered first, the close runs after
	// the deletions registered below.

	verifyServicesHealthy(t)

	// A unique schedule name isolates the test from other runs and from the
	// shared e2e schedules.
	scheduleName := fmt.Sprintf("empty-dag-%s", uuid.NewString()[:8])
	t.Logf("empty-DAG test schedule name: %s", scheduleName)
	t.Cleanup(func() { deleteScheduleAndRuns(t, clients, scheduleName) })

	startEach := func(want codes.Code, why string) {
		t.Helper()
		for _, operation := range []string{"", "test"} {
			_, err := clients.runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: uuid.NewString(),
				ScheduleName: scheduleName,
				Operation:    operation,
			})
			require.Error(t, err, "StartRun(%q, operation=%q) must be refused: %s", scheduleName, operation, why)
			assert.Equal(t, want, status.Code(err), "operation=%q, %s: %v", operation, why, err)
		}
	}

	startEach(codes.NotFound, "no model and no cron, so state does not know the name")

	_, err := clients.stateDB.ExecContext(ctx, `
		INSERT INTO schedules (tenant_id, name, cron, timezone, paused, updated_by)
		VALUES ($1, $2, '0 3 * * *', $3, true, 'e2e')`,
		events.DefaultTenantID, scheduleName, emptyDAGTimezone)
	require.NoError(t, err, "INSERT the paused schedules row of %s", scheduleName)

	startEach(codes.FailedPrecondition, "a cron makes the name known, and its selection is empty")
	assert.Zero(t, runsOfSchedule(t, ctx, clients, scheduleName), "a refused start creates no run")
}

// cronFireWait is how long TestEmptyDAG_ACronFireRecordsAFailedRun waits for
// the fire. The loop wakes on the notification the INSERT sends and sleeps
// until the slot five seconds later; when that notification is lost, the
// pass at the loop's 60 s tick fires the slot, up to one loop tick (60 s)
// after the insert. The wait leaves headroom over that.
const cronFireWait = 90 * time.Second

// TestEmptyDAG_ACronFireRecordsAFailedRun drives one real fire of state's cron
// loop for a schedule the live release has no node of. The test inserts the
// schedule's row by SQL, as UpsertSchedule refuses the first cron of a name
// with no model: its next fire is five seconds ahead on the database's clock,
// and its cron fires once a year, on yesterday's date, so the slot the test
// wrote is the only one that can fire while it runs. Within cronFireWait the
// loop fires that slot: one run of kind cron, stamped with the slot, failed
// with failure_reason empty_selection. The same transaction moves the schedule's
// next fire on to its cron's next slot and leaves the version the row was
// written with.
func TestEmptyDAG_ACronFireRecordsAFailedRun(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	// Cleanups run last in, first out: registered first, the close runs after
	// the deletions registered below.

	verifyServicesHealthy(t)

	scheduleName := fmt.Sprintf("empty-dag-cron-%s", uuid.NewString()[:8])
	t.Cleanup(func() { deleteScheduleAndRuns(t, clients, scheduleName) })

	cron := yearlyCronOnYesterday(t, ctx, clients, emptyDAGTimezone)
	var slot time.Time
	require.NoError(t, clients.stateDB.GetContext(ctx, &slot, `
		INSERT INTO schedules (tenant_id, name, cron, timezone, updated_by, next_fire_at)
		VALUES ($1, $2, $3, $4, 'e2e', now() + interval '5 seconds')
		RETURNING next_fire_at`,
		events.DefaultTenantID, scheduleName, cron, emptyDAGTimezone),
		"INSERT the schedules row of %s", scheduleName)
	t.Logf("schedule %s: cron %q in %s, next fire %s", scheduleName, cron, emptyDAGTimezone, slot.UTC().Format(time.RFC3339Nano))

	var runs []*runv1.RunSummary
	pollUntilDescribed(t, ctx, cronFireWait, time.Second, func() (bool, error) {
		resp, err := clients.runClient.ListRuns(ctx, &runv1.ListRunsRequest{ScheduleName: scheduleName})
		if err != nil {
			return false, err
		}
		runs = resp.GetRuns()
		return len(runs) > 0, nil
	}, func() string {
		return fmt.Sprintf("ListRuns(%s) listed no run %s after its next fire was set 5 s ahead: %s; %s",
			scheduleName, cronFireWait, describeSchedule(ctx, clients, scheduleName), describeRuns(ctx, clients, scheduleName))
	})
	require.Len(t, runs, 1, "the slot fires one run: %s", describeRuns(ctx, clients, scheduleName))

	got, err := clients.runClient.GetRun(ctx, &runv1.GetRunRequest{RunId: runs[0].GetRunId()})
	require.NoError(t, err, "GetRun(%s)", runs[0].GetRunId())
	cronRun := got.GetRun()
	t.Logf("the fire recorded run %s: kind %s, status %s, failure_reason %s", cronRun.GetRunId(), cronRun.GetKind(), cronRun.GetStatus(), cronRun.GetFailureReason())
	assert.Equal(t, "cron", cronRun.GetKind(), "a fire of the cron loop records a cron run")
	assert.Equal(t, "failed", cronRun.GetStatus(), "a cron fire with nothing to run records a failed run")
	assert.Equal(t, "empty_selection", cronRun.GetFailureReason(), "the live release has no node of %s", scheduleName)

	var fired struct {
		ScheduledFor *time.Time `db:"scheduled_for"`
		NextFireAt   *time.Time `db:"next_fire_at"`
		Version      int64      `db:"version"`
	}
	require.NoError(t, clients.stateDB.GetContext(ctx, &fired, `
		SELECT r.scheduled_for, s.next_fire_at, s.version
		FROM runs r
		JOIN schedules s ON s.tenant_id = r.tenant_id AND s.name = r.schedule_name
		WHERE r.tenant_id = $1 AND r.run_id = $2`,
		events.DefaultTenantID, cronRun.GetRunId()),
		"read the run's slot and the schedule's row: %s", describeSchedule(ctx, clients, scheduleName))
	require.NotNil(t, fired.ScheduledFor, "a cron run is stamped with its slot")
	assert.True(t, fired.ScheduledFor.Equal(slot), "the run is the slot's: scheduled_for %s, slot %s", fired.ScheduledFor, slot)
	require.NotNil(t, fired.NextFireAt, "the fire moves the next fire on: %s", describeSchedule(ctx, clients, scheduleName))
	assert.True(t, fired.NextFireAt.After(slot.Add(24*time.Hour)),
		"the next fire is the cron's next slot, almost a year away: %s", fired.NextFireAt)
	assert.Equal(t, int64(1), fired.Version, "a fire leaves the version of the row as it was written")
}

// yearlyCronOnYesterday returns a cron that fires once a year, at noon on
// yesterday's date in tz, read from the database's clock: its next slot is
// almost a year away (four years when yesterday was 29 February).
func yearlyCronOnYesterday(t *testing.T, ctx context.Context, clients *testClients, tz string) string {
	t.Helper()
	var day, month int
	require.NoError(t, clients.stateDB.QueryRowContext(ctx, `
		SELECT extract(day FROM d)::int, extract(month FROM d)::int
		FROM (SELECT (now() AT TIME ZONE $1::text) - interval '1 day' AS d) AS yesterday`, tz).Scan(&day, &month),
		"read yesterday's date in %s", tz)
	return fmt.Sprintf("0 12 %d %d *", day, month)
}

// deleteScheduleAndRuns deletes the default tenant's schedules row of name,
// so it fires no more, then the runs of name with their tasks and outbox rows.
// It runs as a t.Cleanup registered after the one that closes the clients, so
// it runs before that close. It attempts every statement, then fails the test
// with each one that failed: a row left behind can fire during a later test.
func deleteScheduleAndRuns(t *testing.T, clients *testClients, name string) {
	t.Helper()
	bg := context.Background()
	var errs []error
	if err := deleteScheduleRow(bg, clients, name); err != nil {
		errs = append(errs, err)
	}
	var runIDs []string
	if err := clients.stateDB.SelectContext(bg, &runIDs,
		`SELECT run_id FROM runs WHERE tenant_id = $1 AND schedule_name = $2`, events.DefaultTenantID, name); err != nil {
		errs = append(errs, fmt.Errorf("list the runs of %s: %w", name, err))
	}
	for _, runID := range runIDs {
		if _, err := clients.stateDB.ExecContext(bg,
			`DELETE FROM state_outbox WHERE aggregate_id = $1`, runID); err != nil {
			errs = append(errs, fmt.Errorf("delete the state_outbox rows of run %s: %w", runID, err))
		}
	}
	if _, err := clients.stateDB.ExecContext(bg, `
		DELETE FROM tasks WHERE tenant_id = $1 AND run_id IN (
			SELECT run_id FROM runs WHERE tenant_id = $1 AND schedule_name = $2)`, events.DefaultTenantID, name); err != nil {
		errs = append(errs, fmt.Errorf("delete the tasks of %s: %w", name, err))
	}
	if _, err := clients.stateDB.ExecContext(bg,
		`DELETE FROM runs WHERE tenant_id = $1 AND schedule_name = $2`, events.DefaultTenantID, name); err != nil {
		errs = append(errs, fmt.Errorf("delete the runs of %s: %w", name, err))
	}
	for _, err := range errs {
		t.Errorf("cleanup of schedule %s: %v", name, err)
	}
}
