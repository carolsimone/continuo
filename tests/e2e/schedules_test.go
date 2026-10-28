package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	// The fires PreviewSchedule returns are checked on Paris's wall clock; the
	// embedded zone database keeps that check the same whatever host runs it.
	_ "time/tzdata"

	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

const (
	// scheduleEditCron and scheduleEditTimezone are the cron the schedule test
	// saves: 03:00 every day, in Paris.
	scheduleEditCron     = "0 3 * * *"
	scheduleEditTimezone = "Europe/Paris"
	// scheduleEditor is the identity the schedule test edits as.
	scheduleEditor = "e2e"
)

// TestE2E_Schedules_UpsertPreviewAndDeleteOnAPausedSchedule drives the
// schedule RPCs against the deployed state, on e2e-schedule, whose cron the
// test creates paused, so nothing fires and no other test's StartRun of the
// schedule meets an active cron run:
//
//  1. UpsertSchedule(e2e-schedule, "0 3 * * *", Europe/Paris, paused) saves
//     version 1 as the caller's identity, with no next fire.
//  2. ListSchedules lists it paused, with no next fire, beside the live
//     release's models of it.
//  3. PreviewSchedule returns three fires, each at 03:00 in Paris, on
//     consecutive days.
//  4. DeleteSchedule makes it manual: still listed, as the live release has
//     its models, with no cron.
//  5. No run of e2e-schedule was created.
//
// The schedule has no cron before the test and none after it, whatever the
// test's outcome.
func TestE2E_Schedules_UpsertPreviewAndDeleteOnAPausedSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	clients := setupClients(t, ctx)
	// Cleanups run last in, first out: registered first, the close runs after
	// the row's deletion registered below.
	t.Cleanup(func() { clients.close(ctx) })

	verifyServicesHealthy(t)

	// Only a schedule's first cron needs a model of the live release with its
	// tag; the announced e2e DAG has the models of e2e-schedule.
	seedTopology(t, ctx, clients)
	wantModels := dagModelsOf(testScheduleName)

	require.NoError(t, deleteScheduleRow(ctx, clients, testScheduleName), "start with %s manual", testScheduleName)
	t.Cleanup(func() {
		// A row left behind gives e2e-schedule a cron in every later test.
		if err := deleteScheduleRow(context.Background(), clients, testScheduleName); err != nil {
			t.Errorf("cleanup of schedule %s: %v", testScheduleName, err)
		}
	})
	runsBefore := runsOfSchedule(t, ctx, clients, testScheduleName)
	editCtx := metadata.AppendToOutgoingContext(ctx, identity.MetadataKey, scheduleEditor)

	// ── 1. UpsertSchedule creates the cron, paused ───────────────────────────
	saved, err := clients.runClient.UpsertSchedule(editCtx, &runv1.UpsertScheduleRequest{
		ScheduleName: testScheduleName,
		Cron:         scheduleEditCron,
		Timezone:     scheduleEditTimezone,
		Description:  "e2e: a paused schedule",
		Paused:       true,
	})
	require.NoError(t, err, "UpsertSchedule(%s), a name the live release has %d models of", testScheduleName, wantModels)
	s := saved.GetSchedule()
	assert.Equal(t, testScheduleName, s.GetScheduleName())
	assert.Equal(t, scheduleEditCron, s.GetCron())
	assert.Equal(t, scheduleEditTimezone, s.GetTimezone())
	assert.True(t, s.GetPaused(), "the cron is saved paused")
	assert.Equal(t, int64(1), s.GetVersion(), "a schedule's first cron is version 1")
	assert.Equal(t, scheduleEditor, s.GetUpdatedBy(), "the edit is recorded as the caller's identity")
	assert.Nil(t, s.GetNextFireAt(), "a paused schedule has no next fire")
	assert.Empty(t, saved.GetNextFireTimes(), "a paused schedule has no upcoming fires")
	t.Logf("✅ UpsertSchedule saved %s paused: %s", testScheduleName, describeSchedule(ctx, clients, testScheduleName))

	// ── 2. ListSchedules shows it paused, with no next fire ──────────────────
	listed, err := findListedSchedule(ctx, clients, testScheduleName)
	require.NoError(t, err, "ListSchedules")
	require.NotNil(t, listed, "ListSchedules lists %s: %s", testScheduleName, describeSchedule(ctx, clients, testScheduleName))
	assert.Equal(t, scheduleEditCron, listed.GetCron())
	assert.Equal(t, scheduleEditTimezone, listed.GetTimezone())
	assert.True(t, listed.GetPaused(), "listed paused")
	assert.Nil(t, listed.GetNextFireAt(), "listed with no next fire")
	assert.Equal(t, int64(1), listed.GetVersion())
	assert.Equal(t, scheduleEditor, listed.GetUpdatedBy())
	assert.Equal(t, wantModels, int(listed.GetModelCount()), "listed with the live release's models of it")
	var storedNext sql.NullTime
	require.NoError(t, clients.stateDB.GetContext(ctx, &storedNext,
		`SELECT next_fire_at FROM schedules WHERE tenant_id = $1 AND name = $2`, events.DefaultTenantID, testScheduleName))
	assert.False(t, storedNext.Valid, "the row the cron loop reads has no next fire: %s", describeSchedule(ctx, clients, testScheduleName))
	t.Logf("✅ ListSchedules lists %s paused with no next fire and %d models", testScheduleName, listed.GetModelCount())

	// ── 3. PreviewSchedule returns the cron's next three fires ───────────────
	preview, err := clients.runClient.PreviewSchedule(ctx, &runv1.PreviewScheduleRequest{
		Cron:     scheduleEditCron,
		Timezone: scheduleEditTimezone,
	})
	require.NoError(t, err, "PreviewSchedule(%q, %s)", scheduleEditCron, scheduleEditTimezone)
	assert.NotEmpty(t, preview.GetCronWords(), "the cron is described in words")
	fires := preview.GetNextFireTimes()
	require.Len(t, fires, 3, "PreviewSchedule returns the next three fires")
	paris, err := time.LoadLocation(scheduleEditTimezone)
	require.NoError(t, err)
	previewedAt := time.Now()
	var previous time.Time
	onParisClock := make([]string, 0, len(fires))
	for i, f := range fires {
		at := f.AsTime().In(paris)
		onParisClock = append(onParisClock, at.Format(time.DateTime+" MST"))
		assert.Equal(t, "03:00:00", at.Format(time.TimeOnly), "fire %d is at 03:00 in Paris: %s", i, at)
		if i == 0 {
			assert.True(t, at.After(previewedAt.Add(-time.Minute)) && at.Before(previewedAt.Add(25*time.Hour)),
				"the first fire is the next 03:00 in Paris after the preview: %s", at)
		} else {
			assert.Equal(t, previous.AddDate(0, 0, 1).Format(time.DateOnly), at.Format(time.DateOnly),
				"fire %d is the day after fire %d: %s, %s", i, i-1, previous, at)
		}
		previous = at
	}
	t.Logf("✅ PreviewSchedule: %q, next fires %s", preview.GetCronWords(), strings.Join(onParisClock, ", "))

	// ── 4. DeleteSchedule makes it manual ────────────────────────────────────
	_, err = clients.runClient.DeleteSchedule(editCtx, &runv1.DeleteScheduleRequest{ScheduleName: testScheduleName})
	require.NoError(t, err, "DeleteSchedule(%s)", testScheduleName)
	manual, err := findListedSchedule(ctx, clients, testScheduleName)
	require.NoError(t, err, "ListSchedules")
	require.NotNil(t, manual, "with no cron, %s is still listed: the live release has %d models of it", testScheduleName, wantModels)
	assert.Empty(t, manual.GetCron(), "a manual schedule has no cron")
	assert.Empty(t, manual.GetTimezone(), "a manual schedule has no timezone")
	assert.False(t, manual.GetPaused(), "a manual schedule is not paused")
	assert.Nil(t, manual.GetNextFireAt(), "a manual schedule has no next fire")
	assert.Zero(t, manual.GetVersion(), "a manual schedule has no version")
	assert.Empty(t, manual.GetUpdatedBy(), "a manual schedule has no editor")
	assert.Nil(t, manual.GetUpdatedAt(), "a manual schedule has no edit time")
	assert.Equal(t, wantModels, int(manual.GetModelCount()), "still listed with the live release's models of it")
	assert.Equal(t, fmt.Sprintf("schedule %s has no row", testScheduleName), describeSchedule(ctx, clients, testScheduleName),
		"DeleteSchedule removes the row")
	t.Logf("✅ DeleteSchedule left %s manual, listed with %d models", testScheduleName, manual.GetModelCount())

	// ── 5. No run was created ────────────────────────────────────────────────
	assert.Equal(t, runsBefore, runsOfSchedule(t, ctx, clients, testScheduleName),
		"saving a paused cron, previewing a cron and removing the cron start no run")
}

// dagModelsOf counts the nodes of the e2e DAG whose schedule is name.
func dagModelsOf(name string) int {
	n := 0
	for _, node := range e2eDAG {
		if node.schedule == name {
			n++
		}
	}
	return n
}

// findListedSchedule returns the entry ListSchedules lists for name, or nil
// when it lists none.
func findListedSchedule(ctx context.Context, clients *testClients, name string) (*runv1.ScheduleSummary, error) {
	resp, err := clients.runClient.ListSchedules(ctx, &runv1.ListSchedulesRequest{})
	if err != nil {
		return nil, err
	}
	for _, s := range resp.GetSchedules() {
		if s.GetScheduleName() == name {
			return s, nil
		}
	}
	return nil, nil
}

// deleteScheduleRow deletes the default tenant's schedules row of name, which
// leaves the schedule with no cron.
func deleteScheduleRow(ctx context.Context, clients *testClients, name string) error {
	if _, err := clients.stateDB.ExecContext(ctx,
		`DELETE FROM schedules WHERE tenant_id = $1 AND name = $2`, events.DefaultTenantID, name); err != nil {
		return fmt.Errorf("delete the schedules row of %s: %w", name, err)
	}
	return nil
}

// describeSchedule renders the default tenant's schedules row of name for a
// failure message, or says it has none.
func describeSchedule(ctx context.Context, clients *testClients, name string) string {
	var row struct {
		Cron       string       `db:"cron"`
		Timezone   string       `db:"timezone"`
		Paused     bool         `db:"paused"`
		NextFireAt sql.NullTime `db:"next_fire_at"`
		Version    int64        `db:"version"`
		UpdatedBy  string       `db:"updated_by"`
		UpdatedAt  time.Time    `db:"updated_at"`
	}
	err := clients.stateDB.GetContext(ctx, &row, `
		SELECT cron, timezone, paused, next_fire_at, version, updated_by, updated_at
		FROM schedules WHERE tenant_id = $1 AND name = $2`, events.DefaultTenantID, name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Sprintf("schedule %s has no row", name)
	case err != nil:
		return fmt.Sprintf("schedule %s: reading its row failed: %v", name, err)
	}
	next := "none"
	if row.NextFireAt.Valid {
		next = row.NextFireAt.Time.UTC().Format(time.RFC3339Nano)
	}
	return fmt.Sprintf("schedule %s row: cron %q, timezone %s, paused %t, next_fire_at %s, version %d, updated_by %q, updated_at %s",
		name, row.Cron, row.Timezone, row.Paused, next, row.Version, row.UpdatedBy, row.UpdatedAt.UTC().Format(time.RFC3339Nano))
}

// describeRuns renders every run of the default tenant's schedule name for a
// failure message.
func describeRuns(ctx context.Context, clients *testClients, name string) string {
	var rows []struct {
		RunID         string         `db:"run_id"`
		Kind          string         `db:"kind"`
		Status        string         `db:"status"`
		FailureReason sql.NullString `db:"failure_reason"`
		ScheduledFor  sql.NullTime   `db:"scheduled_for"`
		CreatedAt     time.Time      `db:"created_at"`
	}
	if err := clients.stateDB.SelectContext(ctx, &rows, `
		SELECT run_id, kind, status, failure_reason, scheduled_for, created_at
		FROM runs WHERE tenant_id = $1 AND schedule_name = $2
		ORDER BY created_at`, events.DefaultTenantID, name); err != nil {
		return fmt.Sprintf("the runs of %s could not be read: %v", name, err)
	}
	if len(rows) == 0 {
		return fmt.Sprintf("schedule %s has no run", name)
	}
	described := make([]string, 0, len(rows))
	for _, r := range rows {
		slot := "none"
		if r.ScheduledFor.Valid {
			slot = r.ScheduledFor.Time.UTC().Format(time.RFC3339Nano)
		}
		described = append(described, fmt.Sprintf("run %s (kind %s, status %s, failure_reason %q, scheduled_for %s, created_at %s)",
			r.RunID, r.Kind, r.Status, r.FailureReason.String, slot, r.CreatedAt.UTC().Format(time.RFC3339Nano)))
	}
	return fmt.Sprintf("runs of %s: %s", name, strings.Join(described, "; "))
}
