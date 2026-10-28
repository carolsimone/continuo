package test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/state/adapters/cron"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/schedule"
	"github.com/carolsimone/continuo/state/domain/topology"
	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
	"github.com/carolsimone/continuo/state/service/schedules"
)

// The schedules these tests save fire once a year, or are paused, so no pass
// of a cron loop sharing this database ever finds one of them due while it
// exists.
const (
	yearlyCron = "0 3 1 1 *"
	parisZone  = "Europe/Paris"
)

// newScheduleName returns a schedule name no other test uses, short enough to
// be a run's schedule name, whose rows of the default tenant are deleted when
// the test ends.
func newScheduleName(t *testing.T, prefix string) string {
	t.Helper()
	name := prefix + "-" + uuid.NewString()[:8]
	deleteScheduleRowsAtEnd(t, pkgevents.DefaultTenantID, name)
	return name
}

// deleteScheduleRowsAtEnd deletes the tenant's schedule rows of names when the
// test ends.
func deleteScheduleRowsAtEnd(t *testing.T, tenant string, names ...string) {
	t.Helper()
	t.Cleanup(func() {
		for _, name := range names {
			_, _ = stateDB.Exec(`DELETE FROM schedules WHERE tenant_id = $1 AND name = $2`, tenant, name)
		}
	})
}

// asUser is the context of a call whose identity metadata names user.
func asUser(user string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), identity.MetadataKey, user)
}

// scheduleRowsOf counts the default tenant's schedule rows of name.
func scheduleRowsOf(t *testing.T, name string) int {
	t.Helper()
	return countOf(t, stateDB, `SELECT count(*) FROM schedules WHERE tenant_id = 'default' AND name = $1`, name)
}

// summaryOf returns the ListSchedules entry of name, nil when there is none.
func summaryOf(t *testing.T, name string) *runv1.ScheduleSummary {
	t.Helper()
	resp, err := runClient.ListSchedules(context.Background(), &runv1.ListSchedulesRequest{})
	require.NoError(t, err)
	for _, s := range resp.GetSchedules() {
		if s.GetScheduleName() == name {
			return s
		}
	}
	return nil
}

// upsertYearly saves name's cron as a yearly one, edited by user, with the
// expected version given, and returns the response.
func upsertYearly(t *testing.T, user, name string, expected int64, paused bool) *runv1.UpsertScheduleResponse {
	t.Helper()
	resp, err := runClient.UpsertSchedule(asUser(user), &runv1.UpsertScheduleRequest{
		ScheduleName: name, Cron: yearlyCron, Timezone: parisZone, Paused: paused, ExpectedVersion: expected,
	})
	require.NoError(t, err)
	return resp
}

// testParser is the parser state fires crons with.
func testParser(t *testing.T) *cron.Parser {
	t.Helper()
	parser, err := cron.NewParser()
	require.NoError(t, err)
	return parser
}

// promoteTenantRelease stores a release of tenant holding specs and makes it
// the tenant's live release; the tenant's releases are deleted when the test
// ends.
func promoteTenantRelease(t *testing.T, tenant string, specs ...topology.NodeSpec) {
	t.Helper()
	ctx := context.Background()
	releaseID := "rel-" + uuid.NewString()[:8]
	t.Cleanup(func() {
		_, _ = stateDB.Exec(`DELETE FROM live_release WHERE tenant_id = $1`, tenant)
		_, _ = stateDB.Exec(`DELETE FROM topology_releases WHERE tenant_id = $1`, tenant)
	})
	tx, err := stateDB.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback() //nolint:errcheck
	topo := postgres.NewTopologyRepository(stateDB, tx)
	require.NoError(t, topo.LockTopology(ctx))
	rel, err := topology.NewRelease(tenant, releaseID, 1,
		"s3://continuo/tenants/"+tenant+"/topologies/"+releaseID+"/topology.json.gz", "sha-"+releaseID, specs)
	require.NoError(t, err)
	_, err = topo.SaveRelease(ctx, rel)
	require.NoError(t, err)
	require.NoError(t, topo.SetLivePointer(ctx, tenant, topology.Pointer{ReleaseID: releaseID, PromotionSeq: 1}, time.Now()))
	require.NoError(t, tx.Commit())
}

// UpsertSchedule gives a name of the live release its first cron at version 1,
// authored by the caller, and answers with the schedule and its next three
// fires, the first of which is the stored next fire.
func TestSchedules_UpsertCreatesAndReturnsTheNextThreeFires(t *testing.T) {
	name := newScheduleName(t, "sched-create")
	promoteSchedules(t, stateDB, name)

	resp, err := runClient.UpsertSchedule(asUser("alice@example.com"), &runv1.UpsertScheduleRequest{
		ScheduleName: name, Cron: yearlyCron, Timezone: parisZone, Description: "Runs on new year's day",
	})

	require.NoError(t, err)
	got := resp.GetSchedule()
	assert.Equal(t, name, got.GetScheduleName())
	assert.Equal(t, yearlyCron, got.GetCron())
	assert.Equal(t, parisZone, got.GetTimezone())
	assert.Equal(t, "Runs on new year's day", got.GetDescription())
	assert.False(t, got.GetPaused())
	assert.Equal(t, int64(1), got.GetVersion())
	assert.Equal(t, "alice@example.com", got.GetUpdatedBy())

	expr, err := testParser(t).Parse(yearlyCron, parisZone)
	require.NoError(t, err)
	first := expr.Next(got.GetUpdatedAt().AsTime())
	want := []time.Time{first, expr.Next(first), expr.Next(expr.Next(first))}
	require.Len(t, resp.GetNextFireTimes(), 3)
	for i, at := range resp.GetNextFireTimes() {
		assert.True(t, want[i].Equal(at.AsTime()), "fire %d: got %s want %s", i, at.AsTime(), want[i])
	}
	assert.True(t, first.Equal(got.GetNextFireAt().AsTime()))

	var row struct {
		Cron       string       `db:"cron"`
		Timezone   string       `db:"timezone"`
		NextFireAt sql.NullTime `db:"next_fire_at"`
		Version    int64        `db:"version"`
		UpdatedBy  string       `db:"updated_by"`
	}
	require.NoError(t, stateDB.Get(&row, `
		SELECT cron, timezone, next_fire_at, version, updated_by
		  FROM schedules WHERE tenant_id = 'default' AND name = $1`, name))
	assert.Equal(t, yearlyCron, row.Cron)
	assert.Equal(t, parisZone, row.Timezone)
	require.True(t, row.NextFireAt.Valid)
	assert.True(t, first.Equal(row.NextFireAt.Time), "the stored next fire is the first fire answered")
	assert.Equal(t, int64(1), row.Version)
	assert.Equal(t, "alice@example.com", row.UpdatedBy)
}

// A call that names no user is recorded as the system's edit.
func TestSchedules_AnUnnamedCallerIsRecordedAsTheSystem(t *testing.T) {
	name := newScheduleName(t, "sched-system")
	promoteSchedules(t, stateDB, name)

	resp, err := runClient.UpsertSchedule(context.Background(), &runv1.UpsertScheduleRequest{
		ScheduleName: name, Cron: yearlyCron, Timezone: parisZone,
	})

	require.NoError(t, err)
	assert.Equal(t, identity.SystemUserID, resp.GetSchedule().GetUpdatedBy())
}

// A name no model of the live release has cannot get a first cron: the call
// is NotFound and writes nothing.
func TestSchedules_UpsertRefusesANewNameWithNoModels(t *testing.T) {
	name := newScheduleName(t, "sched-unknown")
	promoteSchedules(t, stateDB, "sched-other-"+uuid.NewString()[:8])

	_, err := runClient.UpsertSchedule(asUser("alice@example.com"), &runv1.UpsertScheduleRequest{
		ScheduleName: name, Cron: yearlyCron, Timezone: parisZone,
	})

	require.Equal(t, codes.NotFound, status.Code(err), "%v", err)
	assert.Contains(t, status.Convert(err).Message(), name)
	assert.Zero(t, scheduleRowsOf(t, name))
	assert.Nil(t, summaryOf(t, name))
}

// A schedule whose models a later release dropped keeps its cron, is listed
// with no models, and can still be paused and resumed.
func TestSchedules_UpsertEditsAFlaggedScheduleWhoseModelsAreGone(t *testing.T) {
	name := newScheduleName(t, "sched-flagged")
	promoteSchedules(t, stateDB, name)
	upsertYearly(t, "alice@example.com", name, 0, false)
	promoteSchedules(t, stateDB, "sched-other-"+uuid.NewString()[:8])

	flagged := summaryOf(t, name)
	require.NotNil(t, flagged, "a name with a cron is listed whatever the live release holds")
	assert.Zero(t, flagged.GetModelCount())
	assert.Equal(t, yearlyCron, flagged.GetCron())

	paused := upsertYearly(t, "bob@example.com", name, 1, true)
	assert.True(t, paused.GetSchedule().GetPaused())
	assert.Equal(t, int64(2), paused.GetSchedule().GetVersion())
	assert.Nil(t, paused.GetSchedule().GetNextFireAt())
	assert.Empty(t, paused.GetNextFireTimes(), "a paused schedule fires nothing")

	resumed := upsertYearly(t, "bob@example.com", name, 2, false)
	assert.False(t, resumed.GetSchedule().GetPaused())
	assert.Equal(t, int64(3), resumed.GetSchedule().GetVersion())
	assert.Len(t, resumed.GetNextFireTimes(), 3)
}

// An edit that names a version the schedule is no longer at is Aborted and
// leaves the newer edit in place.
func TestSchedules_AStaleVersionIsAborted(t *testing.T) {
	name := newScheduleName(t, "sched-stale")
	promoteSchedules(t, stateDB, name)
	upsertYearly(t, "alice@example.com", name, 0, false)
	upsertYearly(t, "bob@example.com", name, 1, true)

	_, err := runClient.UpsertSchedule(asUser("carol@example.com"), &runv1.UpsertScheduleRequest{
		ScheduleName: name, Cron: "0 4 1 1 *", Timezone: parisZone, ExpectedVersion: 1,
	})

	require.Equal(t, codes.Aborted, status.Code(err), "%v", err)
	got := summaryOf(t, name)
	require.NotNil(t, got)
	assert.Equal(t, int64(2), got.GetVersion())
	assert.Equal(t, yearlyCron, got.GetCron())
	assert.True(t, got.GetPaused())
	assert.Equal(t, "bob@example.com", got.GetUpdatedBy())

	_, err = runClient.UpsertSchedule(asUser("carol@example.com"), &runv1.UpsertScheduleRequest{
		ScheduleName: newScheduleName(t, "sched-gone"), Cron: yearlyCron, Timezone: parisZone, ExpectedVersion: 1,
	})
	require.Equal(t, codes.Aborted, status.Code(err), "an expected version of a schedule with no cron is stale: %v", err)
}

// Deleting the cron of a schedule that has none succeeds, as does deleting one
// twice; the schedule is then listed as manual.
func TestSchedules_DeleteOfAMissingCronSucceeds(t *testing.T) {
	ctx := context.Background()
	never := newScheduleName(t, "sched-never")
	_, err := runClient.DeleteSchedule(ctx, &runv1.DeleteScheduleRequest{ScheduleName: never})
	require.NoError(t, err)

	name := newScheduleName(t, "sched-delete")
	promoteSchedules(t, stateDB, name)
	upsertYearly(t, "alice@example.com", name, 0, false)
	for range 2 {
		_, err = runClient.DeleteSchedule(ctx, &runv1.DeleteScheduleRequest{ScheduleName: name})
		require.NoError(t, err)
	}

	assert.Zero(t, scheduleRowsOf(t, name))
	manual := summaryOf(t, name)
	require.NotNil(t, manual)
	assert.Empty(t, manual.GetCron())
	assert.Zero(t, manual.GetVersion())
	assert.Equal(t, int32(1), manual.GetModelCount())

	_, err = runClient.DeleteSchedule(ctx, &runv1.DeleteScheduleRequest{})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
}

// Removing the cron of a schedule while a run of it is in progress leaves the
// run running: the schedule only stops starting runs on its own.
func TestSchedules_DeletingACronLeavesItsActiveRunRunning(t *testing.T) {
	ctx := context.Background()
	name := newScheduleName(t, "sched-active")
	seedActiveSchedule(t, name)
	promoteSchedules(t, stateDB, name)
	upsertYearly(t, "alice@example.com", name, 0, false)
	runID := startRun(t, name)
	t.Cleanup(func() {
		_, _ = runClient.CancelRun(context.Background(), &runv1.CancelRunRequest{RunId: runID, CancelledBy: "test"})
		endCancelled(t, runID)
	})

	_, err := runClient.DeleteSchedule(ctx, &runv1.DeleteScheduleRequest{ScheduleName: name})
	require.NoError(t, err)

	assert.Equal(t, "running", runStatusOf(t, runID))
	got, err := runClient.GetRun(ctx, &runv1.GetRunRequest{RunId: runID})
	require.NoError(t, err)
	assert.Equal(t, "running", got.GetRun().GetStatus())
	summary := summaryOf(t, name)
	require.NotNil(t, summary)
	assert.Empty(t, summary.GetCron())
	assert.True(t, summary.GetHasActiveRun())
	assert.Equal(t, runID, summary.GetLastRunId())
}

// ListSchedules is one entry per name the live release has or that has a cron:
// design §9.3's four rows of a scheduled, a paused, a manual and a flagged
// schedule.
func TestSchedules_ListIsTheUnionOfTheLiveReleaseAndTheRows(t *testing.T) {
	prefix := "u" + uuid.NewString()[:6]
	daily, hourly, manual, weekly := prefix+"-daily", prefix+"-hourly", prefix+"-e2e", prefix+"-weekly"
	deleteScheduleRowsAtEnd(t, pkgevents.DefaultTenantID, daily, hourly, manual, weekly)
	table := func(name string, i int) string {
		return fmt.Sprintf("%s_%d", strings.ReplaceAll(name, "-", "_"), i)
	}
	nodes := func(name string, n int) []topology.NodeSpec {
		specs := make([]topology.NodeSpec, 0, n)
		for i := range n {
			specs = append(specs, scheduledNode("mart", table(name, i), "dbt-model", name))
		}
		return specs
	}
	var withWeekly, withoutWeekly []topology.NodeSpec
	for _, set := range [][]topology.NodeSpec{nodes(daily, 3), nodes(hourly, 2), nodes(manual, 1)} {
		withoutWeekly = append(withoutWeekly, set...)
	}
	withWeekly = append(append(withWeekly, withoutWeekly...), nodes(weekly, 1)...)
	promoteRelease(t, stateDB, withWeekly...)
	upsertYearly(t, "alice@example.com", daily, 0, false)
	_, err := runClient.UpsertSchedule(asUser("bob@example.com"), &runv1.UpsertScheduleRequest{
		ScheduleName: hourly, Cron: "0 * * * *", Timezone: parisZone, Paused: true,
	})
	require.NoError(t, err)
	upsertYearly(t, "alice@example.com", weekly, 0, false)
	promoteRelease(t, stateDB, withoutWeekly...)

	resp, err := runClient.ListSchedules(context.Background(), &runv1.ListSchedulesRequest{})
	require.NoError(t, err)
	mine := map[string]*runv1.ScheduleSummary{}
	var order []string
	for _, s := range resp.GetSchedules() {
		if strings.HasPrefix(s.GetScheduleName(), prefix+"-") {
			mine[s.GetScheduleName()] = s
			order = append(order, s.GetScheduleName())
		}
	}
	assert.Equal(t, []string{daily, manual, hourly, weekly}, order, "ordered by name")

	scheduled := mine[daily]
	require.NotNil(t, scheduled)
	assert.Equal(t, int32(3), scheduled.GetModelCount())
	assert.Equal(t, yearlyCron, scheduled.GetCron())
	assert.Equal(t, parisZone, scheduled.GetTimezone())
	assert.NotEmpty(t, scheduled.GetCronWords())
	assert.NotNil(t, scheduled.GetNextFireAt())
	assert.False(t, scheduled.GetPaused())
	assert.Equal(t, int64(1), scheduled.GetVersion())

	paused := mine[hourly]
	require.NotNil(t, paused)
	assert.Equal(t, int32(2), paused.GetModelCount())
	assert.True(t, paused.GetPaused())
	assert.Equal(t, "0 * * * *", paused.GetCron())
	assert.Nil(t, paused.GetNextFireAt())
	assert.Equal(t, "bob@example.com", paused.GetUpdatedBy())
	assert.NotNil(t, paused.GetUpdatedAt())

	manualSummary := mine[manual]
	require.NotNil(t, manualSummary)
	assert.Equal(t, int32(1), manualSummary.GetModelCount())
	assert.Empty(t, manualSummary.GetCron())
	assert.Empty(t, manualSummary.GetCronWords())
	assert.Zero(t, manualSummary.GetVersion())
	assert.Nil(t, manualSummary.GetNextFireAt())

	flagged := mine[weekly]
	require.NotNil(t, flagged)
	assert.Zero(t, flagged.GetModelCount(), "no model of the live release has the tag")
	assert.Equal(t, yearlyCron, flagged.GetCron())
	assert.NotNil(t, flagged.GetNextFireAt())
}

// A row whose stored cron no longer parses is listed as a name with no cron,
// and an edit saves over it, raising its version.
func TestSchedules_UpsertRepairsARowWhoseStoredCronNoLongerParses(t *testing.T) {
	name := newScheduleName(t, "sched-broken")
	promoteSchedules(t, stateDB, name)
	_, err := stateDB.Exec(`
		INSERT INTO schedules (tenant_id, name, cron, timezone, version, updated_by)
		VALUES ('default', $1, '0 3 1 1 *', 'Mars/Olympus', 4, 'by-hand')`, name)
	require.NoError(t, err)

	listed := summaryOf(t, name)
	require.NotNil(t, listed)
	assert.Empty(t, listed.GetCron(), "a cron the parser no longer reads is shown as none")
	assert.Equal(t, int32(1), listed.GetModelCount())

	repaired := upsertYearly(t, "alice@example.com", name, 0, false)

	assert.Equal(t, int64(5), repaired.GetSchedule().GetVersion())
	assert.Equal(t, parisZone, repaired.GetSchedule().GetTimezone())
	got := summaryOf(t, name)
	require.NotNil(t, got)
	assert.Equal(t, yearlyCron, got.GetCron())
	assert.Equal(t, int64(5), got.GetVersion())
}

// Another tenant's schedules are invisible: its live release does not let the
// default tenant create a name, and neither tenant lists the other's.
func TestSchedules_AnotherTenantsSchedulesAreInvisible(t *testing.T) {
	ctx := context.Background()
	tenant := "sched-tenant-" + uuid.NewString()[:8]
	name := "theirs-" + uuid.NewString()[:8]
	deleteScheduleRowsAtEnd(t, tenant, name)
	deleteScheduleRowsAtEnd(t, pkgevents.DefaultTenantID, name)
	promoteTenantRelease(t, tenant, scheduledNode("mart", strings.ReplaceAll(name, "-", "_"), "dbt-model", name))
	_, _, err := scheduleService.Upsert(ctx, tenant, schedules.UpsertInput{Name: name, Cron: yearlyCron, Timezone: parisZone}, "bob@example.com")
	require.NoError(t, err)

	theirs, err := scheduleService.List(ctx, tenant)
	require.NoError(t, err)
	require.Len(t, theirs, 1)
	assert.Equal(t, name, theirs[0].Name)
	assert.Equal(t, 1, theirs[0].ModelCount)
	require.NotNil(t, theirs[0].Schedule)

	assert.Nil(t, summaryOf(t, name), "the default tenant does not list another tenant's schedule")
	_, err = runClient.UpsertSchedule(asUser("alice@example.com"), &runv1.UpsertScheduleRequest{
		ScheduleName: name, Cron: yearlyCron, Timezone: parisZone,
	})
	require.Equal(t, codes.NotFound, status.Code(err), "another tenant's models do not make the name known: %v", err)
	_, err = runClient.DeleteSchedule(ctx, &runv1.DeleteScheduleRequest{ScheduleName: name})
	require.NoError(t, err)

	still, ok, err := postgres.NewScheduleRepository(stateDB, nil, testParser(t), nil).Get(ctx, tenant, name)
	require.NoError(t, err)
	require.True(t, ok, "a delete of the default tenant leaves another tenant's row")
	assert.Equal(t, "bob@example.com", still.UpdatedBy())
	_, _, err = scheduleService.Upsert(ctx, tenant, schedules.UpsertInput{Name: name, Cron: yearlyCron, Timezone: parisZone, ExpectedVersion: 9}, "bob@example.com")
	require.ErrorIs(t, err, schedule.ErrStaleVersion)
}

// PreviewSchedule refuses what the parser refuses with InvalidArgument and the
// parser's own message, as UpsertSchedule does; a cron it reads previews its
// words and three fires.
func TestPreviewSchedule_InvalidCronIsInvalidArgumentWithTheParsersMessage(t *testing.T) {
	ctx := context.Background()
	parser := testParser(t)
	name := newScheduleName(t, "sched-preview")
	promoteSchedules(t, stateDB, name)
	for _, c := range []struct{ cron, timezone string }{
		{"CRON_TZ=Europe/Paris 0 1 * * *", parisZone},
		{"TZ=UTC 0 1 * * *", "UTC"},
		{"61 * * * *", "UTC"},
		{"0 1 * * *", "Mars/Olympus"},
		{"0 1 * * *", ""},
		{"@every 30s", "UTC"},
		{"0 0 30 2 *", "UTC"},
	} {
		_, parseErr := parser.Parse(c.cron, c.timezone)
		require.Error(t, parseErr, c.cron)

		_, err := runClient.PreviewSchedule(ctx, &runv1.PreviewScheduleRequest{Cron: c.cron, Timezone: c.timezone})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%q in %q: %v", c.cron, c.timezone, err)
		assert.Equal(t, parseErr.Error(), status.Convert(err).Message())

		_, err = runClient.UpsertSchedule(asUser("alice@example.com"), &runv1.UpsertScheduleRequest{
			ScheduleName: name, Cron: c.cron, Timezone: c.timezone,
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%q in %q: %v", c.cron, c.timezone, err)
		assert.Equal(t, parseErr.Error(), status.Convert(err).Message())
	}
	assert.Zero(t, scheduleRowsOf(t, name), "a refused cron writes nothing")

	preview, err := runClient.PreviewSchedule(ctx, &runv1.PreviewScheduleRequest{Cron: "0 1 * * *", Timezone: parisZone})
	require.NoError(t, err)
	words, err := parser.Describe("0 1 * * *")
	require.NoError(t, err)
	assert.Equal(t, words, preview.GetCronWords())
	assert.Len(t, preview.GetNextFireTimes(), 3)

	noWords, err := runClient.PreviewSchedule(ctx, &runv1.PreviewScheduleRequest{Cron: "0 9 * * */9", Timezone: "UTC"})
	require.NoError(t, err, "a cron with no words still previews")
	assert.Empty(t, noWords.GetCronWords())
	assert.Len(t, noWords.GetNextFireTimes(), 3)
}
