package test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	runv1 "github.com/carolsimone/continuo/state/proto/run/v1"
)

// A trigger names a schedule state knows: a model of the tenant's live release
// carries the name, or the name has a cron. A name with neither is NotFound
// and stores nothing, while a name a model carries starts with no cron and
// nothing else to say it exists.
func TestStartRun_ANameNoModelHasIsNotFound(t *testing.T) {
	ctx := context.Background()
	known := "start-known-" + uuid.NewString()[:8]
	unknown := "start-unknown-" + uuid.NewString()[:8]
	promoteSchedules(t, stateDB, known)
	requestID := uuid.NewString()

	_, err := runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: unknown})
	require.Equal(t, codes.NotFound, status.Code(err), "%v", err)
	assert.Zero(t, commandRecords(t, requestID), "a refused call stores no record")
	assert.Zero(t, countOf(t, stateDB, `SELECT count(*) FROM runs WHERE schedule_name = $1`, unknown))

	runID := startRun(t, known)
	t.Cleanup(func() { finishRun(t, runID) })
	assert.Equal(t, "running", runStatusOf(t, runID), "a name a model of the live release carries starts")
	assert.Zero(t, scheduleRowsOf(t, known), "with no cron")
}

// A name whose cron outlived its models (a later release dropped every model
// carrying it) is a schedule state knows with nothing to run: a trigger of it
// is refused as an empty selection, creates no run and stores no record.
func TestStartRun_ANameWithACronButNoModelsIsAnEmptySelection(t *testing.T) {
	ctx := context.Background()
	name := newScheduleName(t, "start-cron-only")
	promoteSchedules(t, stateDB, name)
	upsertYearly(t, "alice@example.com", name, 0, true)
	promoteSchedules(t, stateDB, "start-elsewhere-"+uuid.NewString()[:8])
	requestID := uuid.NewString()

	_, err := runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: requestID, ScheduleName: name})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	assert.Contains(t, status.Convert(err).Message(), "the selection is empty")
	assert.Zero(t, commandRecords(t, requestID), "a refused call stores no record")
	assert.Zero(t, countOf(t, stateDB, `SELECT count(*) FROM runs WHERE schedule_name = $1`, name))
	assert.Equal(t, 1, scheduleRowsOf(t, name), "the cron stays")
}

// A schedule whose stored cron no longer parses is still a schedule state
// knows: a trigger of it with no models is the empty selection, not NotFound.
func TestStartRun_ANameWhoseStoredCronNoLongerParsesIsKnown(t *testing.T) {
	ctx := context.Background()
	name := newScheduleName(t, "start-bad-cron")
	insertUnparsableRow(t, name, 1)
	promoteSchedules(t, stateDB, "start-elsewhere-"+uuid.NewString()[:8])

	_, err := runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: uuid.NewString(), ScheduleName: name})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
	assert.Contains(t, status.Convert(err).Message(), "the selection is empty")
}

// A single-node run carries a made-up schedule name that no model of the live
// release and no cron carries. It does not start through StartRun, so it does
// not depend on its name being known: the run and its rerun start, while
// StartRun of its name stays NotFound. (A promote-seed run's made-up name is
// the seed reconcile's, which TestSeedReconcile starts in tenants with no
// schedules.)
func TestStartRun_TheMadeUpNameOfASingleNodeRunNeedsNoModelOrCron(t *testing.T) {
	ctx := context.Background()
	table := "made_up_" + uuid.NewString()[:8]
	promoteRelease(t, stateDB, scheduledNode("mart", table, "dbt-model", ""))

	single, err := runClient.TriggerSingleNodeRun(ctx, &runv1.TriggerSingleNodeRunRequest{
		RequestId: uuid.NewString(), ServiceName: "service-1", SchemaName: "mart", TableName: table, MetadataSource: "latest",
	})
	require.NoError(t, err, "a single-node run starts under its made-up name")
	assert.Zero(t, scheduleRowsOf(t, single.GetScheduleName()))
	_, err = stateDB.Exec(`UPDATE tasks SET status = 'failed', attempt = 2 WHERE run_id = $1`, single.GetRunId())
	require.NoError(t, err)
	finishRun(t, single.GetRunId())

	rerun, err := runClient.TriggerRerun(ctx, &runv1.TriggerRerunRequest{RequestId: uuid.NewString(), SourceRunId: single.GetRunId()})
	require.NoError(t, err, "a rerun of it starts under the same name")
	t.Cleanup(func() { finishRun(t, rerun.GetRunId()) })
	assert.Equal(t, single.GetScheduleName(), rerun.GetScheduleName())

	_, err = runClient.StartRun(ctx, &runv1.StartRunRequest{RequestId: uuid.NewString(), ScheduleName: single.GetScheduleName()})
	require.Equal(t, codes.NotFound, status.Code(err), "%v", err)
}
